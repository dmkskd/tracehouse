#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["clickhouse-driver", "python-dotenv"]
# ///
"""
Continuous Query Runner for TraceHouse Testing

Runs queries of varying types (slow, fast, PK-pattern, JOIN, S3, settings-variation)
to generate activity for the monitoring dashboard.

Usage:
    uv run scripts/run_queries.py [--host HOST] [--port PORT]
"""

import argparse
import os
import sys
import random
import time
import threading
import logging
import warnings
from datetime import datetime
from clickhouse_driver import Client

# Suppress noisy "Error on socket shutdown" messages from clickhouse-driver
# when TLS connections are closed by the server (common on managed services).
logging.getLogger('clickhouse_driver.connection').setLevel(logging.CRITICAL)
warnings.filterwarnings('ignore', message='.*socket.*')

# The clickhouse-driver prints "Error on socket shutdown" directly to stderr
# via Python's print(). Redirect stderr through a filter to suppress these.
import io

class _SocketErrorFilter(io.TextIOBase):
    """Wraps stderr to suppress clickhouse-driver socket shutdown noise."""
    def __init__(self, stream):
        self._stream = stream
    def write(self, s):
        if 'Error on socket shutdown' in s:
            return len(s)  # swallow it
        return self._stream.write(s)
    def flush(self):
        self._stream.flush()
    def fileno(self):
        return self._stream.fileno()

sys.stderr = _SocketErrorFilter(sys.stderr)

# Allow importing the sibling ch_capabilities module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ch_capabilities import probe, Capabilities

# These queries are designed to be SLOW (5-30 seconds) but not OOM
# Key techniques:
# - sleepEachRow() to artificially slow down queries
# - Full table scans with expensive aggregations
# - Mix of single-threaded and multi-threaded queries

SLOW_QUERIES = [
    # Artificially slow query using sleepEachRow - single thread, no cache
    """
    SELECT 
        country_code,
        count() as events,
        uniqExact(user_id) as unique_users,
        sum(revenue) as total_revenue,
        sleepEachRow(0.0001) as _delay
    FROM synthetic_data.events
    GROUP BY country_code
    ORDER BY events DESC
    SETTINGS max_threads = 1, use_uncompressed_cache = 0, use_query_cache = 0
    """,

    # Multi-threaded heavy aggregation - let CH use all cores
    """
    SELECT 
        toStartOfHour(event_time) as hour,
        count() as events,
        uniqExact(user_id) as users,
        avg(duration_ms) as avg_duration
    FROM synthetic_data.events
    GROUP BY hour
    ORDER BY hour
    """,

    # NYC taxi slow aggregation - single thread with sleep, no cache
    """
    SELECT 
        pickup_location_id,
        count() as trips,
        sum(total_amount) as total_fares,
        avg(trip_distance) as avg_distance,
        sleepEachRow(0.0001) as _delay
    FROM nyc_taxi.trips
    GROUP BY pickup_location_id
    ORDER BY trips DESC
    SETTINGS max_threads = 1, use_uncompressed_cache = 0, use_query_cache = 0
    """,

    # Multi-threaded heavy aggregation with multiple uniqExact
    """
    SELECT 
        device_type,
        browser,
        count() as events,
        uniqExact(user_id) as unique_users,
        uniqExact(session_id) as unique_sessions,
        quantilesExact(0.5, 0.9, 0.99)(duration_ms) as duration_pcts
    FROM synthetic_data.events
    GROUP BY device_type, browser
    ORDER BY events DESC
    """,

    # Trip analysis with date functions - multi-threaded
    """
    SELECT 
        toHour(pickup_datetime) as hour,
        toDayOfWeek(pickup_datetime) as dow,
        count() as trips,
        avg(total_amount) as avg_fare,
        sum(tip_amount) as total_tips
    FROM nyc_taxi.trips
    GROUP BY hour, dow
    ORDER BY trips DESC
    """,

    # Session analysis - expensive groupArray, single thread with sleep, no cache
    """
    SELECT 
        session_id,
        count() as events_in_session,
        min(event_time) as session_start,
        max(event_time) as session_end,
        sum(revenue) as session_revenue,
        sleepEachRow(0.0001) as _delay
    FROM synthetic_data.events
    GROUP BY session_id
    HAVING events_in_session >= 2
    ORDER BY session_revenue DESC
    LIMIT 100000
    SETTINGS max_threads = 1, use_uncompressed_cache = 0, use_query_cache = 0
    """,

    # Multi-threaded cross join simulation - heavy CPU
    """
    SELECT 
        a.country_code,
        b.device_type,
        count() as combinations
    FROM synthetic_data.events a
    CROSS JOIN (SELECT DISTINCT device_type FROM synthetic_data.events LIMIT 10) b
    GROUP BY a.country_code, b.device_type
    ORDER BY combinations DESC
    """,

    # Multi-threaded - full scan with multiple aggregations
    """
    SELECT 
        event_type,
        country_code,
        device_type,
        count() as cnt,
        uniqExact(user_id) as users,
        uniqExact(session_id) as sessions,
        sum(duration_ms) as total_duration,
        sum(revenue) as total_revenue
    FROM synthetic_data.events
    GROUP BY event_type, country_code, device_type
    ORDER BY cnt DESC
    """,

    # Window functions - multi-threaded
    """
    SELECT 
        user_id,
        event_time,
        revenue,
        sum(revenue) OVER (PARTITION BY user_id ORDER BY event_time) as cumulative_revenue
    FROM synthetic_data.events
    WHERE revenue > 0
    ORDER BY user_id, event_time
    LIMIT 50000
    """,

    # Heavy sorting - multi-threaded
    """
    SELECT *
    FROM synthetic_data.events
    ORDER BY duration_ms DESC, revenue DESC, event_time
    LIMIT 100000
    """,

    # ── Sharded web_analytics queries (Distributed table, 2 shards) ──

    # Cross-shard aggregation — Distributed table fans out to both shards
    """
    SELECT
        domain,
        count() as pageviews,
        uniqExact(user_id) as unique_visitors,
        avg(duration_ms) as avg_duration,
        sleepEachRow(0.0001) as _delay
    FROM web_analytics.pageviews
    GROUP BY domain
    ORDER BY pageviews DESC
    SETTINGS max_threads = 1, use_uncompressed_cache = 0, use_query_cache = 0
    """,

    # Cross-shard hourly traffic — multi-threaded, heavy merge across shards
    """
    SELECT
        toStartOfHour(event_time) as hour,
        domain,
        count() as hits,
        uniqExact(session_id) as sessions,
        countIf(is_bounce = 1) as bounces
    FROM web_analytics.pageviews
    GROUP BY hour, domain
    ORDER BY hour, hits DESC
    """,

    # Cross-shard funnel analysis — expensive groupArray across shards
    """
    SELECT
        user_id,
        groupArray(path) as page_sequence,
        count() as pages_visited,
        min(event_time) as first_seen,
        max(event_time) as last_seen,
        sleepEachRow(0.0001) as _delay
    FROM web_analytics.pageviews
    GROUP BY user_id
    HAVING pages_visited >= 3
    ORDER BY pages_visited DESC
    LIMIT 50000
    SETTINGS max_threads = 1, use_uncompressed_cache = 0, use_query_cache = 0
    """,

    # Cross-shard referrer analysis — multi-threaded
    """
    SELECT
        referrer,
        domain,
        count() as visits,
        uniqExact(user_id) as unique_users,
        avg(duration_ms) as avg_duration,
        countIf(is_bounce = 1) * 100.0 / count() as bounce_rate
    FROM web_analytics.pageviews
    WHERE referrer != ''
    GROUP BY referrer, domain
    ORDER BY visits DESC
    """,
]

# S3 Parquet queries - test network I/O and parquet parsing from public ClickHouse datasets
S3_PARQUET_QUERIES = [
    # Amazon reviews - 150M+ reviews, snappy-compressed parquet
    # Source: https://clickhouse.com/docs/getting-started/example-datasets/amazon-reviews
    """
    SELECT 
        product_category,
        count() as review_count,
        avg(star_rating) as avg_rating,
        sum(helpful_votes) as total_helpful
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet')
    GROUP BY product_category
    ORDER BY review_count DESC
    LIMIT 20
    """,

    # Stack Overflow posts - query parquet files directly from S3
    # Source: https://clickhouse.com/docs/data-modeling/schema-design
    """
    SELECT 
        toYear(CreationDate) as year,
        count() as posts,
        avg(Score) as avg_score,
        sum(ViewCount) as total_views
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2023.parquet')
    GROUP BY year
    """,

    # Stack Overflow - search for specific content in posts
    """
    SELECT 
        Id,
        Title,
        Score,
        ViewCount
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2023.parquet')
    WHERE Title IS NOT NULL AND position(lower(Title), 'clickhouse') > 0
    ORDER BY Score DESC
    LIMIT 50
    """,

    # Amazon reviews - find products with 'awesome' in reviews
    """
    SELECT 
        product_id,
        any(product_title) as title,
        avg(star_rating) as avg_rating,
        count() as mention_count
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet')
    WHERE position(review_body, 'awesome') > 0
    GROUP BY product_id
    ORDER BY mention_count DESC
    LIMIT 30
    """,

    # UK house prices - parquet from S3 (date column is UInt16 days-since-epoch, cast to Date first)
    """
    SELECT 
        toYear(toDate(date)) as yr,
        count() as transactions,
        avg(price) as avg_price,
        max(price) as max_price
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/uk-house-prices/parquet/house_prices_*.parquet')
    GROUP BY yr
    ORDER BY yr DESC
    LIMIT 20
    """,

    # Stack Overflow users - parquet query (cast Reputation to number)
    """
    SELECT 
        Location,
        count() as user_count,
        avg(toInt64OrZero(toString(Reputation))) as avg_reputation,
        max(toInt64OrZero(toString(Reputation))) as max_reputation
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/users.parquet')
    WHERE Location != ''
    GROUP BY Location
    ORDER BY user_count DESC
    LIMIT 30
    """,

    # Amazon reviews - heavy aggregation across multiple years
    """
    SELECT 
        marketplace,
        product_category,
        count() as reviews,
        avg(star_rating) as avg_stars,
        countIf(verified_purchase) as verified_count
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/amazon_reviews/amazon_reviews_2015.snappy.parquet')
    GROUP BY marketplace, product_category
    ORDER BY reviews DESC
    LIMIT 50
    """,

    # Stack Overflow votes analysis (cast CreationDate to DateTime)
    """
    SELECT 
        VoteTypeId,
        toYear(toDateTime64OrZero(toString(CreationDate), 3)) as year,
        count() as vote_count
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/votes.parquet')
    WHERE year > 2000
    GROUP BY VoteTypeId, year
    ORDER BY year DESC, vote_count DESC
    LIMIT 50
    """,
]

# PK-efficient queries — demonstrate various ORDER BY usage patterns
# These generate a mix of diagnostic categories in the Analytics tab:
# - Full key match (good pruning)
# - Partial key (uses leftmost, not all)
# - Skips leftmost key (poor — filters on 2nd/3rd key column only)
# - No key match (poor — WHERE on non-key columns)
#
# Table ORDER BY keys:
#   synthetic_data.events:         (event_date, user_id, event_time)
#   nyc_taxi.trips:                (pickup_date, pickup_location_id, pickup_datetime)
#   uk_price_paid.uk_price_paid:   (postcode1, postcode2, date)
#
# Many of these are now functions that randomize literal values so that
# executions share the same normalized_query_hash but have different
# parameters — useful for testing the literal-diff feature.

def _rand_days_ago() -> int:
    return random.randint(1, 30)

def _rand_user_id() -> int:
    return random.randint(1, 50000)

def _rand_user_ids(n: int = 5) -> str:
    return ', '.join(str(random.randint(1, 50000)) for _ in range(n))

def _rand_country() -> str:
    return random.choice(['US', 'GB', 'DE', 'FR', 'JP', 'BR', 'IN', 'CA', 'AU', 'MX'])

def _rand_device() -> str:
    return random.choice(['mobile', 'desktop', 'tablet'])

def _rand_pickup_location() -> int:
    return random.choice([132, 138, 161, 186, 237, 48, 79, 230, 170, 162])

def _rand_pickup_locations(n: int = 5) -> str:
    locs = random.sample([132, 138, 161, 186, 237, 48, 79, 230, 170, 162, 100, 234, 249, 113, 114], min(n, 15))
    return ', '.join(str(l) for l in locs)

def _rand_postcode1() -> str:
    return random.choice(['SW1', 'SW3', 'SW7', 'W1', 'W8', 'EC1', 'EC2', 'N1', 'SE1', 'NW1'])

def _rand_postcodes(n: int = 5) -> str:
    codes = random.sample(['SW1', 'SW3', 'SW7', 'W1', 'W8', 'EC1', 'EC2', 'N1', 'SE1', 'NW1', 'E1', 'WC1', 'WC2'], min(n, 13))
    return ', '.join(f"'{c}'" for c in codes)

def _rand_postcode2() -> str:
    return random.choice(['1AA', '2AB', '3BC', '4CD', '5DE', '6EF', '7FG', '8GH', '9HJ'])

def _rand_year_range() -> tuple:
    start_year = random.randint(2020, 2025)
    return f'{start_year}-01-01', f'{start_year}-07-01'

def _rand_town() -> str:
    return random.choice(['LONDON', 'MANCHESTER', 'BIRMINGHAM', 'LEEDS', 'BRISTOL', 'LIVERPOOL', 'EDINBURGH'])

def _rand_county() -> str:
    return random.choice(['GREATER LONDON', 'GREATER MANCHESTER', 'WEST MIDLANDS', 'WEST YORKSHIRE', 'AVON', 'MERSEYSIDE'])

def _rand_limit() -> int:
    return random.choice([10, 20, 30, 50, 100])

def _rand_domain() -> str:
    return random.choice([
        'example.com', 'shop.example.com', 'blog.example.com',
        'docs.example.com', 'api.example.com', 'app.example.com',
        'news.example.com', 'forum.example.com', 'wiki.example.com',
        'status.example.com',
    ])

def _rand_domains(n: int = 4) -> str:
    domains = random.sample([
        'example.com', 'shop.example.com', 'blog.example.com',
        'docs.example.com', 'api.example.com', 'app.example.com',
        'news.example.com', 'forum.example.com', 'wiki.example.com',
        'status.example.com',
    ], min(n, 10))
    return ', '.join(f"'{d}'" for d in domains)

def _rand_path() -> str:
    return random.choice([
        '/', '/about', '/pricing', '/docs', '/blog',
        '/login', '/signup', '/dashboard', '/settings', '/contact',
    ])

def _rand_referrer() -> str:
    return random.choice([
        'https://google.com', 'https://twitter.com', 'https://reddit.com', 'https://hn.com',
    ])


PK_EFFICIENT_QUERY_GENERATORS = [
    # ── synthetic_data.events: ORDER BY (event_date, user_id, event_time) ──

    # Full key match — all 3 ORDER BY columns in WHERE
    lambda: f"""
    SELECT count(), avg(duration_ms), sum(revenue)
    FROM synthetic_data.events
    WHERE event_date = today() - {_rand_days_ago()}
      AND user_id = {_rand_user_id()}
      AND event_time >= now() - INTERVAL {random.randint(1, 7)} DAY
    SETTINGS use_query_cache = 0
    """,

    # Partial key (1/3) — leftmost only
    lambda: f"""
    SELECT event_type, count() AS cnt, uniq(user_id) AS users
    FROM synthetic_data.events
    WHERE event_date = today() - {_rand_days_ago()}
    GROUP BY event_type
    ORDER BY cnt DESC
    SETTINGS use_query_cache = 0
    """,

    # Partial key (2/3) — leftmost + second
    lambda: f"""
    SELECT count(), sum(revenue), avg(duration_ms)
    FROM synthetic_data.events
    WHERE event_date BETWEEN today() - {random.randint(7, 30)} AND today()
      AND user_id IN ({_rand_user_ids()})
    SETTINGS use_query_cache = 0
    """,

    # Skips leftmost key — filters on user_id only (2nd key column)
    lambda: f"""
    SELECT event_date, count(), sum(revenue)
    FROM synthetic_data.events
    WHERE user_id = {_rand_user_id()}
    GROUP BY event_date
    ORDER BY event_date
    SETTINGS use_query_cache = 0
    """,

    # No key match — WHERE on non-key columns (country_code, device_type)
    lambda: f"""
    SELECT event_type, count(), avg(duration_ms)
    FROM synthetic_data.events
    WHERE country_code = '{_rand_country()}' AND device_type = '{_rand_device()}'
    GROUP BY event_type
    ORDER BY count() DESC
    SETTINGS use_query_cache = 0
    """,

    # ── nyc_taxi.trips: ORDER BY (pickup_date, pickup_location_id, pickup_datetime) ──

    # Full key match — all 3 ORDER BY columns
    lambda: f"""
    SELECT count(), avg(total_amount), avg(trip_distance)
    FROM nyc_taxi.trips
    WHERE pickup_date = today() - {_rand_days_ago()}
      AND pickup_location_id = {_rand_pickup_location()}
      AND pickup_datetime >= now() - INTERVAL {random.randint(2, 10)} DAY
    SETTINGS use_query_cache = 0
    """,

    # Partial key (1/3) — leftmost only
    lambda: f"""
    SELECT payment_type, count() AS trips, sum(total_amount) AS revenue
    FROM nyc_taxi.trips
    WHERE pickup_date = today() - {_rand_days_ago()}
    GROUP BY payment_type
    ORDER BY trips DESC
    SETTINGS use_query_cache = 0
    """,

    # Partial key (2/3) — leftmost + second
    lambda: f"""
    SELECT toHour(pickup_datetime) AS hr, count(), avg(fare_amount)
    FROM nyc_taxi.trips
    WHERE pickup_date BETWEEN today() - {random.randint(3, 14)} AND today()
      AND pickup_location_id IN ({_rand_pickup_locations()})
    GROUP BY hr
    ORDER BY hr
    SETTINGS use_query_cache = 0
    """,

    # Skips leftmost key — filters on pickup_location_id only (2nd key column)
    lambda: f"""
    SELECT pickup_date, count(), avg(total_amount)
    FROM nyc_taxi.trips
    WHERE pickup_location_id = {_rand_pickup_location()}
    GROUP BY pickup_date
    ORDER BY pickup_date
    SETTINGS use_query_cache = 0
    """,

    # No key match — WHERE on non-key columns (vendor_name, payment_type)
    lambda: """
    SELECT toStartOfHour(pickup_datetime) AS hour, count()
    FROM nyc_taxi.trips
    WHERE vendor_name = 'Yellow Cab' AND payment_type = 'Credit card'
    GROUP BY hour
    ORDER BY hour
    SETTINGS use_query_cache = 0
    """,

    # ── uk_price_paid.uk_price_paid: ORDER BY (postcode1, postcode2, date) ──

    # Full key match — all 3 ORDER BY columns
    lambda: f"""
    SELECT count(), avg(price), max(price)
    FROM uk_price_paid.uk_price_paid
    WHERE postcode1 = '{_rand_postcode1()}'
      AND postcode2 = '{_rand_postcode2()}'
      AND date >= '{random.randint(2020, 2025)}-01-01'
    SETTINGS use_query_cache = 0
    """,

    # Partial key (1/3) — leftmost only
    lambda: f"""
    SELECT type, count() AS sales, avg(price) AS avg_price
    FROM uk_price_paid.uk_price_paid
    WHERE postcode1 = '{_rand_postcode1()}'
    GROUP BY type
    ORDER BY sales DESC
    SETTINGS use_query_cache = 0
    """,

    # Partial key (2/3) — leftmost + second
    lambda: f"""
    SELECT toYear(date) AS yr, count(), avg(price)
    FROM uk_price_paid.uk_price_paid
    WHERE postcode1 IN ({_rand_postcodes()})
      AND postcode2 = '{_rand_postcode2()}'
    GROUP BY yr
    ORDER BY yr
    SETTINGS use_query_cache = 0
    """,

    # Skips leftmost key — filters on date only (3rd key column)
    lambda: f"""
    SELECT postcode1, count(), avg(price)
    FROM uk_price_paid.uk_price_paid
    WHERE date >= '{_rand_year_range()[0]}' AND date < '{_rand_year_range()[1]}'
    GROUP BY postcode1
    ORDER BY count() DESC
    LIMIT {_rand_limit()}
    SETTINGS use_query_cache = 0
    """,

    # No key match — WHERE on non-key columns (town, county)
    lambda: f"""
    SELECT street, count(), avg(price)
    FROM uk_price_paid.uk_price_paid
    WHERE town = '{_rand_town()}' AND county = '{_rand_county()}'
    GROUP BY street
    ORDER BY avg(price) DESC
    LIMIT {_rand_limit()}
    SETTINGS use_query_cache = 0
    """,

    # ── web_analytics.pageviews (Distributed): ORDER BY (domain, event_date, user_id) ──
    # These hit the Distributed table, so they fan out to both shards.

    # Full key match — all 3 ORDER BY columns
    lambda: f"""
    SELECT count(), avg(duration_ms), countIf(is_bounce = 1)
    FROM web_analytics.pageviews
    WHERE domain = '{_rand_domain()}'
      AND event_date = today() - {_rand_days_ago()}
      AND user_id = {_rand_user_id()}
    SETTINGS use_query_cache = 0
    """,

    # Partial key (1/3) — leftmost only (domain)
    lambda: f"""
    SELECT path, count() AS hits, uniq(user_id) AS visitors
    FROM web_analytics.pageviews
    WHERE domain = '{_rand_domain()}'
    GROUP BY path
    ORDER BY hits DESC
    SETTINGS use_query_cache = 0
    """,

    # Partial key (2/3) — domain + event_date
    lambda: f"""
    SELECT count(), avg(duration_ms), countIf(is_bounce = 1) * 100.0 / count() AS bounce_rate
    FROM web_analytics.pageviews
    WHERE domain IN ({_rand_domains()})
      AND event_date BETWEEN today() - {random.randint(7, 30)} AND today()
    SETTINGS use_query_cache = 0
    """,

    # Skips leftmost key — filters on event_date only (2nd key column)
    lambda: f"""
    SELECT domain, count(), uniq(session_id)
    FROM web_analytics.pageviews
    WHERE event_date = today() - {_rand_days_ago()}
    GROUP BY domain
    ORDER BY count() DESC
    SETTINGS use_query_cache = 0
    """,

    # No key match — WHERE on non-key columns (browser, country_code)
    lambda: f"""
    SELECT path, count(), avg(duration_ms)
    FROM web_analytics.pageviews
    WHERE browser = '{random.choice(["Chrome", "Firefox", "Safari", "Edge"])}'
      AND country_code = '{_rand_country()}'
    GROUP BY path
    ORDER BY count() DESC
    LIMIT {_rand_limit()}
    SETTINGS use_query_cache = 0
    """,
]

# Keep a static list for backward compat (used nowhere now, but just in case)
PK_EFFICIENT_QUERIES = [gen() for gen in PK_EFFICIENT_QUERY_GENERATORS]

# ── Settings-variation queries — same SQL, different max_threads each run ──
# These produce the same normalized_query_hash but with a different
# max_threads SETTINGS value on every execution, useful for tracking
# how a query behaves when its parameters change over time.

def _rand_max_threads() -> int:
    return random.choice([1, 2, 4, 8, 16, 32])

SETTINGS_VARIATION_GENERATORS = [
    # synthetic_data — heavy aggregation with variable parallelism
    lambda: f"""
    SELECT
        country_code,
        device_type,
        count() AS events,
        uniqExact(user_id) AS unique_users,
        sum(revenue) AS total_revenue,
        avg(duration_ms) AS avg_duration
    FROM synthetic_data.events
    WHERE event_date >= today() - {_rand_days_ago()}
    GROUP BY country_code, device_type
    ORDER BY events DESC
    SETTINGS max_threads = {_rand_max_threads()}, use_query_cache = 0
    """,

    # nyc_taxi — trip stats with variable parallelism
    lambda: f"""
    SELECT
        toHour(pickup_datetime) AS hour,
        count() AS trips,
        avg(total_amount) AS avg_fare,
        avg(trip_distance) AS avg_distance,
        sum(tip_amount) AS total_tips
    FROM nyc_taxi.trips
    WHERE pickup_date >= today() - {_rand_days_ago()}
    GROUP BY hour
    ORDER BY hour
    SETTINGS max_threads = {_rand_max_threads()}, use_query_cache = 0
    """,

    # web_analytics — cross-shard aggregation with variable parallelism
    lambda: f"""
    SELECT
        domain,
        path,
        count() AS pageviews,
        uniqExact(user_id) AS unique_visitors,
        countIf(is_bounce = 1) * 100.0 / count() AS bounce_rate
    FROM web_analytics.pageviews
    WHERE event_date >= today() - {_rand_days_ago()}
    GROUP BY domain, path
    ORDER BY pageviews DESC
    LIMIT 200
    SETTINGS max_threads = {_rand_max_threads()}, use_query_cache = 0
    """,

    # uk_price_paid — price analysis with variable parallelism
    lambda: f"""
    SELECT
        postcode1,
        type,
        count() AS sales,
        avg(price) AS avg_price,
        max(price) AS max_price,
        min(price) AS min_price
    FROM uk_price_paid.uk_price_paid
    WHERE town = '{_rand_town()}'
    GROUP BY postcode1, type
    ORDER BY avg_price DESC
    SETTINGS max_threads = {_rand_max_threads()}, use_query_cache = 0
    """,
]

# ── JOIN queries — exercise various join strategies ─────────────────
# These join the fact tables against dimension/lookup tables or against
# each other.  Mix of INNER, LEFT, RIGHT, CROSS, SEMI, ANTI, ASOF, and
# subquery-based joins so the query log shows a variety of join types.

JOIN_QUERY_GENERATORS = [
    # ── INNER JOIN: trips × locations (pickup) ──────────────────────
    lambda: f"""
    SELECT
        l.borough,
        count() AS trips,
        avg(t.total_amount) AS avg_fare,
        avg(t.trip_distance) AS avg_dist
    FROM nyc_taxi.trips AS t
    INNER JOIN nyc_taxi.locations AS l
        ON t.pickup_location_id = l.location_id
    WHERE t.pickup_date >= today() - {random.randint(7, 30)}
    GROUP BY l.borough
    ORDER BY trips DESC
    SETTINGS use_query_cache = 0
    """,

    # ── LEFT JOIN: trips × locations (dropoff) ──────────────────────
    lambda: f"""
    SELECT
        l.zone,
        l.service_zone,
        count() AS dropoffs,
        sum(t.tip_amount) AS total_tips
    FROM nyc_taxi.trips AS t
    LEFT JOIN nyc_taxi.locations AS l
        ON t.dropoff_location_id = l.location_id
    WHERE t.pickup_date = today() - {random.randint(1, 14)}
    GROUP BY l.zone, l.service_zone
    ORDER BY dropoffs DESC
    LIMIT {random.choice([20, 50, 100])}
    SETTINGS use_query_cache = 0
    """,

    # ── Double JOIN: pickup + dropoff locations on same trip ────────
    lambda: f"""
    SELECT
        p.borough AS pickup_borough,
        d.borough AS dropoff_borough,
        count() AS trips,
        avg(t.total_amount) AS avg_fare,
        avg(t.trip_duration_seconds) AS avg_duration_s
    FROM nyc_taxi.trips AS t
    INNER JOIN nyc_taxi.locations AS p ON t.pickup_location_id = p.location_id
    INNER JOIN nyc_taxi.locations AS d ON t.dropoff_location_id = d.location_id
    WHERE t.pickup_date >= today() - {random.randint(3, 14)}
    GROUP BY pickup_borough, dropoff_borough
    ORDER BY trips DESC
    LIMIT 30
    SETTINGS use_query_cache = 0
    """,

    # ── INNER JOIN: events × user_tiers ─────────────────────────────
    lambda: f"""
    SELECT
        u.tier,
        count() AS events,
        uniq(e.session_id) AS sessions,
        sum(e.revenue) AS revenue
    FROM synthetic_data.events AS e
    INNER JOIN synthetic_data.user_tiers AS u
        ON e.user_id = u.user_id
    WHERE e.event_date >= today() - {random.randint(3, 14)}
    GROUP BY u.tier
    ORDER BY revenue DESC
    SETTINGS use_query_cache = 0
    """,

    # ── LEFT JOIN: user_tiers × events (find inactive users) ───────
    lambda: f"""
    SELECT
        u.tier,
        count() AS total_users,
        countIf(e.user_id = 0) AS inactive_users
    FROM synthetic_data.user_tiers AS u
    LEFT JOIN (
        SELECT DISTINCT user_id
        FROM synthetic_data.events
        WHERE event_date >= today() - {random.randint(7, 30)}
    ) AS e ON u.user_id = e.user_id
    GROUP BY u.tier
    ORDER BY total_users DESC
    SETTINGS use_query_cache = 0
    """,

    # ── Self-JOIN: compare pickup vs dropoff stats per location ─────
    lambda: f"""
    SELECT
        p.pickup_location_id AS location_id,
        p.pickups,
        d.dropoffs,
        p.pickups - d.dropoffs AS net_flow
    FROM (
        SELECT pickup_location_id, count() AS pickups
        FROM nyc_taxi.trips
        WHERE pickup_date >= today() - {random.randint(3, 14)}
        GROUP BY pickup_location_id
    ) AS p
    INNER JOIN (
        SELECT dropoff_location_id, count() AS dropoffs
        FROM nyc_taxi.trips
        WHERE pickup_date >= today() - {random.randint(3, 14)}
        GROUP BY dropoff_location_id
    ) AS d ON p.pickup_location_id = d.dropoff_location_id
    ORDER BY net_flow DESC
    LIMIT 20
    SETTINGS use_query_cache = 0
    """,

    # ── LEFT SEMI JOIN: events from users who purchased ─────────────
    lambda: f"""
    SELECT
        e.country_code,
        count() AS events_from_buyers,
        uniq(e.user_id) AS buyer_count
    FROM synthetic_data.events AS e
    LEFT SEMI JOIN (
        SELECT DISTINCT user_id
        FROM synthetic_data.events
        WHERE event_type = 'purchase'
          AND event_date >= today() - {random.randint(7, 30)}
    ) AS buyers ON e.user_id = buyers.user_id
    WHERE e.event_date >= today() - {random.randint(7, 30)}
    GROUP BY e.country_code
    ORDER BY events_from_buyers DESC
    SETTINGS use_query_cache = 0
    """,

    # ── LEFT ANTI JOIN: events from users who never purchased ───────
    lambda: f"""
    SELECT
        e.device_type,
        count() AS events_from_non_buyers,
        uniq(e.user_id) AS non_buyer_count
    FROM synthetic_data.events AS e
    LEFT ANTI JOIN (
        SELECT DISTINCT user_id
        FROM synthetic_data.events
        WHERE event_type = 'purchase'
    ) AS buyers ON e.user_id = buyers.user_id
    WHERE e.event_date >= today() - {random.randint(3, 14)}
    GROUP BY e.device_type
    ORDER BY events_from_non_buyers DESC
    SETTINGS use_query_cache = 0
    """,

    # ── Cross-table JOIN: top taxi zones × web analytics domains ────
    lambda: f"""
    SELECT
        t.borough,
        w.domain,
        t.trip_count,
        w.pageview_count
    FROM (
        SELECT l.borough, count() AS trip_count
        FROM nyc_taxi.trips AS tr
        INNER JOIN nyc_taxi.locations AS l ON tr.pickup_location_id = l.location_id
        WHERE tr.pickup_date >= today() - {random.randint(7, 30)}
        GROUP BY l.borough
        ORDER BY trip_count DESC
        LIMIT 5
    ) AS t
    CROSS JOIN (
        SELECT domain, count() AS pageview_count
        FROM web_analytics.pageviews
        WHERE event_date >= today() - {random.randint(7, 30)}
        GROUP BY domain
        ORDER BY pageview_count DESC
        LIMIT 5
    ) AS w
    ORDER BY t.trip_count DESC, w.pageview_count DESC
    SETTINGS use_query_cache = 0
    """,

    # ── JOIN with window function: user tier revenue ranking ────────
    lambda: f"""
    SELECT
        u.tier,
        e.user_id,
        e.total_revenue,
        row_number() OVER (PARTITION BY u.tier ORDER BY e.total_revenue DESC) AS rank_in_tier
    FROM (
        SELECT user_id, sum(revenue) AS total_revenue
        FROM synthetic_data.events
        WHERE event_date >= today() - {random.randint(7, 30)}
          AND revenue > 0
        GROUP BY user_id
    ) AS e
    INNER JOIN synthetic_data.user_tiers AS u ON e.user_id = u.user_id
    ORDER BY u.tier, rank_in_tier
    LIMIT 100
    SETTINGS use_query_cache = 0
    """,

    # ── ASOF JOIN: match events to closest user signup ──────────────
    lambda: f"""
    SELECT
        u.tier,
        count() AS events,
        avg(e.duration_ms) AS avg_duration
    FROM synthetic_data.events AS e
    ASOF LEFT JOIN synthetic_data.user_tiers AS u
        ON e.user_id = u.user_id AND e.event_date >= u.signup_date
    WHERE e.event_date >= today() - {random.randint(3, 14)}
    GROUP BY u.tier
    ORDER BY events DESC
    SETTINGS use_query_cache = 0
    """,

    # ── Multi-table: borough revenue by user tier ───────────────────
    lambda: f"""
    SELECT
        l.borough,
        u.tier,
        count() AS trip_count,
        avg(t.total_amount) AS avg_fare
    FROM nyc_taxi.trips AS t
    INNER JOIN nyc_taxi.locations AS l ON t.pickup_location_id = l.location_id
    INNER JOIN synthetic_data.user_tiers AS u ON t.passenger_count = u.user_id % 7
    WHERE t.pickup_date >= today() - {random.randint(3, 14)}
    GROUP BY l.borough, u.tier
    ORDER BY trip_count DESC
    LIMIT 40
    SETTINGS use_query_cache = 0
    """,
]

# Fast queries for background noise
FAST_QUERIES = [
    # These should complete quickly even on billion-row tables.
    # count() reads part metadata, no scan needed.
    "SELECT count() FROM synthetic_data.events",
    "SELECT count() FROM nyc_taxi.trips",
    "SELECT count() FROM web_analytics.pageviews",
    # min/max on ORDER BY columns use index skipping
    "SELECT min(event_date), max(event_date) FROM synthetic_data.events",
    "SELECT min(pickup_date), max(pickup_date) FROM nyc_taxi.trips",
    "SELECT min(event_date), max(event_date) FROM web_analytics.pageviews",
    # Narrow PK-aligned scans with LIMIT
    "SELECT event_type, count() FROM synthetic_data.events WHERE event_date = today() GROUP BY event_type",
    "SELECT payment_type, count() FROM nyc_taxi.trips WHERE pickup_date = today() GROUP BY payment_type",
    "SELECT domain, count() FROM web_analytics.pageviews WHERE event_date = today() GROUP BY domain",
    # Point lookups on ORDER BY prefix
    "SELECT count() FROM synthetic_data.events WHERE event_date = today() AND user_id = 42",
    "SELECT count() FROM nyc_taxi.trips WHERE pickup_date = today() AND pickup_location_id = 100",
    "SELECT count() FROM web_analytics.pageviews WHERE domain = 'example.com' AND event_date = today()",
]


def _extract_table(query: str) -> str:
    """Extract the primary table name (db.table) from a SQL query."""
    import re
    # Match FROM db.table or FROM s3('...')
    m = re.search(r'\bFROM\s+(s3\s*\(|(\w+\.\w+))', query, re.IGNORECASE)
    if m:
        if m.group(2):
            return m.group(2)
        return "s3(...)"
    return "?"


def _make_client(host: str, port: int, **conn_kwargs) -> Client:
    """Create a clickhouse-driver Client, suppressing socket shutdown noise."""
    return Client(host=host, port=port, **conn_kwargs)


def _is_connection_error(e: Exception) -> bool:
    """Check if an exception is a connection/socket error that warrants reconnect."""
    msg = str(e).lower()
    return any(tok in msg for tok in [
        'socket', 'connection reset', 'broken pipe', 'eof',
        'errno 57', 'errno 54', 'errno 104', 'connection refused',
        'unexpected eof', 'connection was lost',
    ])


def _is_routing_error(e: Exception) -> bool:
    """Check if an exception looks like a cluster routing issue.

    In a multi-shard cluster behind a load-balancing Service (e.g. K8s
    NodePort), different connections may land on different shards.  A shard
    that doesn't host a particular Replicated database will return Code 81
    (UNKNOWN_DATABASE) or Code 60 (UNKNOWN_TABLE).  Reconnecting gives us
    a chance to reach the right shard.
    """
    msg = str(e)
    return 'Code: 81' in msg or 'Code: 60' in msg


# Track which settings are restricted so we only try them once
_restricted_settings_detected = False

# Global stop event — set on Ctrl+C so workers exit their loops
_stop_event = threading.Event()


def run_query(client_holder: list, host: str, port: int, conn_kwargs: dict,
              query: str, query_type: str) -> None:
    """Execute a single query, reconnecting on socket errors.

    client_holder is a 1-element list so we can swap the client on reconnect.
    """
    if _stop_event.is_set():
        return
    global _restricted_settings_detected
    table = _extract_table(query)
    # Prepend a comment so queries are easy to find in system.query_log
    tagged_query = f"/* run-queries type:{query_type.lower()} table:{table} */ {query}"
    start = time.time()

    # Settings that enhance observability but may be restricted on managed services
    extra_settings: dict | None = None if _restricted_settings_detected else {
        'opentelemetry_start_trace_probability': 0.01,
        'opentelemetry_trace_processors': 0,
        'memory_profiler_sample_probability': 1,
        'max_untracked_memory': 1,
        'log_query_threads': 1,
    }

    for attempt in range(4):
        try:
            if extra_settings:
                result = client_holder[0].execute(tagged_query, settings=extra_settings)
            else:
                result = client_holder[0].execute(tagged_query)
            elapsed = time.time() - start
            rows = len(result) if result else 0
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {query_type:5} {table:30} {elapsed:.2f}s ({rows} rows)")
            return
        except Exception as e:
            err_str = str(e)
            # Setting rejected — remember and retry without them
            if 'Code: 452' in err_str or 'should not be changed' in err_str:
                _restricted_settings_detected = True
                extra_settings = None
                continue
            # Connection or routing error — reconnect and retry
            # In a cluster behind a load balancer, different connections may
            # land on different shards; retry up to 3 times to reach the
            # shard that hosts the target database.
            if (_is_connection_error(e) or _is_routing_error(e)) and attempt < 3:
                try:
                    client_holder[0].disconnect()
                except Exception:
                    pass
                try:
                    client_holder[0] = _make_client(host, port, **conn_kwargs)
                except Exception:
                    pass
                continue
            # Real query error
            elapsed = time.time() - start
            error_msg = err_str[:80]
            query_preview = query.replace('\n', ' ')[:100]
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {query_type:5} {table:30} FAILED {elapsed:.2f}s: {error_msg}")
            print(f"    SQL: {query_preview}...")
            return


def slow_query_worker(host: str, port: int, interval: float, queries: list[str] | None = None, **conn_kwargs) -> None:
    """Worker thread that runs slow queries."""
    client_holder = [_make_client(host, port, **conn_kwargs)]
    query_pool = queries or SLOW_QUERIES
    while not _stop_event.is_set():
        query = random.choice(query_pool)
        run_query(client_holder, host, port, conn_kwargs, query, "SLOW")
        _stop_event.wait(interval + random.uniform(0, interval * 0.5))


def s3_parquet_worker(host: str, port: int, interval: float, queries: list[str] | None = None, **conn_kwargs) -> None:
    """Worker thread that runs S3 parquet queries (network + parsing tests)."""
    client_holder = [_make_client(host, port, **conn_kwargs)]
    query_pool = queries or S3_PARQUET_QUERIES
    while not _stop_event.is_set():
        query = random.choice(query_pool)
        run_query(client_holder, host, port, conn_kwargs, query, "S3")
        _stop_event.wait(interval + random.uniform(0, interval * 0.5))


def pk_pattern_worker(host: str, port: int, interval: float, generators: list | None = None, **conn_kwargs) -> None:
    """Worker thread that runs PK-efficient/inefficient queries for analytics diagnostics.
    Each execution randomizes literal values to produce different parameters
    under the same normalized_query_hash."""
    client_holder = [_make_client(host, port, **conn_kwargs)]
    gen_pool = generators or PK_EFFICIENT_QUERY_GENERATORS
    while not _stop_event.is_set():
        generator = random.choice(gen_pool)
        query = generator()
        run_query(client_holder, host, port, conn_kwargs, query, "PK")
        _stop_event.wait(interval + random.uniform(0, interval * 0.3))


def fast_query_worker(host: str, port: int, interval: float, queries: list[str] | None = None, **conn_kwargs) -> None:
    """Worker thread that runs fast queries."""
    client_holder = [_make_client(host, port, **conn_kwargs)]
    query_pool = queries or FAST_QUERIES
    while not _stop_event.is_set():
        query = random.choice(query_pool)
        run_query(client_holder, host, port, conn_kwargs, query, "FAST")
        _stop_event.wait(interval + random.uniform(0, interval * 0.3))


def join_query_worker(host: str, port: int, interval: float, generators: list | None = None, **conn_kwargs) -> None:
    """Worker thread that runs JOIN queries to exercise various join strategies."""
    client_holder = [_make_client(host, port, **conn_kwargs)]
    gen_pool = generators or JOIN_QUERY_GENERATORS
    while not _stop_event.is_set():
        generator = random.choice(gen_pool)
        query = generator()
        run_query(client_holder, host, port, conn_kwargs, query, "JOIN")
        _stop_event.wait(interval + random.uniform(0, interval * 0.3))


def settings_variation_worker(host: str, port: int, interval: float, generators: list | None = None, **conn_kwargs) -> None:
    """Worker thread that runs queries with randomized max_threads settings.
    Same normalized SQL each time but different SETTINGS values, useful for
    tracking query performance across parameter changes."""
    client_holder = [_make_client(host, port, **conn_kwargs)]
    gen_pool = generators or SETTINGS_VARIATION_GENERATORS
    while not _stop_event.is_set():
        generator = random.choice(gen_pool)
        query = generator()
        run_query(client_holder, host, port, conn_kwargs, query, "SVAR")
        _stop_event.wait(interval + random.uniform(0, interval * 0.3))


def _filter_queries_by_tables(queries: list[str], available_tables: set[str]) -> list[str]:
    """Filter a list of SQL strings, keeping only those whose FROM table exists."""
    import re
    filtered = []
    for q in queries:
        m = re.search(r'\bFROM\s+(s3\s*\(|(\w+\.\w+))', q, re.IGNORECASE)
        if not m:
            filtered.append(q)  # can't determine table, keep it
            continue
        if m.group(2):
            table = m.group(2).lower()
            if table in available_tables:
                filtered.append(q)
        else:
            # s3() function query — handled separately
            filtered.append(q)
    return filtered


def _filter_generators_by_tables(generators: list, available_tables: set[str]) -> list:
    """Filter query generator lambdas by sampling one query and checking tables.

    For JOIN queries that reference multiple tables, ALL referenced tables
    must be available for the generator to be included.
    """
    import re
    filtered = []
    for gen in generators:
        sample = gen()
        tables = re.findall(r'\bFROM\s+(\w+\.\w+)', sample, re.IGNORECASE)
        tables += re.findall(r'\bJOIN\s+(\w+\.\w+)', sample, re.IGNORECASE)
        if not tables:
            filtered.append(gen)
            continue
        unique_tables = {t.lower() for t in tables}
        if unique_tables.issubset(available_tables):
            filtered.append(gen)
    return filtered


def _detect_available_tables(client: Client) -> set[str]:
    """Query system.tables to find which test databases/tables exist.

    In a cluster behind a load balancer, system.tables on one shard may not
    list databases that only exist on another shard.  We query
    system.databases first to see what's visible from this connection.
    """
    try:
        rows = client.execute(
            "SELECT database || '.' || name FROM system.tables "
            "WHERE database IN ('synthetic_data', 'nyc_taxi', 'uk_price_paid', 'web_analytics')"
        )
        return {r[0].lower() for r in rows}
    except Exception as e:
        print(f"  ⚠ Failed to detect tables: {e}")
        return set()


def _env_int(key: str, default: str) -> int:
    """Read an int from env, allowing underscores (e.g. 100_000_000)."""
    return int(os.environ.get(key, default).replace("_", ""))


def _load_env_file(env_file: str | None = None) -> str | None:
    """Load .env file into os.environ.

    When the file is explicitly requested (via --env-file or $CH_ENV_FILE),
    its values override any existing environment variables so the user's
    intent is always honoured.  When falling back to the default repo-root
    .env, existing env vars take precedence (override=False).

    Resolution order:
      1. Explicit --env-file path
      2. $CH_ENV_FILE environment variable
      3. .env in the repo root (infra/scripts/../../.env)

    Returns the resolved path if a file was loaded, else None.
    """
    from dotenv import load_dotenv

    explicit = False
    if env_file:
        path = env_file
        explicit = True
    elif os.environ.get("CH_ENV_FILE"):
        path = os.environ["CH_ENV_FILE"]
        explicit = True
    else:
        path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")

    path = os.path.abspath(path)
    if os.path.isfile(path):
        load_dotenv(path, override=explicit)
        return path
    return None


def _obfuscate(password: str) -> str:
    """Return an obfuscated password for display."""
    if not password:
        return "(empty)"
    if len(password) <= 4:
        return "****"
    return password[:2] + "*" * (len(password) - 4) + password[-2:]


def _print_resolved_config(args: argparse.Namespace, env_path: str | None) -> None:
    """Print which .env was loaded and the final resolved connection variables."""
    print("─" * 50)
    if env_path:
        print(f"  Env file:  {env_path}")
    else:
        print("  Env file:  (none found)")
    print(f"  CH_HOST:     {args.host}")
    print(f"  CH_PORT:     {args.port}")
    print(f"  CH_USER:     {args.user}")
    print(f"  CH_PASSWORD: {_obfuscate(args.password)}")
    print(f"  CH_SECURE:   {args.secure}")
    print("─" * 50)


def main():
    # Pre-parse --env-file so .env is loaded before argparse reads defaults
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--env-file", default=None, help="Path to .env file (default: $CH_ENV_FILE or <repo>/.env)")
    pre_args, _ = pre.parse_known_args()
    env_path = _load_env_file(pre_args.env_file)

    parser = argparse.ArgumentParser(description="Run continuous queries for TraceHouse testing")
    parser.add_argument("--env-file", default=None, help="Path to .env file (default: $CH_ENV_FILE or <repo>/.env)")
    parser.add_argument("--host", default=os.environ.get("CH_HOST", "localhost"), help="ClickHouse host (default: $CH_HOST or localhost)")
    parser.add_argument("--port", type=int, default=_env_int("CH_PORT", "9000"), help="ClickHouse native port (default: $CH_PORT or 9000)")
    parser.add_argument("--user", default=os.environ.get("CH_USER", "default"), help="ClickHouse user (default: $CH_USER or default)")
    parser.add_argument("--password", default=os.environ.get("CH_PASSWORD", ""), help="ClickHouse password (default: $CH_PASSWORD or empty)")
    parser.add_argument("--secure", action="store_true", default=os.environ.get("CH_SECURE", "").lower() in ("1", "true", "yes"), help="Use TLS (default: $CH_SECURE or false)")
    parser.add_argument("--slow-interval", type=float, default=float(os.environ.get("CH_QUERY_SLOW_INTERVAL", "1.0")), help="Interval between slow queries (default: $CH_QUERY_SLOW_INTERVAL or 1)")
    parser.add_argument("--fast-interval", type=float, default=float(os.environ.get("CH_QUERY_FAST_INTERVAL", "10.0")), help="Interval between fast queries (default: $CH_QUERY_FAST_INTERVAL or 10)")
    parser.add_argument("--s3-interval", type=float, default=float(os.environ.get("CH_QUERY_S3_INTERVAL", "30.0")), help="Interval between S3 parquet queries (default: $CH_QUERY_S3_INTERVAL or 30)")
    parser.add_argument("--pk-interval", type=float, default=float(os.environ.get("CH_QUERY_PK_INTERVAL", "5.0")), help="Interval between PK pattern queries (default: $CH_QUERY_PK_INTERVAL or 5)")
    parser.add_argument("--slow-workers", type=int, default=_env_int("CH_QUERY_SLOW_WORKERS", "5"), help="Number of slow query workers (default: $CH_QUERY_SLOW_WORKERS or 5)")
    parser.add_argument("--fast-workers", type=int, default=_env_int("CH_QUERY_FAST_WORKERS", "1"), help="Number of fast query workers (default: $CH_QUERY_FAST_WORKERS or 1)")
    parser.add_argument("--s3-workers", type=int, default=_env_int("CH_QUERY_S3_WORKERS", "2"), help="Number of S3 parquet query workers (default: $CH_QUERY_S3_WORKERS or 2)")
    parser.add_argument("--pk-workers", type=int, default=_env_int("CH_QUERY_PK_WORKERS", "2"), help="Number of PK pattern query workers (default: $CH_QUERY_PK_WORKERS or 2)")
    parser.add_argument("--join-interval", type=float, default=float(os.environ.get("CH_QUERY_JOIN_INTERVAL", "3.0")), help="Interval between JOIN queries (default: $CH_QUERY_JOIN_INTERVAL or 3)")
    parser.add_argument("--join-workers", type=int, default=_env_int("CH_QUERY_JOIN_WORKERS", "2"), help="Number of JOIN query workers (default: $CH_QUERY_JOIN_WORKERS or 2)")
    parser.add_argument("--settings-interval", type=float, default=float(os.environ.get("CH_QUERY_SETTINGS_INTERVAL", "5.0")), help="Interval between settings-variation queries (default: $CH_QUERY_SETTINGS_INTERVAL or 5)")
    parser.add_argument("--settings-workers", type=int, default=_env_int("CH_QUERY_SETTINGS_WORKERS", "1"), help="Number of settings-variation query workers (default: $CH_QUERY_SETTINGS_WORKERS or 1)")
    args = parser.parse_args()

    conn_kwargs = dict(user=args.user, password=args.password, secure=args.secure)

    # ── Probe capabilities ──────────────────────────────────────────
    _print_resolved_config(args, env_path)
    print(f"\nConnecting to {args.host}:{args.port}...")
    probe_client = Client(host=args.host, port=args.port, **conn_kwargs)

    print("Probing server capabilities...")
    caps = probe(probe_client)
    print(caps.summary())

    available_tables = _detect_available_tables(probe_client)
    print(f"\n  Available tables:      {', '.join(sorted(available_tables)) or '(none found)'}")
    probe_client.disconnect()

    # ── Filter queries based on what's actually available ───────────
    slow_queries = _filter_queries_by_tables(SLOW_QUERIES, available_tables)
    fast_queries = _filter_queries_by_tables(FAST_QUERIES, available_tables)
    pk_generators = _filter_generators_by_tables(PK_EFFICIENT_QUERY_GENERATORS, available_tables)
    join_generators = _filter_generators_by_tables(JOIN_QUERY_GENERATORS, available_tables)
    settings_generators = _filter_generators_by_tables(SETTINGS_VARIATION_GENERATORS, available_tables)

    # S3 queries: only if s3() function works
    s3_queries = S3_PARQUET_QUERIES if caps.has_s3_function else []

    print(f"\n  Slow queries:          {len(slow_queries)}/{len(SLOW_QUERIES)} available")
    print(f"  Fast queries:          {len(fast_queries)}/{len(FAST_QUERIES)} available")
    print(f"  PK query generators:   {len(pk_generators)}/{len(PK_EFFICIENT_QUERY_GENERATORS)} available")
    print(f"  JOIN query generators: {len(join_generators)}/{len(JOIN_QUERY_GENERATORS)} available")
    print(f"  S3 parquet queries:    {len(s3_queries)}/{len(S3_PARQUET_QUERIES)} available")
    print(f"  Settings-variation:    {len(settings_generators)}/{len(SETTINGS_VARIATION_GENERATORS)} available")

    if not slow_queries and not fast_queries and not pk_generators and not join_generators and not settings_generators and not s3_queries:
        print("\n✗ No queries available — have you loaded test data? (just load-data)")
        return

    # ── Start workers ───────────────────────────────────────────────
    print(f"\nStarting workers:")
    threads = []

    if slow_queries:
        actual_slow = min(args.slow_workers, len(slow_queries))
        print(f"  Slow:  {actual_slow} workers (interval: {args.slow_interval}s)")
        for i in range(actual_slow):
            t = threading.Thread(
                target=slow_query_worker,
                args=(args.host, args.port, args.slow_interval),
                kwargs={**conn_kwargs, 'queries': slow_queries},
                daemon=True,
                name=f"slow-worker-{i}"
            )
            t.start()
            threads.append(t)
    else:
        print("  Slow:  skipped (no matching tables)")

    if fast_queries:
        print(f"  Fast:  {args.fast_workers} workers (interval: {args.fast_interval}s)")
        for i in range(args.fast_workers):
            t = threading.Thread(
                target=fast_query_worker,
                args=(args.host, args.port, args.fast_interval),
                kwargs={**conn_kwargs, 'queries': fast_queries},
                daemon=True,
                name=f"fast-worker-{i}"
            )
            t.start()
            threads.append(t)
    else:
        print("  Fast:  skipped (no matching tables)")

    if s3_queries:
        print(f"  S3:    {args.s3_workers} workers (interval: {args.s3_interval}s)")
        for i in range(args.s3_workers):
            t = threading.Thread(
                target=s3_parquet_worker,
                args=(args.host, args.port, args.s3_interval),
                kwargs={**conn_kwargs, 'queries': s3_queries},
                daemon=True,
                name=f"s3-worker-{i}"
            )
            t.start()
            threads.append(t)
    else:
        print("  S3:    skipped (s3() function not available)")

    if pk_generators:
        print(f"  PK:    {args.pk_workers} workers (interval: {args.pk_interval}s)")
        for i in range(args.pk_workers):
            t = threading.Thread(
                target=pk_pattern_worker,
                args=(args.host, args.port, args.pk_interval),
                kwargs={**conn_kwargs, 'generators': pk_generators},
                daemon=True,
                name=f"pk-worker-{i}"
            )
            t.start()
            threads.append(t)
    else:
        print("  PK:    skipped (no matching tables)")

    if join_generators:
        print(f"  JOIN:  {args.join_workers} workers (interval: {args.join_interval}s)")
        for i in range(args.join_workers):
            t = threading.Thread(
                target=join_query_worker,
                args=(args.host, args.port, args.join_interval),
                kwargs={**conn_kwargs, 'generators': join_generators},
                daemon=True,
                name=f"join-worker-{i}"
            )
            t.start()
            threads.append(t)
    else:
        print("  JOIN:  skipped (no matching tables / dimension tables)")

    if settings_generators:
        print(f"  SVAR:  {args.settings_workers} workers (interval: {args.settings_interval}s)")
        for i in range(args.settings_workers):
            t = threading.Thread(
                target=settings_variation_worker,
                args=(args.host, args.port, args.settings_interval),
                kwargs={**conn_kwargs, 'generators': settings_generators},
                daemon=True,
                name=f"settings-worker-{i}"
            )
            t.start()
            threads.append(t)
    else:
        print("  SVAR:  skipped (no matching tables)")

    print(f"\n{len(threads)} workers running. Press Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping query runner...")
        _stop_event.set()

        # Kill any in-flight queries tagged by this script
        try:
            kill_client = Client(host=args.host, port=args.port, **conn_kwargs)
            killed = kill_client.execute(
                "KILL QUERY WHERE query LIKE '%/* run-queries %' AND user = currentUser() ASYNC"
            )
            if killed:
                print(f"  Cancelled {len(killed)} in-flight ClickHouse queries")
            kill_client.disconnect()
        except Exception:
            pass

        # Wait briefly for daemon threads to finish their current iteration
        for t in threads:
            t.join(timeout=2)

        print("✓ Stopped")


if __name__ == "__main__":
    main()
