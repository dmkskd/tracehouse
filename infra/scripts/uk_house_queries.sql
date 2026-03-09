-- UK House Prices Example Queries
-- These are common ClickHouse example queries adapted for our synthetic uk_price_paid data
-- Run with: clickhouse-client --multiquery < uk_house_queries.sql

-- Enable OpenTelemetry tracing for these queries
SET opentelemetry_start_trace_probability = 1;
SET opentelemetry_trace_processors = 1;

-- 1. Average price by year
SELECT
    toYear(date) AS year,
    round(avg(price)) AS avg_price,
    formatReadableQuantity(count()) AS sales
FROM uk_price_paid.uk_price_paid
GROUP BY year
ORDER BY year DESC;

-- 2. Average price by property type
SELECT
    type,
    round(avg(price)) AS avg_price,
    formatReadableQuantity(count()) AS count
FROM uk_price_paid.uk_price_paid
GROUP BY type
ORDER BY avg_price DESC;

-- 3. Top 10 most expensive postcodes
SELECT
    postcode1,
    round(avg(price)) AS avg_price,
    count() AS sales
FROM uk_price_paid.uk_price_paid
GROUP BY postcode1
ORDER BY avg_price DESC
LIMIT 10;

-- 4. Price trends by town (heavy query - good for testing)
SELECT
    town,
    toYear(date) AS year,
    round(avg(price)) AS avg_price,
    count() AS sales,
    round(median(price)) AS median_price,
    max(price) AS max_price
FROM uk_price_paid.uk_price_paid
GROUP BY town, year
HAVING sales > 100
ORDER BY town, year;

-- 5. New builds vs existing properties
SELECT
    is_new,
    type,
    round(avg(price)) AS avg_price,
    count() AS count
FROM uk_price_paid.uk_price_paid
GROUP BY is_new, type
ORDER BY is_new, type;

-- 6. Freehold vs Leasehold by county
SELECT
    county,
    duration,
    round(avg(price)) AS avg_price,
    count() AS count
FROM uk_price_paid.uk_price_paid
GROUP BY county, duration
ORDER BY county, duration;

-- 7. Price distribution (histogram)
SELECT
    floor(price / 50000) * 50000 AS price_bucket,
    count() AS count,
    bar(count(), 0, 1000000, 50) AS bar
FROM uk_price_paid.uk_price_paid
GROUP BY price_bucket
ORDER BY price_bucket;

-- 8. Most expensive streets (heavy aggregation)
SELECT
    street,
    town,
    round(avg(price)) AS avg_price,
    count() AS sales
FROM uk_price_paid.uk_price_paid
GROUP BY street, town
HAVING sales > 50
ORDER BY avg_price DESC
LIMIT 20;

-- 9. Year-over-year price change by postcode (window function)
SELECT
    postcode1,
    year,
    avg_price,
    round((avg_price - lag(avg_price) OVER (PARTITION BY postcode1 ORDER BY year)) / 
          lag(avg_price) OVER (PARTITION BY postcode1 ORDER BY year) * 100, 2) AS yoy_change_pct
FROM (
    SELECT
        postcode1,
        toYear(date) AS year,
        round(avg(price)) AS avg_price
    FROM uk_price_paid.uk_price_paid
    GROUP BY postcode1, year
    HAVING count() > 100
)
ORDER BY postcode1, year;

-- 10. Complex aggregation - price percentiles by type and year
SELECT
    type,
    toYear(date) AS year,
    count() AS sales,
    round(quantile(0.25)(price)) AS p25,
    round(quantile(0.50)(price)) AS median,
    round(quantile(0.75)(price)) AS p75,
    round(quantile(0.90)(price)) AS p90,
    round(quantile(0.99)(price)) AS p99
FROM uk_price_paid.uk_price_paid
GROUP BY type, year
ORDER BY type, year;
