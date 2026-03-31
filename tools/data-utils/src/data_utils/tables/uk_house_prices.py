"""uk_price_paid.uk_price_paid — UK house price data."""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

from clickhouse_driver import Client
from .helpers import (
    retry_on_drop_race, create_database, drop_database,
    check_existing_rows, run_batched_insert, ttl_clause, ttl_settings,
    partition_clause, is_sharded,
)
from data_utils.capabilities import Capabilities
from .protocol import InsertMode

if TYPE_CHECKING:
    from .helpers import ProgressTracker
    from .protocol import InsertConfig, QuerySet, Dataset

# ── Reference data ──────────────────────────────────────────────────

POSTCODES1 = [
    'SW1', 'SW3', 'SW7', 'W1', 'W8', 'WC1', 'WC2', 'EC1', 'EC2', 'EC3', 'EC4',
    'N1', 'N2', 'N3', 'NW1', 'NW3', 'NW6', 'NW8', 'SE1', 'SE10', 'SE11',
    'E1', 'E2', 'E3', 'E14', 'E15', 'E16', 'E17', 'E18',
    'M1', 'M2', 'M3', 'M4', 'M14', 'M15', 'M16', 'M20', 'M21',
    'B1', 'B2', 'B3', 'B4', 'B5', 'B15', 'B16', 'B17', 'B18',
    'L1', 'L2', 'L3', 'L4', 'L5', 'L15', 'L17', 'L18',
    'G1', 'G2', 'G3', 'G4', 'G11', 'G12', 'G13', 'G14',
    'EH1', 'EH2', 'EH3', 'EH4', 'EH5', 'EH6', 'EH7', 'EH8',
    'CF1', 'CF2', 'CF3', 'CF10', 'CF11', 'CF14', 'CF15',
    'BS1', 'BS2', 'BS3', 'BS4', 'BS5', 'BS6', 'BS7', 'BS8',
    'LS1', 'LS2', 'LS3', 'LS4', 'LS5', 'LS6', 'LS7', 'LS8',
    'S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', 'S10', 'S11',
    'OX1', 'OX2', 'OX3', 'OX4', 'CB1', 'CB2', 'CB3', 'CB4',
]

TOWNS = [
    'LONDON', 'MANCHESTER', 'BIRMINGHAM', 'LEEDS', 'GLASGOW', 'SHEFFIELD',
    'LIVERPOOL', 'EDINBURGH', 'BRISTOL', 'CARDIFF', 'LEICESTER', 'COVENTRY',
    'NOTTINGHAM', 'NEWCASTLE UPON TYNE', 'SUNDERLAND', 'BRIGHTON', 'HULL',
    'PLYMOUTH', 'STOKE-ON-TRENT', 'WOLVERHAMPTON', 'DERBY', 'SOUTHAMPTON',
    'PORTSMOUTH', 'OXFORD', 'CAMBRIDGE', 'YORK', 'BATH', 'EXETER', 'NORWICH',
    'READING', 'LUTON', 'MILTON KEYNES', 'NORTHAMPTON', 'PETERBOROUGH',
]

COUNTIES = [
    'GREATER LONDON', 'GREATER MANCHESTER', 'WEST MIDLANDS', 'WEST YORKSHIRE',
    'SOUTH YORKSHIRE', 'MERSEYSIDE', 'TYNE AND WEAR', 'AVON', 'SOUTH GLAMORGAN',
    'LEICESTERSHIRE', 'NOTTINGHAMSHIRE', 'DERBYSHIRE', 'HAMPSHIRE', 'KENT',
    'ESSEX', 'SURREY', 'HERTFORDSHIRE', 'OXFORDSHIRE', 'CAMBRIDGESHIRE',
    'NORTH YORKSHIRE', 'SOMERSET', 'DEVON', 'NORFOLK', 'SUFFOLK', 'BERKSHIRE',
    'BEDFORDSHIRE', 'BUCKINGHAMSHIRE', 'NORTHAMPTONSHIRE', 'LINCOLNSHIRE',
]

STREETS = [
    'HIGH STREET', 'STATION ROAD', 'MAIN STREET', 'PARK ROAD', 'CHURCH LANE',
    'MILL LANE', 'SCHOOL LANE', 'THE GREEN', 'KINGS ROAD', 'QUEENS ROAD',
    'VICTORIA ROAD', 'ALBERT ROAD', 'GEORGE STREET', 'LONDON ROAD', 'YORK ROAD',
    'MANOR ROAD', 'GROVE ROAD', 'CHURCH STREET', 'PARK AVENUE', 'WEST STREET',
    'NORTH STREET', 'SOUTH STREET', 'EAST STREET', 'NEW ROAD', 'BRIDGE STREET',
    'MARKET STREET', 'CASTLE STREET', 'WATER LANE', 'MILL ROAD', 'HILL ROAD',
]


def _rand_postcode1() -> str:
    return random.choice(['SW1', 'SW3', 'SW7', 'W1', 'W8', 'EC1', 'EC2', 'N1', 'SE1', 'NW1'])

def _rand_postcodes(n: int = 5) -> str:
    codes = random.sample(['SW1', 'SW3', 'SW7', 'W1', 'W8', 'EC1', 'EC2', 'N1', 'SE1', 'NW1', 'E1', 'WC1', 'WC2'], min(n, 13))
    return ', '.join(f"'{c}'" for c in codes)

def _rand_postcode2() -> str:
    return random.choice(['1AA', '2AB', '3BC', '4CD', '5DE', '6EF', '7FG', '8GH', '9HJ'])

def _rand_year_range() -> tuple[str, str]:
    start_year = random.randint(2020, 2025)
    return f'{start_year}-01-01', f'{start_year}-07-01'

def _rand_town() -> str:
    return random.choice(['LONDON', 'MANCHESTER', 'BIRMINGHAM', 'LEEDS', 'BRISTOL', 'LIVERPOOL', 'EDINBURGH'])

def _rand_county() -> str:
    return random.choice(['GREATER LONDON', 'GREATER MANCHESTER', 'WEST MIDLANDS', 'WEST YORKSHIRE', 'AVON', 'MERSEYSIDE'])

def _rand_limit() -> int:
    return random.choice([10, 20, 30, 50, 100])

def _rand_max_threads() -> int:
    return random.choice([1, 2, 4, 8, 16])


def drop_uk_house_prices(client: Client, caps: Capabilities | None = None) -> None:
    print("Dropping uk_price_paid...")
    sharded, cluster = is_sharded(caps)
    cluster_name = cluster or (caps.cluster_name if caps and caps.has_cluster else "")
    client.execute("DROP TABLE IF EXISTS uk_price_paid.uk_price_paid SYNC")
    if sharded:
        client.execute("DROP TABLE IF EXISTS uk_price_paid.uk_price_paid_local SYNC")
    drop_database(client, "uk_price_paid", cluster=cluster_name)


def create_uk_house_prices(client: Client, caps: Capabilities | None = None, ttl_interval: int = 0) -> None:
    sharded, cluster = is_sharded(caps)
    replicated = caps.has_keeper if caps else False
    cluster_name = cluster or (caps.cluster_name if caps and caps.has_cluster else "")

    ttl = ttl_clause(ttl_interval)
    ttl_s = ttl_settings(ttl_interval)
    extra = f", {ttl_s}" if ttl_s else ""

    _UK_SCHEMA = """
        (
            price UInt32,
            date Date,
            postcode1 LowCardinality(String),
            postcode2 LowCardinality(String),
            type Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
            is_new UInt8,
            duration Enum8('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
            addr1 String,
            addr2 String,
            street LowCardinality(String),
            locality LowCardinality(String),
            town LowCardinality(String),
            district LowCardinality(String),
            county LowCardinality(String),
            _inserted_at DateTime DEFAULT now()
        )
    """

    part = partition_clause(ttl_interval, "toYear(date)")

    _UK_ORDER = f"""
        {part}
        ORDER BY (postcode1, postcode2, date)
        {ttl}
        SETTINGS old_parts_lifetime = 60{extra}
    """

    if sharded:
        print(f"Creating uk_price_paid database (Replicated, cluster={cluster})...")
        create_database(client, "uk_price_paid", replicated=True, cluster=cluster)

        print("Creating uk_price_paid.uk_price_paid_local (ReplicatedMergeTree, per-shard)...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS uk_price_paid.uk_price_paid_local
            {_UK_SCHEMA}
            ENGINE = ReplicatedMergeTree()
            {_UK_ORDER}
        """))
        print("Creating uk_price_paid.uk_price_paid (Distributed, sharded by postcode1)...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS uk_price_paid.uk_price_paid
            AS uk_price_paid.uk_price_paid_local
            ENGINE = Distributed('{cluster}', uk_price_paid, uk_price_paid_local, sipHash64(postcode1))
        """))
    else:
        if replicated:
            print(f"Creating uk_price_paid database (Replicated, cluster={cluster_name})...")
            create_database(client, "uk_price_paid", replicated=True, cluster=cluster_name)
            engine = "ReplicatedMergeTree()"
        else:
            print("Creating uk_price_paid database...")
            create_database(client, "uk_price_paid", replicated=False)
            engine = "MergeTree()"

        print(f"Creating uk_price_paid.uk_price_paid table (engine: {engine.split('(')[0]})...")
        retry_on_drop_race(lambda: client.execute(f"""
            CREATE TABLE IF NOT EXISTS uk_price_paid.uk_price_paid
            {_UK_SCHEMA}
            ENGINE = {engine}
            {_UK_ORDER}
        """))


def insert_uk_house_prices(
    client: Client, rows: int, partitions: int, batch_size: int,
    mode: InsertMode = InsertMode.RESUME,
    tracker: ProgressTracker | None = None, throttle_min: float = 0.0, throttle_max: float = 0.0,
) -> None:
    remaining = check_existing_rows(client, "uk_price_paid.uk_price_paid", rows, mode)
    if remaining is None:
        if tracker:
            tracker.register("uk_price_paid", rows)
            tracker.skip("uk_price_paid")
        return

    years = [(2025 - i, f"Year {2025 - i}") for i in range(partitions)]
    # Convert to (partition_key, label) format expected by run_batched_insert
    year_partitions = [(f"{y}", label) for y, label in years]

    postcodes1_sql = "['" + "','".join(POSTCODES1) + "']"
    towns_sql = "['" + "','".join(TOWNS) + "']"
    counties_sql = "['" + "','".join(COUNTIES) + "']"
    streets_sql = "['" + "','".join(STREETS) + "']"

    def build_sql(year_str: str, batch: int, bs: int, current_batch: int, year_rows: int, _offset: int) -> str:
        return f"""
            INSERT INTO uk_price_paid.uk_price_paid
            SELECT
                toUInt32(50000 + pow(rand() % 100, 2) * 150 + rand() % 100000) as price,
                toDate('{year_str}-01-01') + toIntervalDay(rand() % 365) as date,
                arrayElement({postcodes1_sql}, (rand() % {len(POSTCODES1)}) + 1) as postcode1,
                toString(rand() % 9 + 1) || arrayElement(['AA', 'AB', 'BA', 'BB', 'CA', 'DA', 'EA', 'FA', 'GA', 'HA'], (rand() % 10) + 1) as postcode2,
                toInt8(rand() % 5) as type,
                if(rand() % 10 = 0, 1, 0) as is_new,
                if(rand() % 3 = 0, 2, 1) as duration,
                toString(rand() % 200 + 1) as addr1,
                if(rand() % 3 = 0, concat('FLAT ', toString(rand() % 20 + 1)), '') as addr2,
                arrayElement({streets_sql}, (rand() % {len(STREETS)}) + 1) as street,
                '' as locality,
                arrayElement({towns_sql}, (rand() % {len(TOWNS)}) + 1) as town,
                arrayElement({towns_sql}, (rand() % {len(TOWNS)}) + 1) as district,
                arrayElement({counties_sql}, (rand() % {len(COUNTIES)}) + 1) as county,
                now() as _inserted_at
            FROM numbers({current_batch})
        """

    run_batched_insert(
        client, "uk_price_paid.uk_price_paid", remaining, year_partitions, batch_size, build_sql,
        tracker=tracker, throttle_min=throttle_min, throttle_max=throttle_max,
    )


# ── Plugin class ───────────────────────────────────────────────────


class UkHousePrices:
    """Dataset implementation for uk_price_paid."""

    name = "uk_price_paid"
    flag = "uk_only"

    def __init__(self, caps: Capabilities | None = None, ttl_interval: int = 0):
        self._caps = caps
        self._ttl_interval = ttl_interval

    def drop(self, client: Client) -> None:
        drop_uk_house_prices(client, caps=self._caps)

    def create(self, client: Client) -> None:
        create_uk_house_prices(client, caps=self._caps, ttl_interval=self._ttl_interval)

    def insert(
        self,
        client: Client,
        config: InsertConfig,
        tracker: ProgressTracker | None = None,
    ) -> None:
        insert_uk_house_prices(
            client, config.rows, config.partitions, config.batch_size,
            mode=config.mode, tracker=tracker,
            throttle_min=config.throttle_min, throttle_max=config.throttle_max,
        )

    @property
    def queries(self) -> QuerySet:
        from .protocol import QuerySet
        return QuerySet(
            pk_generators=[
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
            ],
            settings_generators=[
                # Price analysis with variable parallelism
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
            ],
        )


if TYPE_CHECKING:
    _: type[Dataset] = UkHousePrices  # satisfies Dataset
