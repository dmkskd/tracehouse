"""uk_price_paid.uk_price_paid — UK house price data."""

from __future__ import annotations

from clickhouse_driver import Client
from ._helpers import (
    engine_clause, retry_on_drop_race, create_database,
    check_existing_rows, run_batched_insert,
)

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


def drop_uk_house_prices(client: Client) -> None:
    print("Dropping uk_price_paid...")
    client.execute("DROP TABLE IF EXISTS uk_price_paid.uk_price_paid SYNC")
    client.execute("DROP DATABASE IF EXISTS uk_price_paid SYNC")


def create_uk_house_prices(client: Client, replicated: bool) -> None:
    print("Creating uk_price_paid database...")
    create_database(client, "uk_price_paid", replicated)

    engine = engine_clause(replicated)
    print(f"Creating uk_price_paid.uk_price_paid table (engine: {engine.split('(')[0]})...")
    retry_on_drop_race(lambda: client.execute(f"""
        CREATE TABLE IF NOT EXISTS uk_price_paid.uk_price_paid
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
            county LowCardinality(String)
        )
        ENGINE = {engine}
        PARTITION BY toYear(date)
        ORDER BY (postcode1, postcode2, date)
        SETTINGS old_parts_lifetime = 60
    """))


def insert_uk_house_prices(
    client: Client, rows: int, partitions: int, batch_size: int, drop: bool = False,
    tracker=None, throttle_min: float = 0.0, throttle_max: float = 0.0,
) -> None:
    remaining = check_existing_rows(client, "uk_price_paid.uk_price_paid", rows, drop)
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

    def build_sql(year_str, batch, bs, current_batch, year_rows, _offset):
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
                arrayElement({counties_sql}, (rand() % {len(COUNTIES)}) + 1) as county
            FROM numbers({current_batch})
        """

    run_batched_insert(
        client, "uk_price_paid.uk_price_paid", remaining, year_partitions, batch_size, build_sql,
        tracker=tracker, throttle_min=throttle_min, throttle_max=throttle_max,
    )
