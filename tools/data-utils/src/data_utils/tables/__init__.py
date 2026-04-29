from __future__ import annotations

from typing import TYPE_CHECKING

from .synthetic_data import SyntheticData
from .nyc_taxi import NycTaxi
from .uk_house_prices import UkHousePrices
from .web_analytics import WebAnalytics
from .replacing_merge import ReplacingMerge
from .iceberg_nyc_taxi import IcebergNycTaxi
from .helpers import ProgressTracker
from .protocol import Dataset, InsertConfig, InsertMode, QuerySet

if TYPE_CHECKING:
    from data_utils.capabilities import Capabilities

# Single registry of all dataset classes. Add new datasets here — generate,
# drop, and queries will all pick them up automatically.
ALL_DATASET_CLASSES: list[type] = [
    SyntheticData,
    NycTaxi,
    UkHousePrices,
    WebAnalytics,
    ReplacingMerge,
    IcebergNycTaxi,
]


def build_all_datasets(
    caps: Capabilities | None = None,
    ttl_interval: int = 0,
) -> list[Dataset]:
    """Instantiate every registered dataset with the right constructor args.

    This is the single place that knows how to construct each plugin.
    Used by generate, drop, and queries CLIs.
    """
    return [
        SyntheticData(caps=caps, ttl_interval=ttl_interval),
        NycTaxi(caps=caps, ttl_interval=ttl_interval),
        UkHousePrices(caps=caps, ttl_interval=ttl_interval),
        WebAnalytics(caps=caps, ttl_interval=ttl_interval),
        ReplacingMerge(caps=caps, ttl_interval=ttl_interval),
        IcebergNycTaxi(caps=caps, ttl_interval=ttl_interval),
    ]


DATASET_ALIASES: dict[str, str] = {
    'synthetic': 'synthetic_only',
    'taxi': 'taxi_only',
    'uk': 'uk_only',
    'web': 'web_only',
    'replacing': 'replacing_only',
    'iceberg_taxi': 'iceberg_taxi_only',
    'iceberg': 'iceberg_taxi_only',
}


def list_datasets() -> None:
    """Print all registered datasets and exit."""
    datasets = build_all_datasets()
    alias_by_flag = {v: k for k, v in DATASET_ALIASES.items()}
    print(f"{'ALIAS':<12} {'DATABASE':<20} {'FLAG'}")
    print(f"{'─' * 12} {'─' * 20} {'─' * 16}")
    for ds in datasets:
        alias = alias_by_flag.get(ds.flag, '?')
        print(f"{alias:<12} {ds.name:<20} --{ds.flag.replace('_', '-')}")


__all__ = [
    "Dataset", "InsertConfig", "InsertMode", "QuerySet",
    "SyntheticData", "NycTaxi", "UkHousePrices", "WebAnalytics",
    "ReplacingMerge", "IcebergNycTaxi",
    "ALL_DATASET_CLASSES", "build_all_datasets",
    "DATASET_ALIASES", "list_datasets",
    "ProgressTracker",
]
