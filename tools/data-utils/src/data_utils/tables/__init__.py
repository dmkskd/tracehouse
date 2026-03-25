from __future__ import annotations

from typing import TYPE_CHECKING

from .synthetic_data import SyntheticData
from .nyc_taxi import NycTaxi
from .uk_house_prices import UkHousePrices
from .web_analytics import WebAnalytics
from .replacing_merge import ReplacingMerge
from .helpers import ProgressTracker
from .protocol import Dataset, InsertConfig, QuerySet

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
]


def build_all_datasets(
    replicated: bool = False,
    cluster: str = "",
    caps: Capabilities | None = None,
) -> list[Dataset]:
    """Instantiate every registered dataset with the right constructor args.

    This is the single place that knows how to construct each plugin.
    Used by generate, drop, and queries CLIs.
    """
    return [
        SyntheticData(replicated=replicated, cluster=cluster),
        NycTaxi(replicated=replicated, caps=caps, cluster=cluster),
        UkHousePrices(replicated=replicated, cluster=cluster),
        WebAnalytics(caps=caps),
        ReplacingMerge(replicated=replicated, cluster=cluster),
    ]


__all__ = [
    "Dataset", "InsertConfig", "QuerySet",
    "SyntheticData", "NycTaxi", "UkHousePrices", "WebAnalytics",
    "ReplacingMerge",
    "ALL_DATASET_CLASSES", "build_all_datasets",
    "ProgressTracker",
]
