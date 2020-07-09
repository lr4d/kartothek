from __future__ import absolute_import

import copy

from kartothek.io_components.read import dispatch_metapartitions_from_factory
from kartothek.utils.ktk_adapters import (
    get_physical_partition_stats,
    metadata_factory_from_dataset,
)

__all__ = ("collect_stats_block", "get_metapartitions_for_stats", "reduce_stats")


def _fold_stats(result, stats, klee_dataset_id):
    """
    Add stats together.

    Parameters
    ----------
    result: Dict[str, Dict[str, int]]
        Result dictionary, may be empty or a result of a previous call to :meth:`_fold_stats`.
    stats: Dict[str, int]
        Statistics for a single dataset.
    klee_dataset_id: str
        Klee dataset ID for the given ``stats`` object.

    Returns
    -------
    result: Dict[str, Dict[str, int]]
        Result dictionary with ``stats`` added.
    """
    result = copy.deepcopy(result)

    if klee_dataset_id in result:
        ref = result[klee_dataset_id]
        for k, v in stats.items():
            ref[k] += v
    else:
        result[klee_dataset_id] = stats

    return result


def get_metapartitions_for_stats(datasets):
    """
    Get all metapartitions that need to be scanned to gather cube stats.

    Parameters
    ----------
    datasets: Dict[str, kartothek.DatasetMetadata]
        Datasets that are present.

    Returns
    -------
    metapartitions: Tuple[Tuple[str, Tuple[kartothek.io_components.metapartition.MetaPartition, ...]], ...]
        Pre-aligned metapartitions (by primary index / physical partitions) and the klee dataset ID belonging to them.
    """
    all_metapartitions = []
    for klee_dataset_id, ds in datasets.items():
        dataset_factory = metadata_factory_from_dataset(ds)
        for mp in dispatch_metapartitions_from_factory(
            dataset_factory=dataset_factory, dispatch_by=dataset_factory.partition_keys
        ):
            all_metapartitions.append((klee_dataset_id, mp))
    return all_metapartitions


def collect_stats_block(metapartitions, store):
    """
    Gather statistics data for multiple metapartitions.

    Parameters
    ----------
    metapartitions: Tuple[Tuple[str, Tuple[kartothek.io_components.metapartition.MetaPartition, ...]], ...]
        Part of the result of :meth:`get_metapartitions_for_stats`.
    store: Union[simplekv.KeyValueStore, Callable[[], simplekv.KeyValueStore]]
        KV store.

    Returns
    -------
    stats: Dict[str, Dict[str, int]]
        Statistics per klee dataset ID.
    """
    if callable(store):
        store = store()

    result = {}
    for klee_dataset_id, mp in metapartitions:
        stats = get_physical_partition_stats(mp, store)
        result = _fold_stats(result, stats, klee_dataset_id)

    return result


def reduce_stats(stats_iter):
    """
    Sum-up stats data.

    Parameters
    ----------
    stats_iter: Iterable[Dict[str, Dict[str, int]]]
        Iterable of stats objects, either resulting from :meth:`collect_stats_block` or previous :meth:`reduce_stats`
        calls.

    Returns
    -------
    stats: Dict[str, Dict[str, int]]
        Statistics per klee dataset ID.
    """
    result = {}
    for sub in stats_iter:
        for klee_dataset_id, stats in sub.items():
            result = _fold_stats(result, stats, klee_dataset_id)
    return result
