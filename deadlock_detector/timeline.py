"""Temporal analysis for deadlock cycle formation timelines."""

from typing import List, Tuple

Path = Tuple[str, ...]
TimelineEntry = Tuple[str, int]


def extract_cycle_edges(cycles: List[Path]) -> List[Tuple[Tuple[str, str], int]]:
    """Expand each cycle into ((source, destination), cycle_id) records."""
    cycle_edge_records = []
    for cycle_id, cycle in enumerate(cycles):
        cycle_len = len(cycle)
        for index in range(cycle_len):
            source = cycle[index]
            destination = cycle[(index + 1) % cycle_len]
            cycle_edge_records.append(((source, destination), cycle_id))
    return cycle_edge_records


def calculate_deadlock_timeline(sc, cycles: List[Path], edge_timestamps) -> List[TimelineEntry]:
    """Determine the date each cycle first became fully formed and count per day.

    A deadlock cycle is considered formed at the timestamp when its final
    constituent edge first appeared in the resource allocation log.
    """
    if not cycles:
        return []

    cycle_edge_records = extract_cycle_edges(cycles)
    cycle_edges = sc.parallelize(cycle_edge_records)

    edge_times = (
        cycle_edges
        .join(edge_timestamps)
        .map(lambda record: (record[1][0], record[1][1]))
    )

    cycle_completion = edge_times.reduceByKey(max)

    timeline = (
        cycle_completion
        .map(lambda record: (record[1][:10], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
        .collect()
    )

    return timeline
