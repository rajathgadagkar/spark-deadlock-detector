"""Graph structure analysis for resource contention hotspots."""

from typing import List, Tuple

from .config import DEFAULT_TOP_K

HotspotEntry = Tuple[str, int]


def calculate_hotspots(
    edges,
    top_k: int = DEFAULT_TOP_K,
) -> Tuple[List[HotspotEntry], List[HotspotEntry]]:
    """Compute the top-K resource in-degrees and process out-degrees.

    - Resource in-degree: number of distinct processes waiting on the resource.
    - Process out-degree: number of distinct resources the process is waiting on.

    Left outer joins ensure nodes with degree 0 are still ranked when fewer
    than `top_k` nodes have non-zero degree. Ties are broken by ascending ID.
    """
    resource_nodes = (
        edges
        .flatMap(lambda edge: [edge[0], edge[1]])
        .filter(lambda node: node.startswith("R"))
        .distinct()
        .map(lambda node: (node, 1))
    )

    resource_indegrees = (
        edges
        .filter(lambda edge: edge[1].startswith("R"))
        .map(lambda edge: (edge[1], 1))
        .reduceByKey(lambda a, b: a + b)
    )

    top_resources = (
        resource_nodes
        .leftOuterJoin(resource_indegrees)
        .map(lambda item: (item[0], item[1][1] if item[1][1] is not None else 0))
        .takeOrdered(top_k, key=lambda item: (-item[1], item[0]))
    )

    process_nodes = (
        edges
        .flatMap(lambda edge: [edge[0], edge[1]])
        .filter(lambda node: node.startswith("P"))
        .distinct()
        .map(lambda node: (node, 1))
    )

    process_outdegrees = (
        edges
        .filter(lambda edge: edge[0].startswith("P"))
        .map(lambda edge: (edge[0], 1))
        .reduceByKey(lambda a, b: a + b)
    )

    top_processes = (
        process_nodes
        .leftOuterJoin(process_outdegrees)
        .map(lambda item: (item[0], item[1][1] if item[1][1] is not None else 0))
        .takeOrdered(top_k, key=lambda item: (-item[1], item[0]))
    )

    return top_resources, top_processes
