"""Graph topology metrics for the Resource Allocation Graph (RAG)."""

from typing import Tuple


def compute_graph_metrics(edges) -> Tuple[int, int, int]:
    """Compute unique process count, unique resource count, and unique edge count."""
    process_count = (
        edges
        .flatMap(lambda edge: [edge[0], edge[1]])
        .filter(lambda node: node.startswith("P"))
        .distinct()
        .count()
    )

    resource_count = (
        edges
        .flatMap(lambda edge: [edge[0], edge[1]])
        .filter(lambda node: node.startswith("R"))
        .distinct()
        .count()
    )

    edge_count = edges.count()

    return process_count, resource_count, edge_count
