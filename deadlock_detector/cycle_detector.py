"""Bounded iterative BFS path propagation for deadlock cycle detection."""

from typing import List, Tuple

from .config import MAX_CYCLE_LENGTH

Path = Tuple[str, ...]


def build_search_inputs(edges, num_partitions: int):
    """Build partitioned adjacency RDD and initial single-node paths per process."""
    adjacency = (
        edges
        .map(lambda edge: (edge[0], edge[1]))
        .partitionBy(num_partitions)
        .persist()
    )

    process_nodes = (
        edges
        .flatMap(lambda edge: [edge[0], edge[1]])
        .filter(lambda node: node.startswith("P"))
        .distinct(numPartitions=num_partitions)
    )

    # State key: (starting_process, current_node)
    # State value: shortest path discovered from starting_process to current_node.
    initial_paths = (
        process_nodes
        .map(lambda process: ((process, process), (process,)))
        .partitionBy(num_partitions)
        .persist()
    )

    return adjacency, initial_paths


def choose_shorter_path(path_a: Path, path_b: Path) -> Path:
    """Return the shorter path, breaking equal-length ties lexicographically."""
    if len(path_a) < len(path_b):
        return path_a
    if len(path_b) < len(path_a):
        return path_b
    return min(path_a, path_b)


def count_partition(iterator):
    """Yield a single count record per partition, including empty partitions."""
    yield "N", sum(1 for _ in iterator)


def detect_cycles(
    adjacency,
    initial_paths,
    num_partitions: int,
    max_cycle_length: int = MAX_CYCLE_LENGTH,
) -> List[Path]:
    """Detect directed simple deadlock cycles via bounded BFS RDD propagation.

    For each (starting_process, current_node) state, only the shortest path
    discovered so far is retained. Under the single-instance resource model
    where each process and resource belongs to at most one deadlock cycle,
    retaining a single shortest representative per state prevents exponential
    path blowup while recovering all deadlock cycles up to `max_cycle_length`.

    Canonicalization enforces that each reported cycle begins at its
    lexicographically smallest process ID, eliminating cyclic rotations.
    """
    frontier = initial_paths
    visited = initial_paths.mapValues(lambda _path: 1).persist()
    stale_visited = None
    detected_cycles = set()

    for _iteration in range(max_cycle_length):
        # Re-key frontier paths by current_node to join against adjacency.
        frontier_by_node = (
            frontier
            .map(lambda item: (item[0][1], (item[0][0], item[1])))
            .partitionBy(num_partitions)
        )

        joined = frontier_by_node.join(adjacency, numPartitions=num_partitions)

        def propagate(record):
            _current_node, ((start, path), next_node) = record

            # Check for a closing edge back to the canonical starting process.
            if next_node == start:
                if 2 <= len(path) <= max_cycle_length:
                    return [("C", path)]
                return []

            # Reject non-simple paths that revisit an intermediate node.
            if next_node in path:
                return []

            # Enforce the upper bound on cycle length.
            if len(path) >= max_cycle_length:
                return []

            # Canonical rotation pruning: start must be the smallest process ID.
            if next_node.startswith("P") and next_node < start:
                return []

            extended_path = path + (next_node,)
            state_key = (start, next_node)
            return [("P", (state_key, extended_path))]

        # Cache tagged output so both active paths and completed cycles are
        # consumed in a single Spark action for this iteration.
        propagated = joined.flatMap(propagate).persist()

        candidate_paths = (
            propagated
            .filter(lambda item: item[0] == "P")
            .map(lambda item: item[1])
            .reduceByKey(choose_shorter_path, numPartitions=num_partitions)
        )

        next_frontier = (
            candidate_paths
            .subtractByKey(visited, numPartitions=num_partitions)
            .persist()
        )

        cycle_records = (
            propagated
            .filter(lambda item: item[0] == "C")
            .map(lambda item: ("C", item[1]))
        )

        # Single action per iteration: materialize next_frontier and collect cycles.
        path_counts = next_frontier.mapPartitions(count_partition)
        iteration_results = path_counts.union(cycle_records).collect()

        # Safe cleanup of prior cached visited RDD once materialized.
        if stale_visited is not None:
            stale_visited.unpersist()
            stale_visited = None

        next_count = 0
        for record_type, value in iteration_results:
            if record_type == "N":
                next_count += value
            elif record_type == "C":
                detected_cycles.add(value)

        propagated.unpersist()
        frontier.unpersist()

        if next_count == 0:
            next_frontier.unpersist()
            frontier = None
            break

        updated_visited = (
            visited
            .union(next_frontier.mapValues(lambda _path: 1))
            .partitionBy(num_partitions)
            .persist()
        )
        stale_visited = visited
        visited = updated_visited
        frontier = next_frontier

    if frontier is not None:
        frontier.unpersist()
    visited.unpersist()
    if stale_visited is not None:
        stale_visited.unpersist()
    initial_paths.unpersist()

    # Deterministic ordering: ascending cycle length, then lexicographical.
    return sorted(detected_cycles, key=lambda cycle: (len(cycle), cycle))
