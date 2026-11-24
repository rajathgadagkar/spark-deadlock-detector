# Architecture & Algorithmic Design

This document details the MapReduce RDD pipeline, state-space pruning strategy, memory management lifecycle, and determinism guarantees behind **Spark Deadlock Detector**.

---

## 1. End-to-End Pipeline

The analyzer processes raw resource allocation logs through five decoupled stages implemented in `deadlock_detector/`:

1. **Log Parsing & Edge Deduplication (`parser.py`)**
   - Reads raw log records into an RDD partitioned by `num_cores`.
   - Maps each valid record into a directed Resource Allocation Graph (RAG) edge paired with its event timestamp:
     - `WAIT` → `(Process, Resource)` (request edge)
     - `HOLD` → `(Resource, Process)` (allocation edge)
   - Applies `reduceByKey(min)` to collapse duplicate edges while preserving the **earliest** timestamp at which each directed edge appeared.

2. **Graph Topology Metrics (`metrics.py`)**
   - Extracts distinct process nodes (`P*`), distinct resource nodes (`R*`), and total unique directed edges from the cached edge RDD.

3. **Bounded Iterative BFS Cycle Detection (`cycle_detector.py`)**
   - Constructs a hash-partitioned adjacency RDD `(source -> destination)` and seeds one initial state per process node: `((start_process, start_process), (start_process,))`.
   - Executes up to `MAX_CYCLE_LENGTH` (default `25`) synchronized breadth-first propagation iterations.

4. **Deadlock Formation Timeline (`timeline.py`)**
   - Expands each detected cycle into its constituent directed edges (including the closing edge from the last node back to the first).
   - Joins against the earliest-timestamp edge RDD and applies `reduceByKey(max)` per cycle to determine the exact timestamp when the final required edge completed the cycle.
   - Aggregates completions by calendar date (`YYYY-MM-DD`) in chronological order.

5. **Resource Contention Hotspots (`hotspots.py`)**
   - Computes incoming request degree per resource (`Process -> Resource` in-degree) and outgoing wait degree per process (`Process -> Resource` out-degree).
   - Uses `leftOuterJoin` against the full node sets so zero-degree nodes remain representable when graphs are sparse.
   - Ranks top-K nodes by `(-degree, node_id)` for deterministic tie-breaking.

---

## 2. Cycle Detection State Model & Pruning

Naively enumerating all simple paths in a directed graph grows exponentially with path depth. **Spark Deadlock Detector** avoids path explosion using three structural invariants:

### State Keying
Each active search state is keyed by:
```
(starting_process, current_node) -> shortest_path_tuple
```
Because propagation advances level-by-level (BFS), the first iteration that reaches `(start, node)` is guaranteed to yield a shortest path. Within the same iteration, `reduceByKey(choose_shorter_path)` deterministically selects a single representative path (shorter length first, lexicographic tie-break).

Under the single-instance resource model where each process and resource participates in at most one deadlock cycle, retaining one shortest representative per `(starting_process, current_node)` state is sufficient to recover every deadlock cycle within the length bound.

### Canonical Rotation Pruning
A cycle of length $2k$ contains $k$ distinct processes and could otherwise be discovered $k$ times from $k$ different starting processes. During edge expansion, any step moving to a process node `next_node < start` is immediately discarded. Consequently, every cycle is discovered **exactly once**, anchored at its lexicographically smallest process ID.

### Simple-Path & Length Guards
- If `next_node == start` and `2 <= len(path) <= max_cycle_length`, the path is emitted as a completed cycle (`("C", path)`).
- Any intermediate node already present in `path` is rejected to keep paths simple.
- Paths reaching `max_cycle_length` without closing back to `start` are pruned.

---

## 3. RDD Persistence & Single-Action Synchronization

Iterative RDD algorithms in Spark can easily suffer from unbounded lineage growth or memory leaks if cached RDDs are not released carefully.

- **Single Spark Action per Iteration**: Tagged records (`"P"` for active paths, `"C"` for completed cycles) are cached once in `propagated`. Partition counts of `next_frontier` are unioned with `cycle_records` and collected in a single `.collect()` call per depth level.
- **Explicit `stale_visited` Unpersistence**: When `visited` is updated via `visited.union(next_frontier)`, the previous `visited` RDD is kept cached until the *following* iteration's `subtractByKey` materializes the new `visited` RDD, at which point `stale_visited.unpersist()` safely frees executor memory without triggering recomputation.
- **Early Termination**: As soon as the frontier count reaches `0`, the loop exits immediately without executing remaining iterations up to `max_cycle_length`.
