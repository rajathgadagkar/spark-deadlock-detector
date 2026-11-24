"""Log parsing and Resource Allocation Graph (RAG) edge construction."""

from typing import Optional, Tuple

Edge = Tuple[str, str]
EdgeEvent = Tuple[Edge, str]


def parse_log_line(line: str) -> Optional[EdgeEvent]:
    """Parse a single RAG log entry into ((source, destination), timestamp).

    Expected whitespace-separated format:
        YYYY-MM-DD HH:MM:SS  <WAIT|HOLD>  <P-ID>  <R-ID>

    Edge direction rules:
        - WAIT creates a request edge: Process -> Resource
        - HOLD creates an allocation edge: Resource -> Process
    """
    fields = line.strip().split()
    if len(fields) != 5:
        return None

    date, clock, event_type, process_id, resource_id = fields

    if not process_id.startswith("P") or not resource_id.startswith("R"):
        return None

    timestamp = f"{date} {clock}"

    if event_type == "WAIT":
        edge = (process_id, resource_id)
    elif event_type == "HOLD":
        edge = (resource_id, process_id)
    else:
        return None

    return edge, timestamp


def build_rag_edges(sc, log_file: str, num_cores: int):
    """Load raw log records and construct deduplicated RAG edge RDDs.

    Returns:
        Tuple of (edge_timestamps_rdd, edges_rdd), both persisted:
        - edge_timestamps_rdd: RDD of ((source, destination), earliest_timestamp)
        - edges_rdd: RDD of unique (source, destination) directed edges
    """
    raw_logs = sc.textFile(log_file, minPartitions=num_cores)
    parsed_events = (
        raw_logs
        .map(parse_log_line)
        .filter(lambda record: record is not None)
    )

    edge_timestamps = (
        parsed_events
        .reduceByKey(
            lambda ts_a, ts_b: min(ts_a, ts_b),
            numPartitions=num_cores,
        )
        .persist()
    )
    edges = edge_timestamps.keys().persist()

    return edge_timestamps, edges
