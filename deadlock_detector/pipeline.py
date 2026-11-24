"""End-to-end analysis pipeline and output formatting."""

from dataclasses import dataclass
import time
from typing import List, Tuple

from .config import DEFAULT_TOP_K, MAX_CYCLE_LENGTH
from .cycle_detector import Path, build_search_inputs, detect_cycles
from .hotspots import HotspotEntry, calculate_hotspots
from .metrics import compute_graph_metrics
from .parser import build_rag_edges
from .timeline import TimelineEntry, calculate_deadlock_timeline


@dataclass(frozen=True)
class AnalysisResult:
    """Structured container holding all RAG analysis outputs."""

    process_count: int
    resource_count: int
    edge_count: int
    detection_time_seconds: float
    cycles: List[Path]
    involved_processes: List[str]
    timeline: List[TimelineEntry]
    top_resources: List[HotspotEntry]
    top_processes: List[HotspotEntry]


def extract_involved_processes(cycles: List[Path]) -> List[str]:
    """Return sorted unique process IDs participating in any detected cycle."""
    return sorted(
        {
            node
            for cycle in cycles
            for node in cycle
            if node.startswith("P")
        }
    )


def run_analysis(
    sc,
    log_file: str,
    num_cores: int,
    max_cycle_length: int = MAX_CYCLE_LENGTH,
    top_k: int = DEFAULT_TOP_K,
) -> AnalysisResult:
    """Execute the full RAG construction, cycle detection, timeline, and hotspot pipeline."""
    edge_timestamps, edges = build_rag_edges(sc, log_file, num_cores)

    try:
        num_partitions = max(num_cores, edges.getNumPartitions())
        process_count, resource_count, edge_count = compute_graph_metrics(edges)

        detection_start = time.perf_counter()
        adjacency, initial_paths = build_search_inputs(edges, num_partitions)
        cycles = detect_cycles(
            adjacency,
            initial_paths,
            num_partitions,
            max_cycle_length=max_cycle_length,
        )
        detection_time_seconds = time.perf_counter() - detection_start
        adjacency.unpersist()

        involved_processes = extract_involved_processes(cycles)
        timeline = calculate_deadlock_timeline(sc, cycles, edge_timestamps)
        top_resources, top_processes = calculate_hotspots(edges, top_k=top_k)

        return AnalysisResult(
            process_count=process_count,
            resource_count=resource_count,
            edge_count=edge_count,
            detection_time_seconds=detection_time_seconds,
            cycles=cycles,
            involved_processes=involved_processes,
            timeline=timeline,
            top_resources=top_resources,
            top_processes=top_processes,
        )
    finally:
        edges.unpersist()
        edge_timestamps.unpersist()


def format_report(result: AnalysisResult, pretty: bool = False) -> str:
    """Render an AnalysisResult in either compact machine-readable or rich summary format."""
    if not pretty:
        lines = [
            str(result.process_count),
            str(result.resource_count),
            str(result.edge_count),
            f"{result.detection_time_seconds:.2f}",
            str(len(result.cycles)),
        ]
        lines.extend(" ".join(cycle) for cycle in result.cycles)
        lines.append(str(len(result.involved_processes)))
        lines.append(" ".join(result.involved_processes))
        lines.extend(f"{date} {count}" for date, count in result.timeline)
        lines.extend(f"{res_id} {deg}" for res_id, deg in result.top_resources)
        lines.extend(f"{proc_id} {deg}" for proc_id, deg in result.top_processes)
        return "\n".join(lines)

    sections = [
        "=== Resource Allocation Graph (RAG) Summary ===",
        f"Unique Process Nodes   : {result.process_count}",
        f"Unique Resource Nodes  : {result.resource_count}",
        f"Unique Directed Edges  : {result.edge_count}",
        "",
        "=== Deadlock Cycle Detection ===",
        f"Detection Time         : {result.detection_time_seconds:.2f}s",
        f"Cycles Detected        : {len(result.cycles)}",
    ]
    if result.cycles:
        for idx, cycle in enumerate(result.cycles, start=1):
            sections.append(f"  [{idx}] ({len(cycle)} edges) " + " -> ".join(cycle))
    else:
        sections.append("  None")

    sections.extend([
        f"Involved Processes ({len(result.involved_processes)}): "
        + (", ".join(result.involved_processes) if result.involved_processes else "None"),
        "",
        "=== Deadlock Formation Timeline ===",
    ])
    if result.timeline:
        sections.extend(f"  {date}: {count} cycle(s)" for date, count in result.timeline)
    else:
        sections.append("  No deadlocks formed")

    sections.append("")
    sections.append("=== Contention Hotspots ===")
    sections.append("Top Resources (by in-degree / waiting processes):")
    sections.extend(f"  {res_id}: {deg}" for res_id, deg in result.top_resources)
    sections.append("Top Processes (by out-degree / awaited resources):")
    sections.extend(f"  {proc_id}: {deg}" for proc_id, deg in result.top_processes)

    return "\n".join(sections)
