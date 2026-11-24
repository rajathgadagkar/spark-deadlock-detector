"""Spark Deadlock Detector — Distributed Resource Allocation Graph (RAG) Analyzer."""

from .config import DEFAULT_TOP_K, MAX_CYCLE_LENGTH
from .cycle_detector import build_search_inputs, choose_shorter_path, detect_cycles
from .hotspots import calculate_hotspots
from .metrics import compute_graph_metrics
from .parser import build_rag_edges, parse_log_line
from .pipeline import AnalysisResult, format_report, run_analysis
from .timeline import calculate_deadlock_timeline

__all__ = [
    "AnalysisResult",
    "DEFAULT_TOP_K",
    "MAX_CYCLE_LENGTH",
    "build_rag_edges",
    "build_search_inputs",
    "calculate_deadlock_timeline",
    "calculate_hotspots",
    "choose_shorter_path",
    "compute_graph_metrics",
    "detect_cycles",
    "format_report",
    "parse_log_line",
    "run_analysis",
]

__version__ = "1.0.0"
