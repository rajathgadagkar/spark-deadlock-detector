"""Unit tests for pure logic components of Spark Deadlock Detector."""

import unittest

from deadlock_detector.cycle_detector import choose_shorter_path, count_partition
from deadlock_detector.parser import parse_log_line
from deadlock_detector.pipeline import (
    AnalysisResult,
    extract_involved_processes,
    format_report,
)
from deadlock_detector.timeline import extract_cycle_edges


class TestLogParser(unittest.TestCase):
    def test_wait_event_creates_process_to_resource_edge(self):
        line = "2025-10-31 21:07:53\tWAIT\tP106\tR006"
        self.assertEqual(
            parse_log_line(line),
            (("P106", "R006"), "2025-10-31 21:07:53"),
        )

    def test_hold_event_creates_resource_to_process_edge(self):
        line = "2025-10-31 21:07:54\tHOLD\tP101\tR006"
        self.assertEqual(
            parse_log_line(line),
            (("R006", "P101"), "2025-10-31 21:07:54"),
        )

    def test_invalid_lines_return_none(self):
        self.assertIsNone(parse_log_line(""))
        self.assertIsNone(parse_log_line("2025-10-31 21:07:53 RELEASE P101 R001"))
        self.assertIsNone(parse_log_line("2025-10-31 21:07:53 WAIT X101 R001"))
        self.assertIsNone(parse_log_line("2025-10-31 21:07:53 WAIT P101 X001"))
        self.assertIsNone(parse_log_line("malformed line"))


class TestCycleHelpers(unittest.TestCase):
    def test_choose_shorter_path_prefers_fewer_nodes(self):
        short_path = ("P101", "R001", "P102")
        long_path = ("P101", "R002", "P103", "R003", "P102")
        self.assertEqual(choose_shorter_path(short_path, long_path), short_path)
        self.assertEqual(choose_shorter_path(long_path, short_path), short_path)

    def test_choose_shorter_path_breaks_ties_lexicographically(self):
        path_a = ("P101", "R001", "P102")
        path_b = ("P101", "R002", "P102")
        self.assertEqual(choose_shorter_path(path_b, path_a), path_a)

    def test_count_partition_handles_empty_and_non_empty(self):
        self.assertEqual(list(count_partition(iter([]))), [("N", 0)])
        self.assertEqual(list(count_partition(iter([1, 2, 3]))), [("N", 3)])

    def test_extract_cycle_edges_wraps_around(self):
        cycles = [("P101", "R002", "P106", "R006")]
        expected = [
            (("P101", "R002"), 0),
            (("R002", "P106"), 0),
            (("P106", "R006"), 0),
            (("R006", "P101"), 0),
        ]
        self.assertEqual(extract_cycle_edges(cycles), expected)


class TestReporting(unittest.TestCase):
    def setUp(self):
        self.sample_result = AnalysisResult(
            process_count=9,
            resource_count=9,
            edge_count=18,
            detection_time_seconds=7.11,
            cycles=[
                ("P101", "R002", "P106", "R006"),
                ("P105", "R003", "P107", "R007"),
            ],
            involved_processes=["P101", "P105", "P106", "P107"],
            timeline=[("2025-10-31", 1), ("2025-11-02", 1)],
            top_resources=[("R001", 2), ("R008", 2)],
            top_processes=[("P104", 2), ("P109", 2)],
        )

    def test_extract_involved_processes(self):
        procs = extract_involved_processes(self.sample_result.cycles)
        self.assertEqual(procs, ["P101", "P105", "P106", "P107"])

    def test_compact_report_format(self):
        output = format_report(self.sample_result, pretty=False).splitlines()
        self.assertEqual(output[0], "9")
        self.assertEqual(output[1], "9")
        self.assertEqual(output[2], "18")
        self.assertEqual(output[3], "7.11")
        self.assertEqual(output[4], "2")
        self.assertEqual(output[5], "P101 R002 P106 R006")
        self.assertEqual(output[7], "4")
        self.assertEqual(output[8], "P101 P105 P106 P107")
        self.assertEqual(output[9], "2025-10-31 1")

    def test_pretty_report_format(self):
        output = format_report(self.sample_result, pretty=True)
        self.assertIn("Resource Allocation Graph (RAG) Summary", output)
        self.assertIn("P101 -> R002 -> P106 -> R006", output)
        self.assertIn("2025-10-31: 1 cycle(s)", output)


if __name__ == "__main__":
    unittest.main()
