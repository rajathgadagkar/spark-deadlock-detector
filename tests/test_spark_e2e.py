"""End-to-end integration test executing the full PySpark pipeline on sample_hpc_rag.log."""

import subprocess
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_LOG = REPO_ROOT / "data" / "sample_hpc_rag.log"


class TestSparkEndToEnd(unittest.TestCase):
    def test_compact_output_matches_expected_sample_results(self):
        proc = subprocess.run(
            [sys.executable, str(REPO_ROOT / "main.py"), str(SAMPLE_LOG), "2"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=True,
        )
        lines = [line.strip() for line in proc.stdout.strip().splitlines()]

        # Graph topology metrics
        self.assertEqual(lines[0], "9")
        self.assertEqual(lines[1], "9")
        self.assertEqual(lines[2], "18")

        # Execution time is a non-negative float
        self.assertGreaterEqual(float(lines[3]), 0.0)

        # Detected deadlock cycles (sorted by length, then lexicographically)
        self.assertEqual(lines[4], "3")
        self.assertEqual(lines[5], "P101 R002 P106 R006")
        self.assertEqual(lines[6], "P105 R003 P107 R007")
        self.assertEqual(lines[7], "P102 R005 P103 R009 P108 R004")

        # Involved processes
        self.assertEqual(lines[8], "7")
        self.assertEqual(lines[9], "P101 P102 P103 P105 P106 P107 P108")

        # Daily deadlock formation timeline
        self.assertEqual(lines[10], "2025-10-31 1")
        self.assertEqual(lines[11], "2025-11-02 2")

        # Top 5 contested resources (in-degree)
        self.assertEqual(
            lines[12:17],
            ["R001 2", "R008 2", "R002 1", "R003 1", "R004 1"],
        )

        # Top 5 blocked processes (out-degree)
        self.assertEqual(
            lines[17:22],
            ["P104 2", "P109 2", "P101 1", "P102 1", "P103 1"],
        )

    def test_pretty_output_mode_succeeds(self):
        proc = subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "main.py"),
                str(SAMPLE_LOG),
                "2",
                "--pretty",
            ],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertIn("Resource Allocation Graph (RAG) Summary", proc.stdout)
        self.assertIn("P101 -> R002 -> P106 -> R006", proc.stdout)
        self.assertIn("2025-11-02: 2 cycle(s)", proc.stdout)


if __name__ == "__main__":
    unittest.main()
