"""统计脚本自身的独立标准测试，不执行或替代运行时、基准、队列、身份及唤醒逻辑。

使用临时JSON输入验证解析/计数/数值拒绝边界。人造数据只用于校验器，不能用于性能结论。
入口：python3 -m unittest discover -s scripts -p test_single_task_pool_owner_analysis.py -v。
"""

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import analyze_single_task_pool_owner as analyzer


class AnalysisContract(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="pi-owner-analysis-")
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "samples.jsonl"
        self.rows = []
        for mode in analyzer.MODES:
            for sample in range(5):
                ops = 4096 if mode == "empty_run_once" else 2048
                self.rows.append(dict(mode=mode, sample=sample, ops=ops, elapsed_ns=ops * 100,
                    ns_per_op=100.0, ops_per_second=10000000.0, p50_ns=60, p90_ns=80,
                    p99_ns=100, max_ns=200, cpu_ns=ops * 120, rss_kb=2304, max_inflight=1))

    def write(self, rows):
        # 夹具数据使用标准序列化器，不拼接或伪造生产测量输出。
        self.path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

    def test_valid_samples_and_multiple_files(self):
        self.write(self.rows)
        result = analyzer.read([self.path, self.path])
        self.assertEqual(tuple(result), analyzer.MODES)
        self.assertTrue(all(len(rows) == 10 for rows in result.values()))

    def test_resource_unavailable_is_distinct_from_measured_zero(self):
        self.rows[0]["rss_kb"] = None
        self.rows[0]["cpu_ns"] = 0
        self.write(self.rows)
        result = analyzer.read([self.path])[analyzer.MODES[0]][0]
        self.assertIsNone(result["rss_kb"])
        self.assertEqual(result["cpu_ns"], 0)

    def test_reject_invalid_counts_identities_and_object_shapes(self):
        for rows in ([], self.rows[:-1], self.rows + [self.rows[0]],
                [self.rows[0]] * 25, [None] + self.rows[1:], [dict()] + self.rows[1:]):
            with self.subTest(length=len(rows), first=rows[:1]):
                self.write(rows)
                with self.assertRaises(ValueError):
                    analyzer.read([self.path])

    def test_reject_each_numeric_identity_and_resource_boundary(self):
        invalid = (("mode", "unknown"), ("sample", -1), ("sample", 5), ("sample", False),
            ("ops", 2048), ("max_inflight", 2), ("elapsed_ns", 0), ("elapsed_ns", 10000000000),
            ("ns_per_op", 101), ("ops_per_second", 1), ("ns_per_op", float("nan")),
            ("ops_per_second", float("inf")), ("cpu_ns", -1), ("cpu_ns", float("nan")),
            ("rss_kb", -1), ("rss_kb", "2304"), ("p50_ns", -1), ("p90_ns", 59),
            ("p99_ns", 201), ("max_ns", 99), ("max_ns", True))
        for key, value in invalid:
            with self.subTest(key=key, value=value):
                rows = copy.deepcopy(self.rows)
                rows[0][key] = value
                self.write(rows)
                with self.assertRaises(ValueError):
                    analyzer.read([self.path])

    def test_cli_same_input_reports_zero_change_without_running_benchmark(self):
        self.write(self.rows)
        command = [sys.executable, str(Path(analyzer.__file__)), "--before", str(self.path), "--after", str(self.path)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.count("+0.00%"), 5)
        self.assertIn("5/5", result.stdout)
        self.assertIn("10000000", result.stdout)


if __name__ == "__main__":
    unittest.main()
