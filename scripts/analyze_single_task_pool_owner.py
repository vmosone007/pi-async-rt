#!/usr/bin/env python3
"""校验并汇总独立 owner 基准的 JSON 样本；只读取数据、输出统计，不运行基准。

用法：--before 修复前.jsonl [更多文件] --after 修复后.jsonl [更多文件]。
统计场景必须一致，每文件每场景严格五轮；不删除离群值，不生成性能通过结论。
复杂度 O(n log n)，空间 O(n)，不修改输入。原始 p50/p99 的中位数不是合并样本总体分位数。
"""

import argparse
import json
import math
import statistics
from pathlib import Path

MODES = ("empty_run_once", "public_ready", "local_ready", "local_yield", "external_oneshot")


def read(paths):
    """按标准 JSON 解析，严格检查身份、轮次、资源上限和数值，拒绝残缺/重复样本。"""
    result = {mode: [] for mode in MODES}
    for name in paths:
        rows = [json.loads(line) for line in Path(name).read_text(encoding="utf-8").splitlines() if line.strip()]
        if len(rows) != 25:
            raise ValueError(f"{name}: 应有25条原始样本，实际{len(rows)}")
        seen = set()
        for row in rows:
            if not isinstance(row, dict) or not isinstance(row.get("mode"), str):
                raise ValueError(f"{name}: 样本必须是具有场景名的对象")
            for field in ("sample", "ops", "elapsed_ns", "p50_ns", "p90_ns", "p99_ns", "max_ns", "cpu_ns", "max_inflight"):
                if type(row.get(field)) is not int:
                    raise ValueError(f"{name}: {field}必须为整数而非布尔值/浮点数")
            for field in ("ns_per_op", "ops_per_second"):
                if type(row.get(field)) not in (int, float) or not math.isfinite(row[field]):
                    raise ValueError(f"{name}: {field}必须为有限数值")
            if "rss_kb" not in row or (row["rss_kb"] is not None and type(row["rss_kb"]) is not int):
                raise ValueError(f"{name}: RSS必须为整数或null")
            key = (row["mode"], row["sample"])
            if key in seen or key[0] not in MODES or key[1] not in range(5):
                raise ValueError(f"{name}: 非法或重复样本{key}")
            seen.add(key)
            expected = 4096 if key[0] == "empty_run_once" else 2048
            if row["ops"] != expected or row["max_inflight"] != 1:
                raise ValueError(f"{name}: 负载不符{key}")
            if not 0 < row["elapsed_ns"] < 10_000_000_000:
                raise ValueError(f"{name}: 非法场景耗时{key}")
            for field, expected_value in (("ns_per_op", row["elapsed_ns"] / row["ops"]),
                    ("ops_per_second", row["ops"] * 1_000_000_000 / row["elapsed_ns"])):
                if not math.isclose(row[field], expected_value, rel_tol=0, abs_tol=0.00051):
                    raise ValueError(f"{name}: 派生指标与原始计数不一致{key}/{field}")
            if row["cpu_ns"] < 0 or (row["rss_kb"] is not None and row["rss_kb"] < 0):
                raise ValueError(f"{name}: 负的资源用量{key}")
            if not 0 <= row["p50_ns"] <= row["p90_ns"] <= row["p99_ns"] <= row["max_ns"]:
                raise ValueError(f"{name}: 非法分位数{key}")
            result[key[0]].append(row)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", nargs="+", required=True)
    parser.add_argument("--after", nargs="+", required=True)
    args = parser.parse_args()
    before, after = read(args.before), read(args.after)
    print("| 场景 | 前ns/op中位 | 后ns/op中位 | 变化 | 后ops/s中位 | 后ns/op最小~最大 | 后p50/p99样本中位(ns) | 后CPU/墙钟 | 后RSS范围(KiB) |")
    print("| --- | ---: | ---: | ---: | ---: | --- | --- | ---: | --- |")
    for mode in MODES:
        old, new = before[mode], after[mode]
        old_median = statistics.median(row["ns_per_op"] for row in old)
        times = [row["ns_per_op"] for row in new]
        median = statistics.median(times)
        ops = statistics.median(row["ops_per_second"] for row in new)
        p50 = statistics.median(row["p50_ns"] for row in new)
        p99 = statistics.median(row["p99_ns"] for row in new)
        cpu = sum(row["cpu_ns"] for row in new) / sum(row["elapsed_ns"] for row in new)
        rss = [row["rss_kb"] for row in new if row["rss_kb"] is not None]
        rss_text = f"{min(rss)}~{max(rss)}" if rss else "不可用"
        print(f"| {mode} | {old_median:.3f} | {median:.3f} | {(median / old_median - 1) * 100:+.2f}% | {ops:.0f} | {min(times):.3f}~{max(times):.3f} | {p50:.0f}/{p99:.0f} | {cpu:.3f} | {rss_text} |")
    print(f"\n前/后各场景样本数：{len(before[MODES[0]])}/{len(after[MODES[0]])}；全部样本参与，无离群值剔除。")
    print("ns/op含测时/断言/分配/真实调度成本；ops是本场景操作吞吐，不能当作多worker吞吐或硬实时承诺。")


if __name__ == "__main__":
    main()
