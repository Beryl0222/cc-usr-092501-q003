"""确定性重放入口的测试。"""

from __future__ import annotations

import copy
import json
import random
import subprocess
import sys
import unittest
from pathlib import Path

from src.replay import load_jsonl, replay_events

ROOT = Path(__file__).parents[1]


class ReplayTest(unittest.TestCase):
    def setUp(self) -> None:
        self.events = load_jsonl(ROOT / "data" / "demo_events.jsonl")

    def test_demo_stream_properties(self) -> None:
        report = replay_events(copy.deepcopy(self.events))
        self.assertGreaterEqual(report["duplicate_events"], 1)
        # 板式网球玻璃墙证书回执同编号异内容 → 争议
        cert_disputes = {d["cert_id"] for d in report["disputes"] if "cert_id" in d}
        self.assertIn("cert-padel-glass", cert_disputes)
        # 撤销只冻结 fx-101
        frozen = {f["fixture_id"] for f in report["fixtures"] if f["frozen"]}
        self.assertEqual(frozen, {"fx-101"})

    def test_delivery_order_does_not_change_result(self) -> None:
        base = replay_events(copy.deepcopy(self.events))
        # 用多个种子乱序送达，状态指纹与关键结论必须完全一致
        for seed in (1, 7, 99, 2026):
            shuffled = copy.deepcopy(self.events)
            random.Random(seed).shuffle(shuffled)
            report = replay_events(shuffled)
            self.assertEqual(report["fingerprint_sha256"], base["fingerprint_sha256"],
                             f"种子 {seed} 的重放指纹不一致")
            self.assertEqual(report["appended_events"], base["appended_events"])
            self.assertEqual(
                [(f["fixture_id"], f["frozen"]) for f in report["fixtures"]],
                [(f["fixture_id"], f["frozen"]) for f in base["fixtures"]],
            )

    def test_double_replay_is_idempotent(self) -> None:
        """同一批事件重放进同一张表两次，不会产生额外事件。"""
        from src.store import EventStore

        store = EventStore(":memory:")
        first = replay_events(copy.deepcopy(self.events), store)
        self.assertGreater(first["appended_events"], 0)
        second = replay_events(copy.deepcopy(self.events), store)
        # 库中已有同 event_id 的同内容事件：全部按幂等重复处理，不再新增
        self.assertEqual(second["appended_events"], 0)
        self.assertEqual(second["fingerprint_sha256"], first["fingerprint_sha256"])

    def test_historical_rule_can_be_restored(self) -> None:
        report = replay_events(copy.deepcopy(self.events))
        pkg = next(p for p in report["packages"] if p["package_id"] == "tecq-rules")
        numbers = [p["revision"] for p in pkg["applicability_periods"]]
        self.assertEqual(numbers, [1, 2])
        r1 = next(p for p in pkg["applicability_periods"] if p["revision"] == 1)
        self.assertIsNotNone(r1["effective_until"], "r1 有明确的适用截止时点")
        current = pkg["current_revision"]
        self.assertEqual(current["number"], 2)
        self.assertTrue(current["scoring_changed"])

    def test_cli_outputs_deterministic_report(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "src.replay", "data/demo_events.jsonl"],
            cwd=ROOT, text=True, capture_output=True, check=True,
        )
        report = json.loads(result.stdout)
        self.assertIn("fingerprint_sha256", report)
        # 再跑一次，输出逐字节一致（sort_keys + 确定性排序）
        result2 = subprocess.run(
            [sys.executable, "-m", "src.replay", "data/demo_events.jsonl"],
            cwd=ROOT, text=True, capture_output=True, check=True,
        )
        self.assertEqual(result.stdout, result2.stdout)


if __name__ == "__main__":
    unittest.main()
