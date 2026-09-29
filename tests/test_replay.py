"""确定性重放命令的回归测试。"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from src.replay import replay_batch
from src.domain import RuleCertificationService
from src.store import SqliteEventStore

ROOT = Path(__file__).parents[1]


def _sport_package_commands(prefix="p", sport="teqball"):
    base = "2026-09-25T09:{:02d}:00+08:00"
    return [
        {"command_id": f"{prefix}-sport", "occurred_at": base.format(0),
         "command": "register_sport", "args": {"sport": sport, "name": sport}},
        {"command_id": f"{prefix}-pkg", "occurred_at": base.format(1),
         "command": "create_package",
         "args": {"package_id": f"{sport}-v1", "sport": sport,
                  "stage": "qualification", "clauses": {"touch_limit": 3}}},
        {"command_id": f"{prefix}-rev", "occurred_at": base.format(2),
         "command": "submit_revision",
         "args": {"package_id": f"{sport}-v1", "revision_id": f"{prefix}-r1",
                  "submitted_by": "alice", "submitter_role": "technical",
                  "change_class": "initial", "summary": "首版",
                  "content": {"touch_limit": 3}}},
        *[{"command_id": f"{prefix}-sign-{role}", "occurred_at": base.format(3 + i),
           "command": "sign_revision",
           "args": {"revision_id": f"{prefix}-r1", "signer": signer, "role": role}}
          for i, (signer, role) in enumerate(
              [("bob", "technical"), ("carol", "medical_safety"),
               ("dave", "competition_ops")])],
    ]


class ReplayTest(unittest.TestCase):
    def test_shuffled_order_same_hash(self):
        import random
        batch = {"commands": _sport_package_commands()}
        h1 = replay_batch(json.loads(json.dumps(batch)))["event_stream_hash"]
        shuffled = json.loads(json.dumps(batch))
        random.Random(7).shuffle(shuffled["commands"])
        h2 = replay_batch(shuffled)["event_stream_hash"]
        self.assertEqual(h1, h2)

    def test_self_review_rejected_not_applied(self):
        commands = _sport_package_commands("x")
        commands[2]["args"]["submitted_by"] = "alice"
        # 让 technical 签署也由 alice 完成 → 越权自复核。
        commands[3]["args"]["signer"] = "alice"
        report = replay_batch({"commands": commands})
        rejected = [r for r in report["results"] if r["status"] == "rejected"]
        self.assertEqual(len(rejected), 1)
        self.assertIn("自己提交", rejected[0]["error"])
        # 包不应发布。
        self.assertEqual(report["state"]["packages"]["teqball-v1"]["status"], "draft")

    def test_replay_into_sqlite_is_recoverable(self):
        import copy
        batch = {"commands": _sport_package_commands("y")}
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "r.sqlite")
            report1 = replay_batch(copy.deepcopy(batch), SqliteEventStore(db))
            # 第二次重放同一批次：全部幂等，不产生新事件。
            report2 = replay_batch(copy.deepcopy(batch), SqliteEventStore(db))
            self.assertEqual(report2["applied"], 0)
            self.assertEqual(report1["event_stream_hash"], report2["event_stream_hash"])
            svc = RuleCertificationService(SqliteEventStore(db))
            self.assertEqual(
                svc.get_state().packages["teqball-v1"].status, "published")

    def test_cli_sample_runs_and_checks_hash(self):
        result = subprocess.run(
            [sys.executable, "-m", "src.replay", "data/replay_sample.json"],
            cwd=ROOT, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["deduplicated"], 1)
        self.assertEqual(report["rejected"], 0)
        self.assertIn("arr-north-02", report["state"]["frozen_arrangements"])
        self.assertIn("rev-2", report["state"]["pending_revisions"])
        # --check 用错误哈希应非零退出。
        bad = subprocess.run(
            [sys.executable, "-m", "src.replay", "data/replay_sample.json",
             "--check", "deadbeef"],
            cwd=ROOT, text=True, capture_output=True)
        self.assertEqual(bad.returncode, 1)
        good = subprocess.run(
            [sys.executable, "-m", "src.replay", "data/replay_sample.json",
             "--check", report["event_stream_hash"]],
            cwd=ROOT, text=True, capture_output=True)
        self.assertEqual(good.returncode, 0, good.stderr)


if __name__ == "__main__":
    unittest.main()
