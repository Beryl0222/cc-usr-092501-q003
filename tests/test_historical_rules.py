"""证明二：旧赛程仍能还原当时规则。

流程覆盖真实业务顺序：
1. v1 三方签署生效、赛区锁定、两个场次排定；
2. 对 v1 追加一条非破坏性澄清（INT-2026-07 墙面回球解释）；
3. 场次 A 开赛——它的规则从此固定；
4. 赛后再追加澄清，并发生改变计分边界的勘误（派生 v2），
   未开赛场次 B 改引 v2；
5. 校验：A 的当时规则停留在开赛时刻（含赛前澄清、不含赛后澄清、
   不含 v2）；显式 as_of 能还原更早与更晚的视角；B 适用 v2。
"""

from __future__ import annotations

import time
import unittest

from tests.support import publish_package, request, server_harness


def post(base, path, body):
    status, payload = request(base, "POST", path, body)
    assert status in (200, 201), (path, status, payload)
    return payload


class HistoricalRuleReplayTest(unittest.TestCase):
    def test_started_session_pins_rules_at_start_time(self):
        with server_harness() as (base, _db, _svc):
            publish_package(
                base, sport="padbol", stage="qualification",
                package_id="pad-v1", clauses={"wall_return": "allowed"})

            post(base, "/sessions",
                 {"session_id": "pad-A", "zone": "north", "sport": "padbol",
                  "stage": "qualification", "package_id": "pad-v1"})
            post(base, "/sessions",
                 {"session_id": "pad-B", "zone": "north", "sport": "padbol",
                  "stage": "qualification", "package_id": "pad-v1"})

            # 赛前追加的澄清：A、B 都应看到。
            time.sleep(0.01)
            t_before_clarification = _now()
            time.sleep(0.01)
            post(base, "/packages/pad-v1/revisions",
                 {"revision_id": "clar-before", "submitted_by": "frank",
                  "submitter_role": "technical", "change_class": "clarification",
                  "summary": "墙面二次回球判罚口径",
                  "content": {"reference": "INT-2026-07"}})

            time.sleep(0.01)
            post(base, "/sessions/pad-A/start", {})
            post(base, "/sessions/pad-A/finish", {})

            # 赛后动作：新澄清 + 改变计分边界的勘误（派生 v2）。
            time.sleep(0.01)
            post(base, "/packages/pad-v1/revisions",
                 {"revision_id": "clar-after", "submitted_by": "gina",
                  "submitter_role": "technical", "change_class": "clarification",
                  "summary": "赛后补充：贴地球解释",
                  "content": {"reference": "INT-2026-09"}})
            created = post(base, "/packages/pad-v1/revisions",
                           {"revision_id": "rev-boundary", "submitted_by": "henry",
                            "submitter_role": "technical", "change_class": "boundary",
                            "summary": "墙面回球直接得分计分调整",
                            "content": {"wall_return_score": 2}})
            successor = next(
                e["payload"]["successor_id"]
                for e in created["events"]
                if e["event_type"] == "NEW_VERSION_DEMANDED")
            self.assertEqual(successor, "pad-v2")

            # v2 三方签署后生效。
            post(base, "/packages/pad-v2/revisions",
                 {"revision_id": "rev-v2", "submitted_by": "ivan",
                  "submitter_role": "technical", "change_class": "initial",
                  "summary": "v2 首版", "content": {"wall_return_score": 2}})
            for signer, role in (("judy", "technical"), ("kyle", "medical_safety"),
                                 ("leo", "competition_ops")):
                post(base, "/revisions/rev-v2/sign",
                     {"signer": signer, "role": role})
            # B 尚未开赛，允许改引新版本；A 已完赛，禁止改引。
            status, _ = request(base, "POST", "/sessions/pad-A/repin",
                                {"package_id": "pad-v2"})
            self.assertEqual(status, 409, "已完赛场次的判罚不可追改")
            post(base, "/sessions/pad-B/repin", {"package_id": "pad-v2"})

            # 断言一：A 默认返回开赛时刻固定的规则。
            status, view_a = request(base, "GET", "/sessions/pad-A/rules")
            self.assertEqual(status, 200)
            self.assertEqual(view_a["pinned_package_id"], "pad-v1")
            refs = [c["reference"] for c in view_a["rules"]["clarifications"]]
            self.assertIn("INT-2026-07", refs)
            self.assertNotIn("INT-2026-09", refs, "赛后澄清不得追改 A 的判罚依据")
            self.assertNotIn("wall_return_score", view_a["rules"]["clauses"])
            self.assertEqual(view_a["rules"]["clauses"]["wall_return"], "allowed")

            # 断言二：B 当前适用新版本。
            status, view_b = request(base, "GET", "/sessions/pad-B/rules")
            self.assertEqual(view_b["pinned_package_id"], "pad-v2")
            self.assertEqual(view_b["rules"]["version_no"], 2)

            # 断言三：显式 as_of 可以还原“赛前澄清尚未到达”的更早视角。
            from urllib.parse import quote
            status, early = request(
                base, "GET", f"/packages/pad-v1?as_of={quote(t_before_clarification, safe='')}")
            self.assertEqual(early["clarifications"], [])

            # 断言四：从 A 开赛视角看，影响清单里只有未开赛的 B，没有 A。
            status, events = request(base, "GET", "/events")
            impact = next(e["payload"] for e in events["events"]
                          if e["event_type"] == "IMPACT_LISTED")
            self.assertEqual(impact["affected_sessions"], ["pad-B"])

    def test_clauses_immutable_after_lock(self):
        with server_harness() as (base, _db, _svc):
            publish_package(base, sport="teqball", package_id="teq-v1",
                            clauses={"touch_limit": 3})
            # 对已生效包提交普通（非澄清、非边界）修订必须被拒绝，
            # 不能原地改条款。
            status, body = request(base, "POST", "/packages/teq-v1/revisions",
                                   {"revision_id": "sneaky", "submitted_by": "mia",
                                    "submitter_role": "technical",
                                    "change_class": "initial",
                                    "summary": "试图直接改条款"})
            self.assertEqual(status, 409)


def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    unittest.main()
