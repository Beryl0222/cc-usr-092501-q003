"""证明一：并发签署不会越权。

多个真实 HTTP 线程同时为同一份待审修订签署时：
- 同一角色只能有一人成功（后到者 409），不会出现两个技术签署；
- 提交人本人并发复核必被拒绝；
- 三人不得是同一身份；
- 三方齐备只发布一次，事件流中恰有一条 PACKAGE_PUBLISHED。
"""

from __future__ import annotations

import threading
import unittest

from tests.support import request, server_harness


def post_async(base, path, body, results, index, barrier=None):
    if barrier is not None:
        barrier.wait()  # 所有线程同时越过起跑线，制造真正的竞争窗口
    results[index] = request(base, "POST", path, body)


class ConcurrentSignoffTest(unittest.TestCase):
    def test_same_role_race_yields_single_signature(self):
        with server_harness() as (base, _db, _svc):
            request(base, "POST", "/sports", {"sport": "teqball", "name": "台克球"})
            request(base, "POST", "/packages",
                    {"package_id": "teq-v1", "sport": "teqball",
                     "stage": "qualification", "clauses": {"touch_limit": 3}})
            request(base, "POST", "/packages/teq-v1/revisions",
                    {"revision_id": "rev-1", "submitted_by": "alice",
                     "submitter_role": "technical", "change_class": "initial",
                     "summary": "首版", "content": {"touch_limit": 3}})

            # 两个候选同时抢 technical 角色；提交人 alice 同时试图复核；
            # medical/ops 正常签署。
            calls = [
                ("/revisions/rev-1/sign", {"signer": "bob", "role": "technical"}),
                ("/revisions/rev-1/sign", {"signer": "bob2", "role": "technical"}),
                ("/revisions/rev-1/sign", {"signer": "alice", "role": "medical_safety"}),
                ("/revisions/rev-1/sign", {"signer": "carol", "role": "medical_safety"}),
                ("/revisions/rev-1/sign", {"signer": "dave", "role": "competition_ops"}),
            ]
            results: list = [None] * len(calls)
            barrier = threading.Barrier(len(calls))
            threads = [threading.Thread(target=post_async,
                                        args=(base, p, b, results, i, barrier))
                       for i, (p, b) in enumerate(calls)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            statuses = [r[0] for r in results]
            # 两个 technical 恰有一个成功；alice 自复核必失败。
            technical_results = [statuses[0], statuses[1]]
            self.assertEqual(sorted(technical_results), [200, 409])
            self.assertEqual(statuses[2], 409, "提交人不能复核自己的修订")
            self.assertEqual(statuses[3], 200)
            self.assertEqual(statuses[4], 200)

            status, pkg = request(base, "GET", "/packages/teq-v1")
            self.assertEqual(status, 200)
            self.assertEqual(pkg["status"], "published")
            rev = next(r for r in pkg["revisions"] if r["revision_id"] == "rev-1")
            self.assertEqual(set(rev["signatures"]),
                             {"technical", "medical_safety", "competition_ops"})
            self.assertNotIn("alice", rev["signatures"].values())
            # 三个角色的签署人必须互不相同。
            self.assertEqual(len(set(rev["signatures"].values())), 3)

            status, audit = request(base, "GET", "/events")
            publishes = [e for e in audit["events"]
                         if e["event_type"] == "PACKAGE_PUBLISHED"]
            self.assertEqual(len(publishes), 1, "三方齐备只能发布一次")

    def test_concurrent_distinct_roles_all_succeed_once(self):
        with server_harness() as (base, _db, _svc):
            request(base, "POST", "/sports", {"sport": "mma", "name": "综合格斗"})
            request(base, "POST", "/packages",
                    {"package_id": "mma-v1", "sport": "mma", "stage": "finals"})
            request(base, "POST", "/packages/mma-v1/revisions",
                    {"revision_id": "r1", "submitted_by": "zoe",
                     "submitter_role": "competition_ops", "change_class": "initial",
                     "summary": "禁用动作清单", "content": {"banned": ["spiking"]}})
            calls = [
                {"signer": "amy", "role": "technical"},
                {"signer": "ben", "role": "medical_safety"},
                {"signer": "cal", "role": "competition_ops"},
            ]
            results: list = [None] * 3
            barrier = threading.Barrier(3)
            threads = [threading.Thread(
                target=post_async,
                args=(base, "/revisions/r1/sign", body, results, i, barrier))
                for i, body in enumerate(calls)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual({r[0] for r in results}, {200})
            status, pkg = request(base, "GET", "/packages/mma-v1")
            self.assertEqual(pkg["status"], "published")


if __name__ == "__main__":
    unittest.main()
