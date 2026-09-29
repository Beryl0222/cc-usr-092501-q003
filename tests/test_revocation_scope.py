"""证明三：器材证书撤销的影响范围准确。

- 三张证书 net-A / net-B / ball-C；
- 三个场地安排分别引用 A、B、A+B；
- 另一个赛区的安排也引用 A；
- 撤销 net-A：只有实际引用 A 的安排被冻结（arr-a、arr-ab、arr-other），
  仅引用 B 的 arr-b 保持 active；
- 离线回执乱序送达、重复送达幂等；同编号异内容进入争议，
  争议中的证书不能登记引用，也不能直接撤销。
"""

from __future__ import annotations

import threading
import unittest

from tests.support import publish_package, request, server_harness


def post(base, path, body):
    status, payload = request(base, "POST", path, body)
    assert status in (200, 201), (path, status, payload)
    return payload


def arrangements_map(payload):
    return {a["arrangement_id"]: a for a in payload["arrangements"]}


class CertificateRevocationScopeTest(unittest.TestCase):
    def _setup(self, base):
        publish_package(base, sport="teqball", package_id="teq-v1")
        # 离线认证回执：先到一条、再来重复的、再来一条乱序（更晚到达但发生时间更早）。
        post(base, "/certificates/receipts",
             {"cert_id": "net-A", "event_id": "rcpt-A-1",
              "detail": {"model": "TeqNet 500"},
              "occurred_at": "2026-09-25T10:00:00+08:00"})
        dup = post(base, "/certificates/receipts",
                   {"cert_id": "net-A", "event_id": "rcpt-A-1",
                    "detail": {"model": "TeqNet 500"},
                    "occurred_at": "2026-09-25T10:00:00+08:00"})
        self.assertTrue(dup["deduplicated"], "同编号同内容必须幂等丢弃")
        post(base, "/certificates/receipts",
             {"cert_id": "net-B", "event_id": "rcpt-B-1",
              "detail": {"model": "TeqNet 600"}})
        post(base, "/certificates/receipts",
             {"cert_id": "ball-C", "event_id": "rcpt-C-1",
              "detail": {"model": "MatchBall 2026"}})

        post(base, "/arrangements",
             {"arrangement_id": "arr-a", "zone": "north",
              "session_id": "north-qf-01", "cert_refs": ["net-A"]})
        post(base, "/arrangements",
             {"arrangement_id": "arr-b", "zone": "north",
              "session_id": "north-qf-02", "cert_refs": ["net-B"]})
        post(base, "/arrangements",
             {"arrangement_id": "arr-ab", "zone": "north",
              "session_id": "north-qf-03", "cert_refs": ["net-A", "net-B"]})
        post(base, "/arrangements",
             {"arrangement_id": "arr-other", "zone": "south",
              "session_id": "south-qf-09", "cert_refs": ["net-A", "ball-C"]})

    def test_revocation_freezes_only_referencing_arrangements(self):
        with server_harness() as (base, _db, _svc):
            self._setup(base)
            result = post(base, "/certificates/net-A/revoke",
                          {"reason": "网柱批次不达标"})
            self.assertEqual(set(result["frozen_arrangements"]),
                             {"arr-a", "arr-ab", "arr-other"})
            self.assertNotIn("arr-b", result["frozen_arrangements"],
                             "未引用 net-A 的安排不得被冻结")

            status, payload = request(base, "GET", "/arrangements")
            amap = arrangements_map(payload)
            self.assertEqual(amap["arr-a"]["status"], "frozen")
            self.assertEqual(amap["arr-a"]["frozen_for"], ["net-A"])
            self.assertEqual(amap["arr-b"]["status"], "active")
            self.assertEqual(amap["arr-b"]["frozen_for"], [])
            self.assertEqual(amap["arr-ab"]["status"], "frozen")
            self.assertEqual(amap["arr-ab"]["frozen_for"], ["net-A"])
            # arr-ab 仍引用 net-B；撤销 net-B 时它会再追加一个冻结原因。
            post(base, "/certificates/net-B/revoke", {"reason": "网面张力异常"})
            status, payload = request(base, "GET", "/arrangements")
            amap = arrangements_map(payload)
            self.assertEqual(sorted(amap["arr-ab"]["frozen_for"]),
                             ["net-A", "net-B"])
            self.assertEqual(amap["arr-a"]["frozen_for"], ["net-A"],
                             "arr-a 不引用 net-B，不应重复记账")

            # 证书状态与事件审计。
            status, certs = request(base, "GET", "/certificates")
            cmap = {c["cert_id"]: c for c in certs["certificates"]}
            self.assertEqual(cmap["net-A"]["status"], "revoked")
            self.assertEqual(cmap["net-B"]["status"], "revoked")
            self.assertEqual(cmap["ball-C"]["status"], "issued")

    def test_same_id_different_content_opens_dispute(self):
        with server_harness() as (base, _db, _svc):
            post(base, "/certificates/receipts",
                 {"cert_id": "net-D", "event_id": "rcpt-D-1",
                  "detail": {"max_load": 120}})
            # 同编号、内容不同 → 争议。
            post(base, "/certificates/receipts",
                 {"cert_id": "net-D", "event_id": "rcpt-D-1",
                  "detail": {"max_load": 90}})
            status, disputes = request(base, "GET", "/disputes")
            self.assertEqual(status, 200)
            self.assertEqual(len(disputes["disputes"]), 1)
            dispute = disputes["disputes"][0]
            self.assertEqual(dispute["event_id"], "rcpt-D-1")
            hashes = {v["content_hash"] for v in dispute["variants"]}
            self.assertEqual(len(hashes), 2, "争议须保留两个内容变体")

            # 争议未裁决：不能把证书用于场地安排，也不能撤销。
            status, body = request(base, "POST", "/arrangements",
                                   {"arrangement_id": "arr-d", "zone": "north",
                                    "session_id": "north-qf-01",
                                    "cert_refs": ["net-D"]})
            self.assertEqual(status, 409)
            status, body = request(base, "POST", "/certificates/net-D/revoke",
                                   {"reason": "争议中试图撤销"})
            self.assertEqual(status, 409)

            # 裁决后证书恢复可用。
            post(base, "/disputes/dispute-rcpt-D-1/resolve",
                 {"resolution": "采用 max_load=90 的来件"})
            post(base, "/arrangements",
                 {"arrangement_id": "arr-d", "zone": "north",
                  "session_id": "north-qf-01", "cert_refs": ["net-D"]})

    def test_concurrent_duplicate_receipts_stay_idempotent(self):
        with server_harness() as (base, _db, _svc):
            publish_package(base)

            def fire(results, i):
                results[i] = request(base, "POST", "/certificates/receipts",
                                     {"cert_id": "net-E", "event_id": "rcpt-E-1",
                                      "detail": {"model": "TeqNet 700"}})

            results = [None] * 8
            barrier = threading.Barrier(8)

            def worker(i):
                barrier.wait()
                fire(results, i)

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            applied = sum(1 for s, b in results if not b.get("deduplicated")
                          and any(e["event_type"] == "CERTIFICATE_ISSUED"
                                  for e in b.get("events", [])))
            self.assertEqual(applied, 1, "8 个并发相同回执只能有一条签发")
            status, certs = request(base, "GET", "/certificates")
            self.assertEqual(len(certs["certificates"]), 1)


if __name__ == "__main__":
    unittest.main()
