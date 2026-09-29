"""HTTP API 的端到端测试。"""

from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from src.api import build_server
from src.store import EventStore


def t(day: int, hour: int = 10) -> str:
    return f"2026-09-{day:02d}T{hour:02d}:00:00+08:00"


class ApiClient:
    def __init__(self, base: str) -> None:
        self.base = base

    def call(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read().decode("utf-8"))


class HttpApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = build_server("127.0.0.1", 0, EventStore(":memory:"))
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.api = ApiClient(f"http://127.0.0.1:{self.port}")

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def _publish_tecq(self) -> None:
        self.api.call("POST", "/sports", {"code": "tecqball", "name": "台克球"})
        self.api.call("POST", "/stages", {"stage_id": "finals", "name": "总决赛"})
        self.api.call("POST", "/packages", {
            "package_id": "tecq-rules", "sport_code": "tecqball",
            "stage_id": "finals", "title": "台克球总决赛规则包",
        })
        self.api.call("POST", "/packages/tecq-rules/revisions", {
            "submitted_by": "editor-li", "title": "r1", "summary": "三次触球",
            "contents": {"touch_limit": 3},
        })
        for role, signer in [
            ("technical", "tech-wang"),
            ("medical_safety", "med-chen"),
            ("competition_operations", "ops-zhao"),
        ]:
            self.api.call("POST", "/packages/tecq-rules/signatures",
                          {"role": role, "signer": signer})

    def test_health_and_full_flow(self) -> None:
        status, body = self.api.call("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")

        self._publish_tecq()
        status, body = self.api.call("GET", "/packages/tecq-rules")
        self.assertEqual(status, 200)
        self.assertEqual(body["current_revision"]["number"], 1)

        # 锁定与查询
        status, body = self.api.call("POST", "/venues/zone-a/lock",
                                     {"name": "北京赛区", "package_id": "tecq-rules"})
        self.assertEqual(status, 201)
        self.assertEqual(body["snapshot"]["revision"], 1)

        status, venue = self.api.call("GET", "/venues/zone-a")
        self.assertEqual(status, 200)
        self.assertEqual(venue["lock"]["snapshot"]["clauses"], {"touch_limit": 3})

    def test_self_review_is_forbidden_over_http(self) -> None:
        self._publish_tecq_base_without_publish()
        status, body = self.api.call("POST", "/packages/tecq-rules/signatures",
                                     {"role": "technical", "signer": "editor-li"})
        self.assertEqual(status, 400)
        self.assertIn("不能复核自己提交的修订", body["error"])

    def _publish_tecq_base_without_publish(self) -> None:
        self.api.call("POST", "/sports", {"code": "tecqball", "name": "台克球"})
        self.api.call("POST", "/stages", {"stage_id": "finals", "name": "总决赛"})
        self.api.call("POST", "/packages", {
            "package_id": "tecq-rules", "sport_code": "tecqball",
            "stage_id": "finals", "title": "台克球规则包",
        })
        self.api.call("POST", "/packages/tecq-rules/revisions", {
            "submitted_by": "editor-li", "title": "r1", "summary": "s",
            "contents": {"touch_limit": 3},
        })

    def test_lock_before_publish_is_400(self) -> None:
        self._publish_tecq_base_without_publish()
        status, body = self.api.call("POST", "/venues/zone-a/lock",
                                     {"package_id": "tecq-rules"})
        self.assertEqual(status, 400)
        self.assertIn("三方签署", body["error"])

    def test_historical_query_and_revocation_scope(self) -> None:
        self._publish_tecq()
        self.api.call("POST", "/venues/zone-a/lock", {"package_id": "tecq-rules"})
        self.api.call("POST", "/certificates/cert-x/receipts", {
            "package_id": "tecq-rules", "equipment_code": "tecq-table",
            "receipt_id": "R-1", "content_hash": "hash-1",
        })
        self.api.call("POST", "/fixtures", {
            "fixture_id": "fx-101", "venue_id": "zone-a", "package_id": "tecq-rules",
            "scheduled_start": t(20, 19), "cert_refs": ["cert-x"],
        })
        self.api.call("POST", "/fixtures", {
            "fixture_id": "fx-103", "venue_id": "zone-a", "package_id": "tecq-rules",
            "scheduled_start": t(21, 19), "cert_refs": ["cert-other"],
        })

        # 重复回执幂等（撤销之前）
        status, body = self.api.call("POST", "/certificates/cert-x/receipts", {
            "package_id": "tecq-rules", "equipment_code": "tecq-table",
            "receipt_id": "R-1", "content_hash": "hash-1",
        })
        self.assertEqual(body["status"], "duplicate")

        status, body = self.api.call("POST", "/certificates/cert-x/revoke",
                                     {"reason": "抽检不合格"})
        self.assertEqual(status, 200)
        self.assertEqual(body["frozen_fixture_ids"], ["fx-101"])

        status, body = self.api.call("GET", "/fixtures")
        frozen = {f["fixture_id"]: f["frozen"] for f in body["fixtures"]}
        self.assertEqual(frozen, {"fx-101": True, "fx-103": False})

        # 撤销后的回执不再受理
        status, body = self.api.call("POST", "/certificates/cert-x/receipts", {
            "package_id": "tecq-rules", "equipment_code": "tecq-table",
            "receipt_id": "R-3", "content_hash": "hash-3",
        })
        self.assertEqual(status, 400)
        self.assertIn("已撤销", body["error"])

        # 同编号异内容 → 争议
        status, body = self.api.call("POST", "/certificates/cert-other/receipts", {
            "package_id": "tecq-rules", "equipment_code": "net",
            "receipt_id": "R-2", "content_hash": "hash-a",
        })
        status, body = self.api.call("POST", "/certificates/cert-other/receipts", {
            "package_id": "tecq-rules", "equipment_code": "net",
            "receipt_id": "R-2", "content_hash": "hash-b",
        })
        self.assertEqual(body["status"], "disputed")

    def test_unknown_routes_and_bad_bodies(self) -> None:
        status, body = self.api.call("GET", "/packages/nope")
        self.assertEqual(status, 404)
        status, body = self.api.call("POST", "/sports", {"code": "x"})
        self.assertEqual(status, 400)
        self.assertIn("缺少字段", body["error"])


if __name__ == "__main__":
    unittest.main()
