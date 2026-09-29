"""证明四：进程恢复后待审修订不会丢失。

待审修订、未裁决争议、只签了一两个角色的规则包都必须随事件持久化：
模拟进程退出（关闭 HTTP 服务、丢弃内存中的服务对象），再用同一个
SQLite 文件启动新实例，状态完全由事件流重建，业务可以无缝继续。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.api import build_server
from src.domain import RuleCertificationService, fold
from src.store import SqliteEventStore
from tests.support import request, server_harness


def post(base, path, body):
    status, payload = request(base, "POST", path, body)
    assert status in (200, 201), (path, status, payload)
    return payload


class ProcessRecoveryTest(unittest.TestCase):
    def test_pending_revision_survives_restart(self):
        tmp = tempfile.TemporaryDirectory()
        db_path = str(Path(tmp.name) / "events.sqlite")
        self.addCleanup(tmp.cleanup)

        # —— 进程生命周期 1：登记、发布 v1、锁定，然后提交一条尚未签完的修订 ——
        httpd = build_server(db_path, port=0)
        import threading
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{httpd.server_address[1]}"
        try:
            post(base, "/sports", {"sport": "teqball", "name": "台克球"})
            post(base, "/packages",
                 {"package_id": "teq-v1", "sport": "teqball",
                  "stage": "qualification", "clauses": {"touch_limit": 3}})
            post(base, "/packages/teq-v1/revisions",
                 {"revision_id": "rev-1", "submitted_by": "alice",
                  "submitter_role": "technical", "change_class": "initial",
                  "summary": "首版", "content": {"touch_limit": 3}})
            for signer, role in (("bob", "technical"), ("carol", "medical_safety"),
                                 ("dave", "competition_ops")):
                post(base, "/revisions/rev-1/sign", {"signer": signer, "role": role})
            post(base, "/zones/north/locks", {"package_id": "teq-v1"})

            # 边界勘误产生 v2 草稿，修订只拿到一个签署，进程此刻崩溃。
            post(base, "/packages/teq-v1/revisions",
                 {"revision_id": "rev-2", "submitted_by": "erin",
                  "submitter_role": "technical", "change_class": "boundary",
                  "summary": "触球上限勘误为2", "content": {"touch_limit": 2}})
            post(base, "/revisions/rev-2/sign",
                 {"signer": "frank", "role": "technical"})
        finally:
            httpd.shutdown()
            httpd.server_close()
        # 到这里原服务对象、内存状态全部丢弃，只剩磁盘上的 SQLite 文件。

        # —— 进程生命周期 2：新实例直接打开同一文件 ——
        store = SqliteEventStore(db_path)
        svc = RuleCertificationService(store)
        state = svc.get_state()

        self.assertIn("teq-v1", state.packages)
        self.assertEqual(state.packages["teq-v1"].status, "published")
        self.assertIn("north", state.packages["teq-v1"].locked_by)
        self.assertIn("teq-v2", state.packages, "新版本草稿必须随事件恢复")
        rev2 = state.revisions["rev-2"]
        self.assertEqual(rev2.status, "pending", "待审修订不能丢失或被自动批准")
        self.assertEqual(rev2.package_id, "teq-v2")
        self.assertEqual(set(rev2.signatures), {"technical"},
                         "已完成的一个签署必须保留")

        # 业务继续：补齐另外两个角色，v2 正常生效——不需要重新提交修订。
        svc.sign_revision("rev-2", signer="grace", role="medical_safety")
        svc.sign_revision("rev-2", signer="heidi", role="competition_ops")
        state = svc.get_state()
        self.assertEqual(state.revisions["rev-2"].status, "approved")
        self.assertEqual(state.packages["teq-v2"].status, "published")
        self.assertEqual(state.packages["teq-v2"].clauses["touch_limit"], 2)

    def test_event_stream_rebuilds_identical_state(self):
        # 同一份事件流在全新进程里 fold，应得到逐字段一致的状态视图。
        with server_harness(cleanup=False) as (base, db_path, svc_before):
            post(base, "/sports", {"sport": "mma", "name": "综合格斗"})
            post(base, "/packages",
                 {"package_id": "mma-v1", "sport": "mma", "stage": "finals",
                  "clauses": {"banned_moves": ["soccer_kick"]}})
            post(base, "/packages/mma-v1/revisions",
                 {"revision_id": "r1", "submitted_by": "zoe",
                  "submitter_role": "medical_safety", "change_class": "initial",
                  "summary": "禁用动作", "content": {"banned_moves": ["soccer_kick"]}})
            for signer, role in (("amy", "technical"), ("zoe2", "medical_safety"),
                                 ("cal", "competition_ops")):
                post(base, "/revisions/r1/sign", {"signer": signer, "role": role})
            post(base, "/certificates/receipts",
                 {"cert_id": "glove-1", "event_id": "rcpt-g1",
                  "detail": {"model": "OpenGlove X"}})
            view_before = svc_before.get_package("mma-v1")

        try:
            store = SqliteEventStore(db_path)
            svc_after = RuleCertificationService(store)
            view_after = svc_after.get_package("mma-v1")
            self.assertEqual(view_before, view_after)
            self.assertEqual(svc_after.get_state().certificates["glove-1"].status, "issued")
            # 空操作重放不应产生重复事件。
            count_after = len(store.all_events())
            svc_after2 = RuleCertificationService(SqliteEventStore(db_path))
            self.assertEqual(len(svc_after2.store.all_events()), count_after)
        finally:
            Path(db_path).unlink(missing_ok=True)
            Path(db_path).parent.rmdir()

    def test_open_dispute_survives_restart(self):
        with server_harness(cleanup=False) as (base, db_path, _svc):
            post(base, "/certificates/receipts",
                 {"cert_id": "net-Z", "event_id": "rcpt-Z-1",
                  "detail": {"v": 1}})
            post(base, "/certificates/receipts",
                 {"cert_id": "net-Z", "event_id": "rcpt-Z-1",
                  "detail": {"v": 2}})

        try:
            svc = RuleCertificationService(SqliteEventStore(db_path))
            state = svc.get_state()
            self.assertIn("dispute-rcpt-Z-1", state.disputes, "未裁决争议必须恢复")
            dispute = state.disputes["dispute-rcpt-Z-1"]
            self.assertEqual(dispute.status, "open")
            self.assertEqual(len(dispute.variants), 2)
            with self.assertRaisesRegex(Exception, "争议"):
                svc.revoke_certificate("net-Z")
        finally:
            Path(db_path).unlink(missing_ok=True)
            Path(db_path).parent.rmdir()


if __name__ == "__main__":
    unittest.main()
