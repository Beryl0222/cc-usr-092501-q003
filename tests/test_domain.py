"""领域服务测试：签署越权、时点还原、撤销影响范围、进程恢复等核心保证。"""

from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from src.service import ConcurrentUpdate, DomainError, RuleCertService
from src.store import EventConflict, EventStore


def t(day: int, hour: int = 10) -> str:
    return f"2026-09-{day:02d}T{hour:02d}:00:00+08:00"


def build_base(store: EventStore) -> RuleCertService:
    """项目/阶段/规则包 r1 待签署的基线场景。"""
    svc = RuleCertService(store)
    svc.register_sport("tecqball", "台克球", t(1))
    svc.define_stage("finals", "总决赛", t(1))
    svc.draft_package("tecq-rules", "tecqball", "finals", "台克球总决赛规则包", t(2))
    svc.submit_revision(
        "tecq-rules", "editor-li", "台克球规则 r1", "每回合最多三次触球",
        {"touch_limit": 3}, at=t(2),
    )
    return svc


def publish_r1(svc: RuleCertService) -> None:
    svc.sign_revision("tecq-rules", "technical", "tech-wang", t(3, 9))
    svc.sign_revision("tecq-rules", "medical_safety", "med-chen", t(3, 10))
    svc.sign_revision("tecq-rules", "competition_operations", "ops-zhao", t(3, 11))


class SigningRulesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = build_base(EventStore(":memory:"))

    def test_signer_cannot_review_own_revision(self) -> None:
        with self.assertRaises(DomainError) as cm:
            self.svc.sign_revision("tecq-rules", "technical", "editor-li", t(3))
        self.assertIn("不能复核自己提交的修订", str(cm.exception))

    def test_unknown_role_rejected(self) -> None:
        with self.assertRaises(DomainError):
            self.svc.sign_revision("tecq-rules", "referee_chief", "judge-01", t(3))

    def test_publish_requires_three_distinct_roles(self) -> None:
        self.svc.sign_revision("tecq-rules", "technical", "tech-wang", t(3, 9))
        # 同一职能方换人签署不允许
        with self.assertRaises(DomainError):
            self.svc.sign_revision("tecq-rules", "technical", "tech-other", t(3, 10))
        # 同一人重复签署幂等
        again = self.svc.sign_revision("tecq-rules", "technical", "tech-wang", t(3, 10))
        self.assertEqual(again["status"], "already_signed")
        # 两方签完尚未发布
        self.svc.sign_revision("tecq-rules", "medical_safety", "med-chen", t(3, 11))
        view = self.svc.get_package("tecq-rules")
        self.assertIsNone(view["current_revision"])
        # 第三方签齐才发布
        result = self.svc.sign_revision("tecq-rules", "competition_operations", "ops-zhao", t(3, 12))
        self.assertEqual(result["status"], "published")
        view = self.svc.get_package("tecq-rules")
        self.assertEqual(view["current_revision"]["number"], 1)
        self.assertEqual(set(view["current_revision"]["signatures"]),
                         {"technical", "medical_safety", "competition_operations"})

    def test_concurrent_signatures_never_exceed_authority(self) -> None:
        """多线程同时签署：提交人永不成功，每职能方至多一签，发布恰有一次。"""
        attempts = [
            ("technical", "tech-wang"),
            ("medical_safety", "med-chen"),
            ("competition_operations", "ops-zhao"),
            # 提交人换着职能方想自己复核，必须全部被拒
            ("technical", "editor-li"),
            ("medical_safety", "editor-li"),
            ("competition_operations", "editor-li"),
            # 同职能方竞争者
            ("technical", "tech-rival"),
            ("medical_safety", "med-rival"),
            ("competition_operations", "ops-rival"),
            # 合法签署人各自再重复一次，检验幂等
            ("technical", "tech-wang"),
            ("medical_safety", "med-chen"),
            ("competition_operations", "ops-zhao"),
        ]
        outcomes: list[tuple[str, str, object]] = []
        barrier = threading.Barrier(len(attempts))

        def worker(role: str, signer: str) -> None:
            barrier.wait()
            try:
                self.svc.sign_revision("tecq-rules", role, signer, t(3, 15))
                outcomes.append((role, signer, "ok"))
            except DomainError as error:
                outcomes.append((role, signer, str(error)))
            except ConcurrentUpdate as error:
                outcomes.append((role, signer, f"conflict:{error}"))

        threads = [threading.Thread(target=worker, args=a) for a in attempts]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        # 提交人的每一次尝试都被拒绝
        for role, signer, result in outcomes:
            if signer == "editor-li":
                self.assertNotEqual(result, "ok", f"提交人越权签署成功：{role}")

        events = self.svc.store.load_events()
        sign_events = [e for e in events if e["event_type"] == "REVIEW_SIGNED"]
        publish_events = [e for e in events if e["event_type"] == "PACKAGE_PUBLISHED"]
        roles_taken = {e["data"]["role"]: e["data"]["signer"] for e in sign_events}
        self.assertEqual(len(sign_events), 3, "恰好三方各签署一次")
        self.assertEqual(set(roles_taken), {"technical", "medical_safety", "competition_operations"})
        # 三个职能方的胜出者都必须是合法候选人，绝不能是提交人；具体谁在
        # 锁竞争中获胜由调度决定，不构成越权。
        self.assertIn(roles_taken["technical"], {"tech-wang", "tech-rival"})
        self.assertIn(roles_taken["medical_safety"], {"med-chen", "med-rival"})
        self.assertIn(roles_taken["competition_operations"], {"ops-zhao", "ops-rival"})
        # 落选的合法竞争者：要么被告知该职能方已由他人签署，要么在三方
        # 签齐发布后被告知已无待审修订——两种拒绝都合法，关键是没有第二条签署落地。
        allowed = ("不能复核自己提交的修订", "已由", "没有待签署的修订")
        for role, signer, result in outcomes:
            if result != "ok":
                self.assertIsInstance(result, str)
                self.assertTrue(
                    any(hint in result for hint in allowed),
                    f"意外的拒绝原因：{role}/{signer} -> {result}",
                )
        self.assertEqual(len(publish_events), 1, "三方签齐后只发布一次")

    def test_concurrent_conflicting_commands_only_one_wins(self) -> None:
        """两个修订提交并发竞争待审名额：恰好一个成功，另一个拿到冲突。"""
        def submit(signer: str) -> object:
            try:
                return self.svc.sign_revision("tecq-rules", "technical", signer, t(3))
            except (DomainError, ConcurrentUpdate) as error:
                return error

        # 先制造第二个待审修订冲突不容易（同包只允许一个待审）；改为并发签署同一职能方
        results: list[object] = []
        barrier = threading.Barrier(2)

        def worker(name: str) -> None:
            barrier.wait()
            results.append(submit(name))

        t1 = threading.Thread(target=worker, args=("tech-a",))
        t2 = threading.Thread(target=worker, args=("tech-b",))
        t1.start(); t2.start(); t1.join(); t2.join()
        successes = [r for r in results if not isinstance(r, Exception)]
        failures = [r for r in results if isinstance(r, Exception)]
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(failures), 1)


class RevisionAndLockTest(unittest.TestCase):
    def setUp(self) -> None:
        store = EventStore(":memory:")
        self.svc = build_base(store)
        publish_r1(self.svc)
        self.svc.lock_venue("zone-a", "北京赛区", "tecq-rules", t(4))
        self.svc.declare_fixture("fx-101", "zone-a", "tecq-rules", t(20, 19), ["cert-x"], t(5))
        self.svc.declare_fixture("fx-102", "zone-a", "tecq-rules", t(6, 19), ["cert-x"], t(5))
        self.svc.start_fixture("fx-102", t(6, 18))

    def test_lock_cannot_precede_publication(self) -> None:
        svc = build_base(EventStore(":memory:"))  # 未发布
        with self.assertRaises(DomainError):
            svc.lock_venue("zone-b", "上海赛区", "tecq-rules", t(4))

    def test_non_breaking_change_after_lock_must_be_clarification(self) -> None:
        # 已发布后提交不带任何边界标记的修订 → 拒绝，要求走澄清
        with self.assertRaises(DomainError) as cm:
            self.svc.submit_revision(
                "tecq-rules", "editor-li", "台克球规则 r2", "纯文字润色",
                {"touch_limit": 3}, at=t(9),
            )
        self.assertIn("非破坏性澄清", str(cm.exception))

        result = self.svc.append_clarification(
            "tecq-rules", "zone-a", "触球计数口径", "拦网与救球合并计数。",
            ["doc-touch-v1.pdf"], "ops-zhao", t(7),
        )
        self.assertEqual(result["clarification_id"], "clar-001-zone-a")
        # 澄清不改当前修订号
        self.assertEqual(self.svc.get_package("tecq-rules")["current_revision"]["number"], 1)
        venue = self.svc.get_venue("zone-a")
        self.assertEqual(venue["lock"]["revision"], 1)
        self.assertEqual(len(venue["clarifications"]), 1)

    def test_boundary_change_forms_new_version_and_lists_affected_fixtures(self) -> None:
        # r1 锁定后先追加一条非破坏性澄清
        self.svc.append_clarification(
            "tecq-rules", "zone-a", "触球计数口径", "拦网与救球合并计数。",
            ["doc-touch-v1.pdf"], "ops-zhao", t(7),
        )
        result = self.svc.submit_revision(
            "tecq-rules", "editor-li", "台克球规则 r2", "改变计分判定",
            {"touch_limit": 3, "block_exempt": True},
            scoring_changed=True, at=t(10),
        )
        self.assertEqual(result["revision"], 2)
        # fx-102 已开始，不在受影响名单；未开始的 fx-101 在
        self.assertEqual(result["impact_fixture_ids"], ["fx-101"])

        publish_r2 = self.svc
        publish_r2.sign_revision("tecq-rules", "technical", "tech-wang", t(11, 9))
        publish_r2.sign_revision("tecq-rules", "medical_safety", "med-chen", t(11, 10))
        publish_r2.sign_revision("tecq-rules", "competition_operations", "ops-zhao", t(11, 11))

        view = self.svc.get_package("tecq-rules")
        self.assertEqual(view["current_revision"]["number"], 2)
        periods = view["applicability_periods"]
        r1 = next(p for p in periods if p["revision"] == 1)
        r2 = next(p for p in periods if p["revision"] == 2)
        self.assertIsNotNone(r1["effective_until"])
        self.assertIsNone(r2["effective_until"])

        # 旧赛程按当时时间点仍还原到 r1
        old = self.svc.get_package("tecq-rules", as_of=t(8))
        self.assertEqual(old["current_revision"]["number"], 1)
        newer = self.svc.get_package("tecq-rules", as_of=t(12))
        self.assertEqual(newer["current_revision"]["number"], 2)

        # 赛区重新锁定 r2 后，历史时点查询仍能取出 r1 的锁定快照
        self.svc.lock_venue("zone-a", "北京赛区", "tecq-rules", t(12))
        self.assertEqual(self.svc.get_venue("zone-a", as_of=t(8))["lock"]["revision"], 1)
        self.assertEqual(self.svc.get_venue("zone-a")["lock"]["revision"], 2)

        # 澄清在追加之后可见、之前不可见
        self.svc.append_clarification(
            "tecq-rules", "zone-a", "r2 适用说明", "仅适用 r2。",
            ["case-01"], "ops-zhao", t(13))
        venue_at_8 = self.svc.get_venue("zone-a", as_of=t(8))
        self.assertEqual({c["title"] for c in venue_at_8["clarifications"]}, {"触球计数口径"})

    def test_started_fixture_judgments_are_never_retroactively_changed(self) -> None:
        # 即使 r2 发布，已开始场次 fx-102 仍可还原到锁定时的 r1 快照
        self.svc.submit_revision(
            "tecq-rules", "editor-li", "r2", "安全边界变化",
            {"touch_limit": 2}, safety_changed=True, at=t(10))
        self.svc.sign_revision("tecq-rules", "technical", "tech-wang", t(11, 9))
        self.svc.sign_revision("tecq-rules", "medical_safety", "med-chen", t(11, 10))
        self.svc.sign_revision("tecq-rules", "competition_operations", "ops-zhao", t(11, 11))
        venue = self.svc.get_venue("zone-a", as_of=t(6, 19))
        self.assertEqual(venue["lock"]["revision"], 1)
        self.assertEqual(venue["lock"]["snapshot"]["clauses"], {"touch_limit": 3})


class ReceiptAndRevocationTest(unittest.TestCase):
    def setUp(self) -> None:
        store = EventStore(":memory:")
        self.svc = build_base(store)
        publish_r1(self.svc)
        self.svc.lock_venue("zone-a", "北京赛区", "tecq-rules", t(4))
        # 三个场次：101 引用 cert-x 未开始；102 引用 cert-x 已开始；103 引用别的证书
        self.svc.declare_fixture("fx-101", "zone-a", "tecq-rules", t(20, 19), ["cert-x", "cert-y"], t(5))
        self.svc.declare_fixture("fx-102", "zone-a", "tecq-rules", t(6, 19), ["cert-x"], t(5))
        self.svc.declare_fixture("fx-103", "zone-a", "tecq-rules", t(21, 19), ["cert-y"], t(5))
        self.svc.start_fixture("fx-102", t(6, 18))

    def test_receipts_idempotent_duplicate_and_out_of_order(self) -> None:
        # 乱序送达：不同编号先到先收，互不影响
        first = self.svc.receive_certificate_receipt(
            "cert-x", "tecq-rules", "tecq-table", "R-2", "hash-2", t(5, 12))
        second = self.svc.receive_certificate_receipt(
            "cert-x", "tecq-rules", "tecq-table", "R-1", "hash-1", t(5, 11))
        self.assertEqual(first["status"], "accepted")
        self.assertEqual(second["status"], "accepted")
        # 重复送达：同编号同内容 → 幂等重复
        dup = self.svc.receive_certificate_receipt(
            "cert-x", "tecq-rules", "tecq-table", "R-1", "hash-1", t(5, 18))
        self.assertEqual(dup["status"], "duplicate")
        view = self.svc.get_certificate("cert-x")
        self.assertEqual(view["status"], "active")
        r1 = next(r for r in view["receipts"] if r["receipt_id"] == "R-1")
        self.assertEqual(r1["duplicates"], 1)

    def test_same_id_different_content_enters_dispute(self) -> None:
        self.svc.receive_certificate_receipt(
            "cert-y", "tecq-rules", "tecq-net", "R-9", "hash-original", t(5, 12))
        result = self.svc.receive_certificate_receipt(
            "cert-y", "tecq-rules", "tecq-net", "R-9", "hash-tampered", t(5, 20))
        self.assertEqual(result["status"], "disputed")
        view = self.svc.get_certificate("cert-y")
        self.assertEqual(view["status"], "disputed")
        r9 = next(r for r in view["receipts"] if r["receipt_id"] == "R-9")
        self.assertEqual(r9["status"], "disputed")
        self.assertEqual(r9["content_hash"], "hash-original", "争议时保留先到内容，不覆盖")

    def test_revocation_freezes_only_referencing_scheduled_fixtures(self) -> None:
        self.svc.receive_certificate_receipt(
            "cert-x", "tecq-rules", "tecq-table", "R-1", "hash-1", t(5))
        result = self.svc.revoke_certificate("cert-x", "台面回弹系数抽检不合格", t(13))
        # 只有 fx-101：实际引用 cert-x、未开始、尚未冻结
        self.assertEqual(result["frozen_fixture_ids"], ["fx-101"])
        fixtures = {f["fixture_id"]: f for f in self.svc.list_fixtures()}
        self.assertTrue(fixtures["fx-101"]["frozen"])
        self.assertFalse(fixtures["fx-102"]["frozen"], "已开始场次不冻结，既有判罚不追改")
        self.assertFalse(fixtures["fx-103"]["frozen"], "未引用 cert-x 的场次不冻结")
        self.assertEqual(fixtures["fx-101"]["frozen_reasons"][0]["cert_id"], "cert-x")
        view = self.svc.get_certificate("cert-x")
        self.assertEqual(view["frozen_fixtures"], ["fx-101"])
        self.assertEqual(set(view["referencing_fixtures"]), {"fx-101", "fx-102"})

        # 已冻结的场次不能开始
        with self.assertRaises(DomainError):
            self.svc.start_fixture("fx-101", t(14))

        # 撤销幂等：再次撤销不会重复冻结
        again = self.svc.revoke_certificate("cert-x", "台面回弹系数抽检不合格", t(14))
        self.assertEqual(again["frozen_fixture_ids"], [])

        # 解除冻结后可正常开始
        self.svc.unfreeze_fixture("fx-101", "cert-x", t(15))
        self.assertFalse(next(f for f in self.svc.list_fixtures() if f["fixture_id"] == "fx-101")["frozen"])
        self.svc.start_fixture("fx-101", t(16))

    def test_revoke_unknown_certificate_rejected(self) -> None:
        with self.assertRaises(DomainError):
            self.svc.revoke_certificate("cert-nope", "不存在", t(13))


class PersistenceTest(unittest.TestCase):
    def test_pending_revision_survives_process_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "rule_cert.db"
            store = EventStore(db)
            svc = build_base(store)
            # 仅完成两方签署，修订处于待审状态
            svc.sign_revision("tecq-rules", "technical", "tech-wang", t(3, 9))
            svc.sign_revision("tecq-rules", "medical_safety", "med-chen", t(3, 10))
            pending_before = svc.get_package("tecq-rules")["pending_revision"]
            self.assertIsNotNone(pending_before)
            store.close()

            # 模拟进程重启：重新打开同一个 SQLite 文件
            store2 = EventStore(db)
            svc2 = RuleCertService(store2)
            pending_after = svc2.get_package("tecq-rules")["pending_revision"]
            self.assertIsNotNone(pending_after, "重启后待审修订不能丢失")
            self.assertEqual(pending_after["number"], pending_before["number"])
            self.assertEqual(pending_after["signatures"]["technical"], "tech-wang")
            self.assertEqual(set(pending_after["signatures"]), {"technical", "medical_safety"})
            # 第三方签署后发布成功
            result = svc2.sign_revision("tecq-rules", "competition_operations", "ops-zhao", t(3, 12))
            self.assertEqual(result["status"], "published")
            store2.close()

    def test_failed_batch_leaves_no_partial_events(self) -> None:
        """事务原子性：批量追加中途失败，前面的事件也不可见。"""
        store = EventStore(":memory:")
        svc = build_base(store)
        before = len(store.load_events())
        good = {
            "event_id": "receipt:R-7777:v1:CERTIFICATE_RECEIVED",
            "event_type": "CERTIFICATE_RECEIVED",
            "aggregate_type": "equipment_certificate",
            "aggregate_id": "cert-batch",
            "occurred_at": t(8),
            "version": 1,
            "summary": "批量中的正常事件",
            "data": {"receipt_id": "R-7777", "content_hash": "h1"},
        }
        bad = dict(good)
        bad["summary"] = "同编号被篡改的异内容"
        with self.assertRaises(EventConflict):
            store.append_many([good, bad])
        self.assertEqual(len(store.load_events()), before, "失败事务必须整体回滚")
        # 失败后存储仍可正常写入
        store.append(good)
        self.assertEqual(len(store.load_events()), before + 1)


if __name__ == "__main__":
    unittest.main()
