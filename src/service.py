"""规则认证库命令服务。

所有命令都遵循同一条临界区：在事件存储的写锁内重新折叠状态、判定
业务规则、在同一事务追加事件。因此：

- 并发签署被串行化，版本号不会互相覆盖；
- 签署人若等于修订提交人会被拒绝，不能复核自己的修订；
- 撤销证书与冻结场次在同一事务内完成，影响范围只含实际引用该证书
  且尚未开始、未被冻结的场次；
- 命令一旦成功返回，事件已落盘，进程重启后待审修订仍在。
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from .state import ROLES, Registry, fold, package_at
from .store import EventStore, VersionConflict

# 三个必须各自签署的职能方（见 state.ROLES）：
# technical / medical_safety / competition_operations


class DomainError(Exception):
    """请求不满足业务规则（4xx 语义）。"""


class ConcurrentUpdate(DomainError):
    """并发命令竞争同一聚合版本，调用方应重新读取后重试。"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RuleCertService:
    def __init__(self, store: EventStore) -> None:
        self.store = store

    # ------------------------------------------------------------------ 基础

    def _reload(self) -> Registry:
        return fold(self.store.load_events())

    def _next_version(self, reg: Registry, aggregate_type: str, aggregate_id: str) -> int:
        counts = getattr(reg, "_counts", {})
        return int(counts.get(f"{aggregate_type}:{aggregate_id}", 0)) + 1

    def _event(self, reg: Registry, event_type: str, aggregate_type: str, aggregate_id: str,
               at: str, summary: str, data: dict[str, Any]) -> dict:
        # 在同一命令内连续分配版本：折叠计数随分配推进，避免一个事务里
        # 多个同聚合事件（签署+替代+发布、撤销+多场次冻结）争用同版本号。
        counts = getattr(reg, "_counts", {})
        key = f"{aggregate_type}:{aggregate_id}"
        version = int(counts.get(key, 0)) + 1
        counts[key] = version
        return {
            "event_id": f"{aggregate_type}:{aggregate_id}:v{version}:{event_type}",
            "event_type": event_type,
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "occurred_at": at,
            "version": version,
            "summary": summary,
            "data": data,
        }

    def _commit(self, events: list[dict]) -> list[dict]:
        try:
            return self.store.append_many(events)
        except VersionConflict as exc:
            raise ConcurrentUpdate(str(exc)) from exc

    def register_sport(self, code: str, name: str, at: str | None = None) -> dict:
        at = at or _now()
        with self.store.lock:
            reg = self._reload()
            if code in reg.sports:
                raise DomainError(f"项目已登记：{code}")
            event = self._event(reg, "SPORT_REGISTERED", "sport", code, at,
                                f"登记项目 {name}", {"code": code, "name": name})
            self._commit([event])
            return event

    def define_stage(self, stage_id: str, name: str, at: str | None = None) -> dict:
        at = at or _now()
        with self.store.lock:
            reg = self._reload()
            if stage_id in reg.stages:
                raise DomainError(f"竞赛阶段已定义：{stage_id}")
            event = self._event(reg, "COMPETITION_STAGE_DEFINED", "competition_stage", stage_id,
                                at, f"定义竞赛阶段 {name}", {"stage_id": stage_id, "name": name})
            self._commit([event])
            return event

    def grant_judge_level(self, judge_id: str, sport_code: str, level: str,
                          at: str | None = None) -> dict:
        at = at or _now()
        with self.store.lock:
            reg = self._reload()
            version = self._next_version(reg, "judge", judge_id)
            event = {
                "event_id": f"judge:{judge_id}:v{version}:JUDGE_LEVEL_GRANTED",
                "event_type": "JUDGE_LEVEL_GRANTED",
                "aggregate_type": "judge",
                "aggregate_id": judge_id,
                "occurred_at": at,
                "version": version,
                "summary": f"裁判 {judge_id} 获得 {sport_code} {level} 等级",
                "data": {"judge_id": judge_id, "sport_code": sport_code, "level": level},
            }
            self._commit([event])
            return event

    def add_interpretation_case(self, case_id: str, package_id: str, clause_ref: str,
                                ruling: str, at: str | None = None) -> dict:
        at = at or _now()
        with self.store.lock:
            reg = self._reload()
            if package_id not in reg.packages:
                raise DomainError(f"规则包不存在：{package_id}")
            version = self._next_version(reg, "interpretation_case", case_id)
            event = {
                "event_id": f"interpretation_case:{case_id}:v{version}:INTERPRETATION_CASED",
                "event_type": "INTERPRETATION_CASED",
                "aggregate_type": "interpretation_case",
                "aggregate_id": case_id,
                "occurred_at": at,
                "version": version,
                "summary": f"记录解释案例 {case_id}",
                "data": {"case_id": case_id, "package_id": package_id,
                         "clause_ref": clause_ref, "ruling": ruling},
            }
            self._commit([event])
            return event

    def add_local_supplement(self, supplement_id: str, package_id: str, scope: str,
                             text: str, effective_from: str | None = None,
                             at: str | None = None) -> dict:
        at = at or _now()
        with self.store.lock:
            reg = self._reload()
            if package_id not in reg.packages:
                raise DomainError(f"规则包不存在：{package_id}")
            version = self._next_version(reg, "local_supplement", supplement_id)
            event = {
                "event_id": f"local_supplement:{supplement_id}:v{version}:LOCAL_SUPPLEMENT_ADDED",
                "event_type": "LOCAL_SUPPLEMENT_ADDED",
                "aggregate_type": "local_supplement",
                "aggregate_id": supplement_id,
                "occurred_at": at,
                "version": version,
                "summary": f"追加本地补充规定 {supplement_id}",
                "data": {"supplement_id": supplement_id, "package_id": package_id,
                         "scope": scope, "text": text,
                         "effective_from": effective_from or at},
            }
            self._commit([event])
            return event

    # ------------------------------------------------------------------ 规则包

    def draft_package(self, package_id: str, sport_code: str, stage_id: str, title: str,
                      at: str | None = None) -> dict:
        at = at or _now()
        with self.store.lock:
            reg = self._reload()
            if sport_code not in reg.sports:
                raise DomainError(f"项目未登记：{sport_code}")
            if stage_id not in reg.stages:
                raise DomainError(f"竞赛阶段未定义：{stage_id}")
            if package_id in reg.packages:
                raise DomainError(f"规则包已存在：{package_id}")
            event = self._event(
                reg, "RULE_PACKAGE_DRAFTED", "rule_package", package_id, at,
                f"起草规则包 {title}",
                {"sport_code": sport_code, "stage_id": stage_id, "title": title},
            )
            self._commit([event])
            return event

    def submit_revision(self, package_id: str, submitted_by: str, title: str, summary: str,
                        contents: dict[str, Any], *, qualification_changed: bool = False,
                        scoring_changed: bool = False, safety_changed: bool = False,
                        at: str | None = None) -> dict:
        """提交修订。首版为 initial；改变资格/计分/安全边界的后续修订
        必须形成 new_version，并自动列出受影响的尚未开始场次。"""
        at = at or _now()
        with self.store.lock:
            reg = self._reload()
            pkg = reg.packages.get(package_id)
            if pkg is None:
                raise DomainError(f"规则包不存在：{package_id}")
            pending = pkg.pending_revision()
            if pending is not None:
                raise DomainError(f"已有待审修订 r{pending.number}，不能重复提交")

            boundary = any((qualification_changed, scoring_changed, safety_changed))
            has_published = pkg.current_revision is not None
            kind = "initial" if not has_published else "new_version"
            if has_published and not boundary:
                raise DomainError(
                    "规则包锁定后仅允许非破坏性澄清（走 append_clarification）；"
                    "改变资格、计分或安全边界必须标注边界变化以形成新版本"
                )
            revision_no = (max(pkg.revisions, default=0) + 1)
            affected = self._affected_fixtures(reg, package_id) if boundary else []

            event = self._event(
                reg, "RULE_SUBMITTED", "rule_package", package_id, at,
                f"{submitted_by} 提交 {package_id} r{revision_no}（{kind}）",
                {
                    "revision": revision_no,
                    "kind": kind,
                    "title": title,
                    "summary": summary,
                    "contents": contents,
                    "submitted_by": submitted_by,
                    "qualification_changed": qualification_changed,
                    "scoring_changed": scoring_changed,
                    "safety_changed": safety_changed,
                    "impact_fixture_ids": affected,
                },
            )
            self._commit([event])
            return {"event": event, "revision": revision_no, "impact_fixture_ids": affected}

    @staticmethod
    def _affected_fixtures(reg: Registry, package_id: str) -> list[str]:
        return sorted(
            fx.fixture_id
            for fx in reg.fixtures.values()
            if fx.package_id == package_id and fx.status == "scheduled" and not fx.frozen
        )

    def sign_revision(self, package_id: str, role: str, signer: str,
                      at: str | None = None) -> dict:
        """某一职能方签署待审修订；三方签齐即发布。

        - 签署人不得是该修订的提交人（不能复核自己提交的修订）；
        - 每个职能方只能签署一次；
        - 第三次签署在同一事务内连带着发布事件，原子生效。
        """
        at = at or _now()
        if role not in ROLES:
            raise DomainError(f"未知签署职能方：{role}（应为 {', '.join(ROLES)}）")
        with self.store.lock:
            reg = self._reload()
            pkg = reg.packages.get(package_id)
            if pkg is None:
                raise DomainError(f"规则包不存在：{package_id}")
            rev = pkg.pending_revision()
            if rev is None:
                raise DomainError("没有待签署的修订")
            if signer == rev.submitted_by:
                raise DomainError(f"{signer} 是修订提交人，不能复核自己提交的修订")
            existing = rev.signatures.get(role)
            if existing is not None:
                if existing == signer:
                    return {"status": "already_signed", "role": role, "signer": signer}
                raise DomainError(f"职能方 {role} 已由 {existing} 签署")

            events: list[dict] = []
            events.append(self._event(
                reg, "REVIEW_SIGNED", "rule_package", package_id, at,
                f"{role} 由 {signer} 签署 {package_id} r{rev.number}",
                {"revision": rev.number, "role": role, "signer": signer},
            ))
            signatures = dict(rev.signatures)
            signatures[role] = signer
            published = False
            if set(signatures) == set(ROLES):
                if pkg.current_revision is not None:
                    events.append(self._event(
                        reg, "PACKAGE_VERSION_SUPERSEDED", "rule_package", package_id, at,
                        f"r{pkg.current_revision} 被 r{rev.number} 替代",
                        {"revision": pkg.current_revision},
                    ))
                events.append(self._event(
                    reg, "PACKAGE_PUBLISHED", "rule_package", package_id, at,
                    f"三方签署完成，发布 {package_id} r{rev.number}",
                    {"revision": rev.number, "signatures": signatures},
                ))
                published = True
            self._commit(events)
            return {
                "status": "published" if published else "signed",
                "revision": rev.number,
                "role": role,
                "awaiting_roles": sorted(set(ROLES) - set(signatures)),
                "events": events,
            }

    # ------------------------------------------------------------------ 锁定与澄清

    def _build_snapshot(self, reg: Registry, package_id: str, revision_no: int, at: str) -> dict:
        pkg = reg.packages[package_id]
        rev = pkg.revisions[revision_no]
        return {
            "package_id": package_id,
            "sport_code": pkg.sport_code,
            "stage_id": pkg.stage_id,
            "revision": revision_no,
            "title": rev.title,
            "clauses": rev.contents,
            "boundary": {
                "qualification_changed": rev.qualification_changed,
                "scoring_changed": rev.scoring_changed,
                "safety_changed": rev.safety_changed,
            },
            "equipment_certificates": sorted(
                cid for cid, cert in reg.certificates.items()
                if cert.package_id == package_id and cert.status == "active"
            ),
            "judge_levels": [
                {"judge_id": jid, "level": j["level"]}
                for jid, j in sorted(reg.judges.items())
                if j["sport_code"] == pkg.sport_code
            ],
            "interpretation_cases": sorted(
                cid for cid, c in reg.cases.items() if c.get("package_id") == package_id
            ),
            "local_supplements": sorted(
                sid for sid, s in reg.supplements.items()
                if s.get("package_id") == package_id and s.get("effective_from", at) <= at
            ),
            "snapshot_at": at,
        }

    def lock_venue(self, venue_id: str, name: str, package_id: str,
                   at: str | None = None) -> dict:
        """赛区锁定当前已发布修订；快照此后不可变，旧赛程永远可还原。"""
        at = at or _now()
        with self.store.lock:
            reg = self._reload()
            pkg = reg.packages.get(package_id)
            if pkg is None:
                raise DomainError(f"规则包不存在：{package_id}")
            revision_no = pkg.current_revision
            if revision_no is None:
                raise DomainError("规则包尚无三方签署完成的已发布修订，不能锁定")
            venue = reg.venues.get(venue_id)
            if venue is not None:
                current = venue.lock_at()
                if current is not None and current["revision"] == revision_no:
                    return {"status": "already_locked", "venue_id": venue_id,
                            "revision": revision_no, "snapshot": current["snapshot"]}
            snapshot = self._build_snapshot(reg, package_id, revision_no, at)
            version = self._next_version(reg, "venue_adoption", venue_id)
            event = {
                "event_id": f"venue_adoption:{venue_id}:v{version}:VENUE_LOCKED",
                "event_type": "VENUE_LOCKED",
                "aggregate_type": "venue_adoption",
                "aggregate_id": venue_id,
                "occurred_at": at,
                "version": version,
                "summary": f"赛区 {name} 锁定 {package_id} r{revision_no}",
                "data": {"venue_id": venue_id, "name": name,
                         "package_id": package_id, "revision": revision_no,
                         "snapshot": snapshot},
            }
            self._commit([event])
            return {"status": "locked", "event": event, "snapshot": snapshot}

    def append_clarification(self, package_id: str, venue_id: str, title: str, text: str,
                             references: list[str], author: str,
                             at: str | None = None) -> dict:
        """对已锁定规则包追加非破坏性澄清。

        澄清只能引用既有文件/回执，不得改变资格、计分或安全边界，因此
        不产生新版本、不改动锁定快照。
        """
        at = at or _now()
        with self.store.lock:
            reg = self._reload()
            venue = reg.venues.get(venue_id)
            if venue is None or venue.lock_at() is None:
                raise DomainError(f"赛区尚未锁定规则包：{venue_id}")
            lock = venue.lock_at()
            assert lock is not None
            if lock["package_id"] != package_id:
                raise DomainError(f"赛区 {venue_id} 锁定的是 {lock['package_id']}，不是 {package_id}")
            clarification_id = f"clar-{len(reg.packages[package_id].clarifications) + 1:03d}-{venue_id}"
            event = self._event(
                reg, "CLARIFICATION_APPENDED", "rule_package", package_id, at,
                f"向 {venue_id} 追加非破坏性澄清《{title}》",
                {"clarification_id": clarification_id, "venue_id": venue_id,
                 "locked_revision": lock["revision"], "title": title, "text": text,
                 "references": list(references), "author": author},
            )
            self._commit([event])
            return {"event": event, "clarification_id": clarification_id}

    # ------------------------------------------------------------------ 器材回执与撤销

    def receive_certificate_receipt(self, cert_id: str, package_id: str, equipment_code: str,
                                    receipt_id: str, content_hash: str,
                                    at: str | None = None) -> dict:
        """登记离线送达的器材认证回执。

        回执可能乱序或重复：同编号同内容按重复幂等处理；同编号异内容
        进入争议，证书不得继续使用。
        """
        at = at or _now()
        with self.store.lock:
            reg = self._reload()
            cert = reg.certificates.get(cert_id)
            if cert is not None:
                if cert.package_id and cert.package_id != package_id:
                    raise DomainError(f"证书 {cert_id} 归属规则包 {cert.package_id}，与 {package_id} 不符")
                if cert.status == "revoked":
                    raise DomainError(f"证书 {cert_id} 已撤销，回执不再受理")
            version = self._next_version(reg, "equipment_certificate", cert_id)
            base = {
                "aggregate_type": "equipment_certificate",
                "aggregate_id": cert_id,
                "occurred_at": at,
            }
            if cert is None or not cert.receipts:
                event = {
                    **base,
                    "event_id": f"equipment_certificate:{cert_id}:v{version}:CERTIFICATE_RECEIVED",
                    "event_type": "CERTIFICATE_RECEIVED",
                    "version": version,
                    "summary": f"首次收到器材证书 {cert_id}（{equipment_code}）回执 {receipt_id}",
                    "data": {"package_id": package_id, "equipment_code": equipment_code,
                             "receipt_id": receipt_id, "content_hash": content_hash},
                }
                self._commit([event])
                return {"status": "accepted", "event": event}

            prior = cert.receipts.get(receipt_id)
            if prior is None:
                event = {
                    **base,
                    "event_id": f"equipment_certificate:{cert_id}:v{version}:CERTIFICATE_RECEIVED",
                    "event_type": "CERTIFICATE_RECEIVED",
                    "version": version,
                    "summary": f"器材证书 {cert_id} 补收回执 {receipt_id}",
                    "data": {"package_id": package_id, "equipment_code": equipment_code,
                             "receipt_id": receipt_id, "content_hash": content_hash},
                }
                self._commit([event])
                return {"status": "accepted", "event": event}
            if prior.content_hash == content_hash:
                event = {
                    **base,
                    "event_id": f"equipment_certificate:{cert_id}:v{version}:CERTIFICATE_DUPLICATE_RECEIVED",
                    "event_type": "CERTIFICATE_DUPLICATE_RECEIVED",
                    "version": version,
                    "summary": f"器材证书 {cert_id} 回执 {receipt_id} 重复送达（内容一致）",
                    "data": {"receipt_id": receipt_id, "content_hash": content_hash},
                }
                self._commit([event])
                return {"status": "duplicate", "event": event, "duplicates": prior.duplicates + 1}

            event = {
                **base,
                "event_id": f"equipment_certificate:{cert_id}:v{version}:RECEIPT_DISPUTED",
                "event_type": "RECEIPT_DISPUTED",
                "version": version,
                "summary": f"器材证书 {cert_id} 回执 {receipt_id} 同编号异内容，进入争议",
                "data": {"receipt_id": receipt_id, "prior_hash": prior.content_hash,
                         "conflict_hash": content_hash},
            }
            self._commit([event])
            return {"status": "disputed", "event": event}

    def revoke_certificate(self, cert_id: str, reason: str, at: str | None = None) -> dict:
        """撤销器材证书，并只冻结实际引用它的未开始场次。"""
        at = at or _now()
        with self.store.lock:
            reg = self._reload()
            cert = reg.certificates.get(cert_id)
            if cert is None:
                raise DomainError(f"证书不存在：{cert_id}")
            events: list[dict] = []
            if cert.status != "revoked":
                events.append(self._event(
                    reg, "CERTIFICATE_REVOKED", "equipment_certificate", cert_id, at,
                    f"撤销器材证书 {cert_id}：{reason}", {"reason": reason},
                ))
            frozen_ids: list[str] = []
            for fx in sorted(reg.fixtures.values(), key=lambda f: f.fixture_id):
                if fx.affected_by_revoke(cert_id):
                    version = self._next_version(reg, "fixture", fx.fixture_id)
                    events.append({
                        "event_id": f"fixture:{fx.fixture_id}:v{version}:FIXTURE_FROZEN",
                        "event_type": "FIXTURE_FROZEN",
                        "aggregate_type": "fixture",
                        "aggregate_id": fx.fixture_id,
                        "occurred_at": at,
                        "version": version,
                        "summary": f"证书 {cert_id} 撤销，冻结未开始场次 {fx.fixture_id}",
                        "data": {"cert_id": cert_id, "reason": reason},
                    })
                    frozen_ids.append(fx.fixture_id)
            if events:
                self._commit(events)
            return {"status": "revoked", "cert_id": cert_id,
                    "frozen_fixture_ids": frozen_ids, "events": events}

    def unfreeze_fixture(self, fixture_id: str, cert_id: str, at: str | None = None) -> dict:
        at = at or _now()
        with self.store.lock:
            reg = self._reload()
            fx = reg.fixtures.get(fixture_id)
            if fx is None:
                raise DomainError(f"场次不存在：{fixture_id}")
            if not fx.frozen:
                return {"status": "not_frozen", "fixture_id": fixture_id}
            event = self._event(
                reg, "FIXTURE_UNFROZEN", "fixture", fixture_id, at,
                f"解除场次 {fixture_id} 因 {cert_id} 的冻结", {"cert_id": cert_id},
            )
            self._commit([event])
            return {"status": "unfrozen", "event": event}

    # ------------------------------------------------------------------ 场次

    def declare_fixture(self, fixture_id: str, venue_id: str, package_id: str,
                        scheduled_start: str, cert_refs: list[str],
                        at: str | None = None) -> dict:
        at = at or _now()
        with self.store.lock:
            reg = self._reload()
            if fixture_id in reg.fixtures:
                raise DomainError(f"场次已存在：{fixture_id}")
            if package_id not in reg.packages:
                raise DomainError(f"规则包不存在：{package_id}")
            version = self._next_version(reg, "fixture", fixture_id)
            event = {
                "event_id": f"fixture:{fixture_id}:v{version}:FIXTURE_DECLARED",
                "event_type": "FIXTURE_DECLARED",
                "aggregate_type": "fixture",
                "aggregate_id": fixture_id,
                "occurred_at": at,
                "version": version,
                "summary": f"登记场次 {fixture_id}（{package_id}，{scheduled_start}）",
                "data": {"fixture_id": fixture_id, "venue_id": venue_id,
                         "package_id": package_id, "scheduled_start": scheduled_start,
                         "cert_refs": list(cert_refs)},
            }
            self._commit([event])
            return event

    def start_fixture(self, fixture_id: str, at: str | None = None) -> dict:
        at = at or _now()
        with self.store.lock:
            reg = self._reload()
            fx = reg.fixtures.get(fixture_id)
            if fx is None:
                raise DomainError(f"场次不存在：{fixture_id}")
            if fx.status != "scheduled":
                return {"status": fx.status, "fixture_id": fixture_id}
            if fx.frozen:
                raise DomainError(f"场次 {fixture_id} 处于冻结状态，不能开始")
            event = self._event(
                reg, "FIXTURE_STARTED", "fixture", fixture_id, at,
                f"场次 {fixture_id} 已开始，既有判罚不再受后续修订影响", {},
            )
            self._commit([event])
            return {"status": "started", "event": event}

    # ------------------------------------------------------------------ 查询

    def _revision_view(self, rev) -> dict:
        return {
            "number": rev.number,
            "kind": rev.kind,
            "title": rev.title,
            "summary": rev.summary,
            "contents": rev.contents,
            "submitted_by": rev.submitted_by,
            "submitted_at": rev.submitted_at,
            "status": rev.status,
            "published_at": rev.published_at,
            "superseded_at": rev.superseded_at,
            "qualification_changed": rev.qualification_changed,
            "scoring_changed": rev.scoring_changed,
            "safety_changed": rev.safety_changed,
            "impact_fixture_ids": rev.impact_fixture_ids,
            "signatures": dict(rev.signatures),
        }

    def get_package(self, package_id: str, as_of: str | None = None) -> dict:
        reg = self._reload()
        pkg = reg.packages.get(package_id)
        if pkg is None:
            raise DomainError(f"规则包不存在：{package_id}")
        applicability = [
            {
                "revision": rev.number,
                "kind": rev.kind,
                "effective_from": rev.published_at,
                "effective_until": rev.superseded_at,
                "qualification_changed": rev.qualification_changed,
                "scoring_changed": rev.scoring_changed,
                "safety_changed": rev.safety_changed,
            }
            for rev in sorted(pkg.revisions.values(), key=lambda r: r.number)
            if rev.published_at is not None
        ]
        if as_of is not None:
            rev = package_at(reg, package_id, as_of)
            current = self._revision_view(rev) if rev else None
            clarifications = [
                {"clarification_id": c.clarification_id, "venue_id": c.venue_id,
                 "title": c.title, "text": c.text, "references": c.references,
                 "author": c.author, "appended_at": c.appended_at}
                for c in pkg.clarifications
                if c.appended_at <= as_of
            ]
        else:
            current = self._revision_view(pkg.revisions[pkg.current_revision]) if pkg.current_revision else None
            pending = pkg.pending_revision()
            clarifications = [asdict(c) for c in pkg.clarifications]
        pending = pkg.pending_revision()
        return {
            "package_id": package_id,
            "sport_code": pkg.sport_code,
            "stage_id": pkg.stage_id,
            "title": pkg.title,
            "as_of": as_of,
            "current_revision": current,
            "pending_revision": self._revision_view(pending) if pending else None,
            "applicability_periods": applicability,
            "clarifications": clarifications,
            "revisions": [self._revision_view(r) for r in sorted(pkg.revisions.values(), key=lambda r: r.number)],
        }

    def get_venue(self, venue_id: str, as_of: str | None = None) -> dict:
        reg = self._reload()
        venue = reg.venues.get(venue_id)
        if venue is None or not venue.locks:
            raise DomainError(f"赛区从未锁定规则包：{venue_id}")
        lock = venue.lock_at(as_of)
        if lock is None:
            raise DomainError(f"{as_of} 时点赛区 {venue_id} 尚未锁定任何规则包")
        pkg = reg.packages[lock["package_id"]]
        clarifications = [
            {"clarification_id": c.clarification_id, "title": c.title, "text": c.text,
             "references": c.references, "author": c.author, "appended_at": c.appended_at}
            for c in pkg.clarifications
            if c.venue_id == venue_id and c.locked_revision == lock["revision"]
            and (as_of is None or c.appended_at <= as_of)
        ]
        return {
            "venue_id": venue_id,
            "name": venue.name,
            "as_of": as_of,
            "lock": lock,
            "clarifications": clarifications,
        }

    def get_certificate(self, cert_id: str) -> dict:
        reg = self._reload()
        cert = reg.certificates.get(cert_id)
        if cert is None:
            raise DomainError(f"证书不存在：{cert_id}")
        referencing = sorted(
            fx.fixture_id for fx in reg.fixtures.values() if cert_id in fx.cert_refs
        )
        frozen_due_to = sorted(
            fx.fixture_id for fx in reg.fixtures.values()
            if fx.frozen and any(r["cert_id"] == cert_id for r in fx.frozen_reasons)
        )
        return {
            "cert_id": cert_id,
            "package_id": cert.package_id,
            "equipment_code": cert.equipment_code,
            "status": cert.status,
            "revoked_at": cert.revoked_at,
            "revoke_reason": cert.revoke_reason,
            "receipts": [
                {"receipt_id": rid, "content_hash": r.content_hash,
                 "status": r.status, "duplicates": r.duplicates}
                for rid, r in sorted(cert.receipts.items())
            ],
            "referencing_fixtures": referencing,
            "frozen_fixtures": frozen_due_to,
        }

    def list_fixtures(self) -> list[dict]:
        reg = self._reload()
        return [
            {
                "fixture_id": fx.fixture_id,
                "venue_id": fx.venue_id,
                "package_id": fx.package_id,
                "scheduled_start": fx.scheduled_start,
                "status": fx.status,
                "frozen": fx.frozen,
                "cert_refs": list(fx.cert_refs),
                "frozen_reasons": list(fx.frozen_reasons),
            }
            for fx in sorted(reg.fixtures.values(), key=lambda f: f.fixture_id)
        ]
