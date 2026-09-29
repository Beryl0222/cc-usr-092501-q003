"""规则认证库领域内核。

所有状态都由事件流 fold 得到，命令的唯一副作用是向事件存储追加事件：

* 规则包（rule_package）草稿 → 三方（技术 / 医疗安全 / 竞赛运营）各自签署
  初始修订后才能 ``PACKAGE_PUBLISHED`` 生效；
* 签署人不能复核自己提交的修订，同一角色重复签署或越权角色一律拒绝；
* 赛区 ``PACKAGE_LOCKED`` 后：非破坏性澄清只能 ``CLARIFICATION_APPENDED``
  追加引用；改变资格 / 计分 / 安全边界的勘误形成新版本
  （``NEW_VERSION_DEMANDED`` + ``IMPACT_LISTED``），只列出尚未开始的场次，
  已开始或已结束的场次继续引用旧版本，既有判罚不可追改；
* 离线认证回执同编号同内容幂等丢弃，同编号异内容开启 ``DISPUTE_OPENED``；
* 器材证书 ``CERTIFICATE_REVOKED`` 只冻结实际引用它的场地安排。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .events import (
    SIGNOFF_ROLES, SESSION_REPINNED, Event, make_event, now_iso,
)
from .store import APPENDED, CONFLICT, DUPLICATE, EventStore

CLARIFICATION = "clarification"   # 非破坏性澄清
BOUNDARY = "boundary"             # 改变资格/计分/安全边界
INITIAL = "initial"               # 规则包首版内容
CHANGE_CLASSES = {CLARIFICATION, BOUNDARY, INITIAL}

SESSION_SCHEDULED = "scheduled"   # 尚未开始
SESSION_STARTED = "started"
SESSION_FINISHED = "finished"

ARRANGEMENT_ACTIVE = "active"
ARRANGEMENT_FROZEN = "frozen"

CERT_ISSUED = "issued"
CERT_REVOKED = "revoked"


class DomainError(Exception):
    """违反领域规则；HTTP 层映射为 4xx，重放层记录为拒绝。"""


@dataclass
class Revision:
    revision_id: str
    package_id: str
    submitted_by: str
    submitter_role: str
    change_class: str
    summary: str
    content: dict[str, Any]
    signatures: dict[str, str] = field(default_factory=dict)  # role -> signer
    status: str = "pending"  # pending / approved
    created_seq: int = 0


@dataclass
class Package:
    package_id: str
    sport: str
    stage: str
    version_no: int
    parent_id: str | None = None
    clauses: dict[str, Any] = field(default_factory=dict)
    status: str = "draft"  # draft / published
    revisions: dict[str, Revision] = field(default_factory=dict)
    clarifications: list[dict[str, Any]] = field(default_factory=list)
    locked_by: dict[str, str] = field(default_factory=dict)  # zone -> 锁定时间
    successor_id: str | None = None
    published_at: str | None = None
    created_seq: int = 0


@dataclass
class Certificate:
    cert_id: str
    detail: dict[str, Any]
    status: str = CERT_ISSUED
    dispute_ids: list[str] = field(default_factory=list)
    issued_at: str | None = None
    revoked_at: str | None = None


@dataclass
class Arrangement:
    arrangement_id: str
    zone: str
    session_id: str
    cert_refs: list[str]
    status: str = ARRANGEMENT_ACTIVE
    frozen_for: list[str] = field(default_factory=list)  # 触发冻结的证书


@dataclass
class Session:
    session_id: str
    zone: str
    sport: str
    stage: str
    status: str = SESSION_SCHEDULED
    package_id: str | None = None
    scheduled_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None


@dataclass
class Dispute:
    dispute_id: str
    event_id: str
    variants: list[dict[str, Any]] = field(default_factory=list)
    status: str = "open"


@dataclass
class State:
    seq: int = 0
    sports: dict[str, dict[str, Any]] = field(default_factory=dict)
    packages: dict[str, Package] = field(default_factory=dict)
    revisions: dict[str, Revision] = field(default_factory=dict)
    certificates: dict[str, Certificate] = field(default_factory=dict)
    arrangements: dict[str, Arrangement] = field(default_factory=dict)
    sessions: dict[str, Session] = field(default_factory=dict)
    disputes: dict[str, Dispute] = field(default_factory=dict)
    event_ids: dict[str, str] = field(default_factory=dict)  # event_id -> hash


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def fold(events: list[Event], *, as_of_seq: int | None = None,
         as_of: str | None = None) -> State:
    """把事件流折叠为状态。``as_of_seq`` / ``as_of`` 支持时点还原。"""
    state = State()
    cutoff = _parse_ts(as_of)
    # 事件按 seq 顺序应用；seq 由存储在追加时赋予，fold 时按数组下标+1。
    for index, event in enumerate(events, start=1):
        if as_of_seq is not None and index > as_of_seq:
            break
        if cutoff is not None:
            if _parse_ts(event.occurred_at) > cutoff:
                continue
        state.seq = index
        state.event_ids[event.event_id] = event.hash
        p = event.payload
        t = event.event_type

        if t == "SPORT_REGISTERED":
            state.sports[p["sport"]] = {"sport": p["sport"], "name": p.get("name", p["sport"])}

        elif t == "PACKAGE_CREATED":
            state.packages[p["package_id"]] = Package(
                package_id=p["package_id"], sport=p["sport"], stage=p["stage"],
                version_no=int(p.get("version_no", 1)), parent_id=p.get("parent_id"),
                clauses=dict(p.get("clauses", {})), created_seq=index,
            )
            if p.get("parent_id") and p["parent_id"] in state.packages:
                state.packages[p["parent_id"]].successor_id = p["package_id"]

        elif t == "REVISION_SUBMITTED":
            rev = Revision(
                revision_id=p["revision_id"], package_id=p["package_id"],
                submitted_by=p["submitted_by"], submitter_role=p["submitter_role"],
                change_class=p["change_class"], summary=p.get("summary", ""),
                content=dict(p.get("content", {})), created_seq=index,
            )
            state.revisions[rev.revision_id] = rev
            pkg = state.packages.get(rev.package_id)
            if pkg is not None:
                pkg.revisions[rev.revision_id] = rev

        elif t == "REVIEW_SIGNED":
            rev = state.revisions.get(p["revision_id"])
            if rev is not None:
                rev.signatures[p["role"]] = p["signer"]

        elif t == "PACKAGE_PUBLISHED":
            pkg = state.packages.get(p["package_id"])
            if pkg is not None:
                pkg.status = "published"
                pkg.published_at = event.occurred_at
                rid = p.get("revision_id")
                if rid in pkg.revisions:
                    approved = pkg.revisions[rid]
                    approved.status = "approved"
                    # 生效以三方签署批准的修订内容为准：在创建时的条款
                    # （首版草稿或继任版本继承的旧条款）之上叠加修订快照。
                    merged = dict(pkg.clauses)
                    merged.update(approved.content)
                    pkg.clauses = merged

        elif t == "PACKAGE_LOCKED":
            pkg = state.packages.get(p["package_id"])
            if pkg is not None:
                pkg.locked_by[p["zone"]] = event.occurred_at

        elif t == "CLARIFICATION_APPENDED":
            pkg = state.packages.get(p["package_id"])
            if pkg is not None:
                pkg.clarifications.append({
                    "reference": p["reference"], "summary": p.get("summary", ""),
                    "submitted_by": p.get("submitted_by"), "at": event.occurred_at,
                })

        elif t == "IMPACT_LISTED":
            pass  # 影响清单保存在事件负载中，属于不可变审计事实

        elif t in ("CERTIFICATE_ISSUED", "CERTIFICATE_REVOKED"):
            cert = state.certificates.get(p["cert_id"])
            if cert is None:
                cert = Certificate(cert_id=p["cert_id"], detail=dict(p.get("detail", {})))
                state.certificates[p["cert_id"]] = cert
            if t == "CERTIFICATE_ISSUED":
                cert.status = CERT_ISSUED
                cert.issued_at = event.occurred_at
                cert.detail.update(p.get("detail", {}))
            else:
                cert.status = CERT_REVOKED
                cert.revoked_at = event.occurred_at

        elif t == "VENUE_ARRANGEMENT_REGISTERED":
            state.arrangements[p["arrangement_id"]] = Arrangement(
                arrangement_id=p["arrangement_id"], zone=p["zone"],
                session_id=p["session_id"], cert_refs=list(p.get("cert_refs", [])),
            )

        elif t == "VENUE_ARRANGEMENT_FROZEN":
            arr = state.arrangements.get(p["arrangement_id"])
            if arr is not None:
                arr.status = ARRANGEMENT_FROZEN
                if p["cert_id"] not in arr.frozen_for:
                    arr.frozen_for.append(p["cert_id"])

        elif t == "SESSION_SCHEDULED":
            state.sessions[p["session_id"]] = Session(
                session_id=p["session_id"], zone=p["zone"], sport=p["sport"],
                stage=p["stage"], package_id=p.get("package_id"),
                scheduled_at=event.occurred_at,
            )

        elif t == "SESSION_STARTED":
            sess = state.sessions.get(p["session_id"])
            if sess is not None:
                sess.status = SESSION_STARTED
                sess.started_at = event.occurred_at

        elif t == "SESSION_FINISHED":
            sess = state.sessions.get(p["session_id"])
            if sess is not None:
                sess.status = SESSION_FINISHED
                sess.finished_at = event.occurred_at

        elif t == "SESSION_REPINNED":
            sess = state.sessions.get(p["session_id"])
            if sess is not None:
                sess.package_id = p["package_id"]

        elif t == "DISPUTE_OPENED":
            state.disputes[p["dispute_id"]] = Dispute(
                dispute_id=p["dispute_id"], event_id=p["event_id"],
                variants=list(p.get("variants", [])),
            )
            cert = state.certificates.get(p.get("cert_id", ""))
            if cert is not None:
                cert.dispute_ids.append(p["dispute_id"])

        elif t == "DISPUTE_RESOLVED":
            dispute = state.disputes.get(p["dispute_id"])
            if dispute is not None:
                dispute.status = "resolved"

    return state


class RuleCertificationService:
    """命令服务：每个命令在同一把锁内完成“折叠 → 校验 → 追加”。"""

    def __init__(self, store: EventStore, *, clock: Any = None):
        self.store = store
        self._lock = threading.RLock()
        # 可注入时钟；确定性重放时按命令的 occurred_at 固定。
        self.clock = clock or now_iso

    # --- 内部工具 ---
    def _state(self) -> State:
        return fold(self.store.all_events())

    def _ts(self) -> str:
        return self.clock()

    def _append(self, event: Event) -> Event:
        result = self.store.append(event)
        if result == CONFLICT:
            raise DomainError(f"事件编号内容冲突：{event.event_id}")
        return event

    def _require_package(self, state: State, package_id: str) -> Package:
        pkg = state.packages.get(package_id)
        if pkg is None:
            raise DomainError(f"规则包不存在：{package_id}")
        return pkg

    def _lineage(self, state: State, package_id: str) -> set[str]:
        """该版本及其全部祖先（受新版本影响的旧包谱系）。"""
        lineage = {package_id}
        pkg = state.packages.get(package_id)
        while pkg is not None and pkg.parent_id:
            lineage.add(pkg.parent_id)
            pkg = state.packages.get(pkg.parent_id)
        return lineage

    def _not_started_sessions(self, state: State, pkg: Package) -> list[str]:
        """受边界勘误影响的“尚未开始”场次。

        判定：同项目同阶段、未开赛、当前引用该版本或其祖先版本。
        已开始/结束的场次不在清单中——它们继续引用开赛时固定的
        旧版本，既有判罚不可追改；没有引用该版本谱系的场次不受影响。
        """
        lineage = self._lineage(state, pkg.package_id)
        out = []
        for sess in state.sessions.values():
            if sess.sport != pkg.sport or sess.stage != pkg.stage:
                continue
            if sess.status != SESSION_SCHEDULED:
                continue
            if sess.package_id not in lineage:
                continue
            out.append(sess.session_id)
        return sorted(out)

    # --- 项目 / 规则包 ---
    def register_sport(self, sport: str, *, name: str = "") -> Event:
        with self._lock:
            state = self._state()
            if sport in state.sports:
                raise DomainError(f"项目已登记：{sport}")
            return self._append(make_event(
                "SPORT_REGISTERED", "sport_rulebook", sport,
                {"sport": sport, "name": name or sport},
                event_id=f"sport-{sport}", occurred_at=self._ts(), version=1,
                summary=f"登记项目 {sport}",
            ))

    def create_package(self, package_id: str, sport: str, stage: str, *,
                       clauses: dict[str, Any] | None = None,
                       version_no: int = 1, parent_id: str | None = None) -> Event:
        with self._lock:
            state = self._state()
            if package_id in state.packages:
                raise DomainError(f"规则包已存在：{package_id}")
            if sport not in state.sports:
                raise DomainError(f"项目未登记：{sport}")
            if parent_id is not None and parent_id not in state.packages:
                raise DomainError(f"父版本不存在：{parent_id}")
            payload = {"package_id": package_id, "sport": sport, "stage": stage,
                       "version_no": version_no, "clauses": clauses or {}}
            if parent_id:
                payload["parent_id"] = parent_id
            return self._append(make_event(
                "PACKAGE_CREATED", "rule_package", package_id, payload,
                event_id=f"pkg-create-{package_id}", occurred_at=self._ts(),
                version=version_no, summary=f"创建规则包 {package_id}",
            ))

    def submit_revision(self, package_id: str, revision_id: str, *,
                        submitted_by: str, submitter_role: str,
                        change_class: str, summary: str = "",
                        content: dict[str, Any] | None = None) -> list[Event]:
        """提交修订。

        草稿包上只能提交首版修订；已生效（尤其已被赛区锁定）的包上：
        澄清只追加引用，边界勘误自动派生新版本并列出受影响场次。
        """
        if change_class not in CHANGE_CLASSES:
            raise DomainError(f"未知修订类别：{change_class}")
        if submitter_role not in SIGNOFF_ROLES:
            raise DomainError(f"未知角色：{submitter_role}")
        with self._lock:
            state = self._state()
            pkg = self._require_package(state, package_id)
            if revision_id in state.revisions:
                raise DomainError(f"修订已存在：{revision_id}")
            events: list[Event] = []
            ts = self._ts()

            target = pkg
            if change_class == CLARIFICATION:
                # 非破坏性澄清：对已生效包追加引用，不形成新版本。
                if pkg.status != "published":
                    raise DomainError("规则包尚未生效，不能追加澄清")
                events.append(self._append(make_event(
                    "CLARIFICATION_APPENDED", "rule_package", package_id,
                    {"package_id": package_id,
                     "reference": (content or {}).get("reference", summary),
                     "summary": summary, "submitted_by": submitted_by},
                    event_id=f"clar-{revision_id}", occurred_at=ts,
                    version=len(pkg.clarifications) + 1,
                    summary=f"追加澄清引用：{summary}",
                )))
                return events

            if pkg.status == "published" and change_class == BOUNDARY:
                # 改变资格/计分/安全边界 → 必须形成新版本。
                if not pkg.locked_by:
                    # 未锁定但已生效同样不可原地改，保持生效内容不可变。
                    pass
                successor_id = self._successor_id(pkg.package_id, pkg.version_no, state)
                events.append(self._append(make_event(
                    "NEW_VERSION_DEMANDED", "rule_package", package_id,
                    {"package_id": package_id, "successor_id": successor_id,
                     "revision_id": revision_id, "reason": summary},
                    event_id=f"newver-{revision_id}", occurred_at=ts,
                    version=pkg.version_no + 1,
                    summary=f"边界勘误要求新版本：{summary}",
                )))
                inherited = dict(pkg.clauses)
                events.append(self._append(make_event(
                    "PACKAGE_CREATED", "rule_package", successor_id,
                    {"package_id": successor_id, "sport": pkg.sport,
                     "stage": pkg.stage, "version_no": pkg.version_no + 1,
                     "parent_id": package_id, "clauses": inherited},
                    event_id=f"pkg-create-{successor_id}", occurred_at=ts,
                    version=pkg.version_no + 1,
                    summary=f"创建继任版本 {successor_id}",
                )))
                affected = self._not_started_sessions(state, pkg)
                events.append(self._append(make_event(
                    "IMPACT_LISTED", "rule_package", successor_id,
                    {"package_id": package_id, "successor_id": successor_id,
                     "revision_id": revision_id, "affected_sessions": affected},
                    event_id=f"impact-{revision_id}", occurred_at=ts,
                    version=pkg.version_no + 1,
                    summary=f"受影响且尚未开始的场次：{', '.join(affected) or '无'}",
                )))
                self._require_package(self._state(), successor_id)
                target_package_id = successor_id
            else:
                if pkg.status == "published":
                    raise DomainError("已生效规则包的非澄清修订必须走新版本流程")
                if change_class == BOUNDARY and pkg.status == "draft":
                    # 草稿阶段的边界内容并入首版即可。
                    change_class = INITIAL
                target_package_id = package_id

            events.append(self._append(make_event(
                "REVISION_SUBMITTED", "rule_revision", revision_id,
                {"revision_id": revision_id, "package_id": target_package_id,
                 "submitted_by": submitted_by, "submitter_role": submitter_role,
                 "change_class": change_class, "summary": summary,
                 "content": content or {}},
                event_id=f"rev-{revision_id}", occurred_at=ts, version=1,
                summary=f"{submitted_by} 提交修订 {revision_id}",
            )))
            return events

    @staticmethod
    def _successor_id(package_id: str, version_no: int, state: State) -> str:
        base = package_id
        if f"-v{version_no}" in package_id:
            base = package_id.rsplit(f"-v{version_no}", 1)[0]
        candidate = f"{base}-v{version_no + 1}"
        suffix = 2
        while candidate in state.packages:
            candidate = f"{base}-v{version_no + 1}-{suffix}"
            suffix += 1
        return candidate

    def sign_revision(self, revision_id: str, *, signer: str, role: str) -> list[Event]:
        if role not in SIGNOFF_ROLES:
            raise DomainError(f"未知签署角色：{role}")
        with self._lock:
            state = self._state()
            rev = state.revisions.get(revision_id)
            if rev is None:
                raise DomainError(f"修订不存在：{revision_id}")
            if rev.status == "approved":
                raise DomainError("修订已完成签署")
            # 核心隔离：签署人不能复核自己提交的修订。
            if signer == rev.submitted_by:
                raise DomainError("签署人不能复核自己提交的修订")
            if role in rev.signatures:
                raise DomainError(f"角色 {role} 已由 {rev.signatures[role]} 签署")
            if signer in rev.signatures.values():
                raise DomainError(f"{signer} 已以其他角色签署，不得重复签署")
            pkg = self._require_package(state, rev.package_id)
            ts = self._ts()
            events = [self._append(make_event(
                "REVIEW_SIGNED", "rule_revision", revision_id,
                {"revision_id": revision_id, "package_id": pkg.package_id,
                 "signer": signer, "role": role},
                event_id=f"sign-{revision_id}-{role}", occurred_at=ts,
                version=len(rev.signatures) + 1,
                summary=f"{signer} 以 {role} 签署 {revision_id}",
            ))]
            # 重新折叠判断三方是否齐备。
            fresh = self._state()
            rev2 = fresh.revisions[revision_id]
            if all(r in rev2.signatures for r in SIGNOFF_ROLES) and pkg.status == "draft":
                events.append(self._append(make_event(
                    "PACKAGE_PUBLISHED", "rule_package", pkg.package_id,
                    {"package_id": pkg.package_id, "revision_id": revision_id},
                    event_id=f"publish-{pkg.package_id}-{revision_id}",
                    occurred_at=ts, version=pkg.version_no,
                    summary=f"三方签署齐备，{pkg.package_id} 生效",
                )))
            return events

    def lock_package(self, zone: str, package_id: str) -> Event:
        with self._lock:
            state = self._state()
            pkg = self._require_package(state, package_id)
            if pkg.status != "published":
                raise DomainError("只能锁定已生效的规则包")
            if zone in pkg.locked_by:
                raise DomainError(f"赛区 {zone} 已锁定该规则包")
            return self._append(make_event(
                "PACKAGE_LOCKED", "rule_package", package_id,
                {"zone": zone, "package_id": package_id},
                event_id=f"lock-{zone}-{package_id}", occurred_at=self._ts(),
                version=len(pkg.locked_by) + 1,
                summary=f"赛区 {zone} 锁定 {package_id}",
            ))

    # --- 场次 ---
    def schedule_session(self, session_id: str, *, zone: str, sport: str,
                         stage: str, package_id: str | None = None) -> Event:
        with self._lock:
            state = self._state()
            if session_id in state.sessions:
                raise DomainError(f"场次已存在：{session_id}")
            if package_id:
                pkg = self._require_package(state, package_id)
                if pkg.status != "published":
                    raise DomainError("排期只能引用已生效的规则包")
            return self._append(make_event(
                "SESSION_SCHEDULED", "sport_rulebook", session_id,
                {"session_id": session_id, "zone": zone, "sport": sport,
                 "stage": stage, "package_id": package_id},
                event_id=f"sess-{session_id}", occurred_at=self._ts(), version=1,
                summary=f"排定场次 {session_id}",
            ))

    def start_session(self, session_id: str) -> Event:
        with self._lock:
            state = self._state()
            sess = state.sessions.get(session_id)
            if sess is None:
                raise DomainError(f"场次不存在：{session_id}")
            if sess.status != SESSION_SCHEDULED:
                raise DomainError("只有尚未开始的场次可以开赛")
            return self._append(make_event(
                "SESSION_STARTED", "sport_rulebook", session_id,
                {"session_id": session_id},
                event_id=f"start-{session_id}", occurred_at=self._ts(), version=2,
                summary=f"场次 {session_id} 开赛",
            ))

    def finish_session(self, session_id: str) -> Event:
        with self._lock:
            state = self._state()
            sess = state.sessions.get(session_id)
            if sess is None or sess.status != SESSION_STARTED:
                raise DomainError("只有进行中的场次可以结束")
            return self._append(make_event(
                "SESSION_FINISHED", "sport_rulebook", session_id,
                {"session_id": session_id},
                event_id=f"finish-{session_id}", occurred_at=self._ts(), version=3,
                summary=f"场次 {session_id} 结束",
            ))

    def repin_session(self, session_id: str, package_id: str) -> Event:
        """新版本生效后，只有尚未开始的场次允许改引新版本。"""
        with self._lock:
            state = self._state()
            sess = state.sessions.get(session_id)
            if sess is None:
                raise DomainError(f"场次不存在：{session_id}")
            if sess.status != SESSION_SCHEDULED:
                raise DomainError("场次已开始或结束，既有判罚不可追改")
            pkg = self._require_package(state, package_id)
            if pkg.status != "published":
                raise DomainError("只能改引已生效的新版本")
            return self._append(make_event(
                SESSION_REPINNED, "sport_rulebook", session_id,
                {"session_id": session_id, "package_id": package_id},
                event_id=f"repin-{session_id}-{package_id}",
                occurred_at=self._ts(), version=2,
                summary=f"场次 {session_id} 改引 {package_id}",
            ))

    # --- 器材认证 / 场地安排 ---
    def register_arrangement(self, arrangement_id: str, *, zone: str,
                             session_id: str, cert_refs: list[str]) -> Event:
        with self._lock:
            state = self._state()
            if arrangement_id in state.arrangements:
                raise DomainError(f"场地安排已存在：{arrangement_id}")
            for cert_id in cert_refs:
                cert = state.certificates.get(cert_id)
                if cert is None:
                    raise DomainError(f"引用的器材证书不存在：{cert_id}")
                if cert.status == CERT_REVOKED:
                    raise DomainError(f"引用的器材证书已撤销：{cert_id}")
                if cert.dispute_ids and any(
                        state.disputes[d].status == "open"
                        for d in cert.dispute_ids if d in state.disputes):
                    raise DomainError(f"引用的器材证书处于争议中：{cert_id}")
            return self._append(make_event(
                "VENUE_ARRANGEMENT_REGISTERED", "venue_adoption", arrangement_id,
                {"arrangement_id": arrangement_id, "zone": zone,
                 "session_id": session_id, "cert_refs": list(cert_refs)},
                event_id=f"arr-{arrangement_id}", occurred_at=self._ts(), version=1,
                summary=f"登记场地安排 {arrangement_id}",
            ))

    def receive_certificate(self, cert_id: str, *, event_id: str,
                            detail: dict[str, Any] | None = None,
                            occurred_at: str | None = None) -> list[Event]:
        """受理离线送达的器材认证回执。

        由事件存储判定重复与冲突：
        - 同编号同内容 → 幂等丢弃，返回空；
        - 同编号异内容 → 开启争议，争议期间证书不视为可用；
        - 乱序送达不影响结果，事件按 occurred_at 之外的存储 seq 折叠。
        入站候选即使冲突也作为一个 variant 记录在争议负载中。
        """
        with self._lock:
            candidate = make_event(
                "CERTIFICATE_ISSUED", "equipment_certificate", cert_id,
                {"cert_id": cert_id, "detail": detail or {}},
                event_id=event_id, occurred_at=occurred_at or self._ts(), version=1,
                summary=f"器材认证签发：{cert_id}",
            )
            result = self.store.append(candidate)
            if result == APPENDED:
                return [candidate]
            if result == DUPLICATE:
                return []
            # CONFLICT：同编号异内容，收集已知变体并开启争议。
            state = self._state()
            dispute_id = f"dispute-{event_id}"
            if dispute_id in state.disputes:
                return []
            variants = self._collect_variants(event_id)
            incoming = {"content_hash": candidate.hash, "cert_id": cert_id,
                        "detail": detail or {}, "note": "未入库的冲突来件"}
            if not any(v["content_hash"] == candidate.hash for v in variants):
                variants.append(incoming)
            event = self._append(make_event(
                "DISPUTE_OPENED", "dispute", dispute_id,
                {"dispute_id": dispute_id, "event_id": event_id,
                 "cert_id": cert_id, "variants": variants},
                event_id=f"open-{dispute_id}", occurred_at=self._ts(),
                version=1, summary=f"认证回执同编号异内容：{event_id}",
            ))
            return [event]

    def _collect_variants(self, event_id: str) -> list[dict[str, Any]]:
        variants: list[dict[str, Any]] = []
        for event in self.store.all_events():
            if event.event_id == event_id:
                variants.append({"content_hash": event.hash, "cert_id": event.payload.get("cert_id"),
                                 "detail": event.payload.get("detail", {})})
        return variants

    def resolve_dispute(self, dispute_id: str, *, resolution: str) -> Event:
        with self._lock:
            state = self._state()
            if dispute_id not in state.disputes:
                raise DomainError(f"争议不存在：{dispute_id}")
            return self._append(make_event(
                "DISPUTE_RESOLVED", "dispute", dispute_id,
                {"dispute_id": dispute_id, "resolution": resolution},
                event_id=f"resolve-{dispute_id}", occurred_at=self._ts(), version=2,
                summary=f"争议裁决：{resolution}",
            ))

    def revoke_certificate(self, cert_id: str, *, reason: str = "") -> list[Event]:
        """撤销证书：只冻结实际引用它的场地安排。"""
        with self._lock:
            state = self._state()
            cert = state.certificates.get(cert_id)
            if cert is None:
                raise DomainError(f"器材证书不存在：{cert_id}")
            if cert.status == CERT_REVOKED:
                raise DomainError("证书已撤销")
            if cert.dispute_ids and any(
                    state.disputes[d].status == "open" for d in cert.dispute_ids):
                raise DomainError("证书存在未裁决争议，不能撤销，应先裁决")
            ts = self._ts()
            events = [self._append(make_event(
                "CERTIFICATE_REVOKED", "equipment_certificate", cert_id,
                {"cert_id": cert_id, "reason": reason},
                event_id=f"revoke-{cert_id}", occurred_at=ts, version=2,
                summary=f"撤销器材证书：{cert_id}",
            ))]
            fresh = self._state()
            for arr in fresh.arrangements.values():
                # 已冻结的安排若也引用新撤销的证书，追加冻结原因而非跳过。
                if cert_id in arr.cert_refs and cert_id not in arr.frozen_for:
                    events.append(self._append(make_event(
                        "VENUE_ARRANGEMENT_FROZEN", "venue_adoption",
                        arr.arrangement_id,
                        {"arrangement_id": arr.arrangement_id, "cert_id": cert_id,
                         "zone": arr.zone, "session_id": arr.session_id},
                        event_id=f"freeze-{arr.arrangement_id}-{cert_id}",
                        occurred_at=ts, version=len(arr.frozen_for) + 2,
                        summary=f"冻结引用 {cert_id} 的场地安排 {arr.arrangement_id}",
                    )))
            return events

    # --- 查询（只读） ---
    def get_state(self) -> State:
        with self._lock:
            return self._state()

    def get_package(self, package_id: str, *, as_of_seq: int | None = None,
                    as_of: str | None = None) -> dict[str, Any]:
        with self._lock:
            state = fold(self.store.all_events(), as_of_seq=as_of_seq, as_of=as_of)
            pkg = state.packages.get(package_id)
            if pkg is None:
                raise DomainError(f"规则包不存在：{package_id}")
            return self._package_view(pkg)

    @staticmethod
    def _package_view(pkg: Package) -> dict[str, Any]:
        return {
            "package_id": pkg.package_id, "sport": pkg.sport, "stage": pkg.stage,
            "version_no": pkg.version_no, "parent_id": pkg.parent_id,
            "status": pkg.status, "clauses": pkg.clauses,
            "clarifications": list(pkg.clarifications),
            "locked_by_zones": sorted(pkg.locked_by),
            "successor_id": pkg.successor_id, "published_at": pkg.published_at,
            "revisions": [
                {"revision_id": rid, "submitted_by": rev.submitted_by,
                 "change_class": rev.change_class, "summary": rev.summary,
                 "status": rev.status, "signatures": dict(rev.signatures)}
                for rid, rev in sorted(pkg.revisions.items())
            ],
        }

    def session_rules(self, session_id: str, *, as_of: str | None = None) -> dict[str, Any]:
        """还原因场次而固定的规则。

        - 已开赛/已结束：默认还原开赛时刻固定的规则，之后的澄清与新版本
          都不出现在其中（既有判罚不可追改）；
        - 尚未开赛：默认返回当前引用版本（可能已因新版本而改引）；
        - 显式传 ``as_of`` 时一律按该时点折叠。
        """
        with self._lock:
            current = self._state()
            sess = current.sessions.get(session_id)
            if sess is None:
                raise DomainError(f"场次不存在：{session_id}")
            if as_of is not None:
                point = as_of
                state = fold(self.store.all_events(), as_of=point)
                hist_sess = state.sessions.get(session_id)
                pkg_id = hist_sess.package_id if hist_sess else sess.package_id
            elif sess.status in (SESSION_STARTED, SESSION_FINISHED):
                point = sess.started_at or sess.finished_at or sess.scheduled_at
                state = fold(self.store.all_events(), as_of=point)
                hist_sess = state.sessions.get(session_id)
                pkg_id = hist_sess.package_id if hist_sess and hist_sess.package_id else sess.package_id
            else:
                # 尚未开赛：以当前状态回答它将适用的版本。
                point = None
                state = current
                pkg_id = sess.package_id
            pkg = state.packages.get(pkg_id) if pkg_id else None
            return {
                "session_id": session_id, "status": sess.status,
                "pinned_package_id": pkg_id,
                "as_of": point or "current",
                "rules": self._package_view(pkg) if pkg else None,
            }
