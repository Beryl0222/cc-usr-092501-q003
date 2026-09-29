"""领域状态折叠：把追加的事件流还原成当前（或历史时点）状态。

折叠是纯函数，同一段事件无论重放多少次、是否跨越进程重启，得到的
状态完全一致。所有历史修订、签署、锁定快照、回执与撤销都保留在事件
里，因此旧赛程可以按当时时间点还原规则。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .envelope import parse_time

ROLES = ("technical", "medical_safety", "competition_operations")


@dataclass
class Revision:
    number: int
    kind: str  # initial | new_version
    title: str
    summary: str
    contents: dict[str, Any]
    submitted_by: str
    submitted_at: str
    qualification_changed: bool
    scoring_changed: bool
    safety_changed: bool
    signatures: dict[str, str] = field(default_factory=dict)  # role -> signer
    status: str = "submitted"  # submitted | published | superseded
    published_at: str | None = None
    superseded_at: str | None = None
    impact_fixture_ids: list[str] = field(default_factory=list)


@dataclass
class Clarification:
    clarification_id: str
    venue_id: str
    locked_revision: int
    title: str
    text: str
    references: list[str]
    author: str
    appended_at: str


@dataclass
class PackageState:
    package_id: str
    sport_code: str = ""
    stage_id: str = ""
    title: str = ""
    revisions: dict[int, Revision] = field(default_factory=dict)
    current_revision: int | None = None
    clarifications: list[Clarification] = field(default_factory=list)
    version: int = 0

    def pending_revision(self) -> Revision | None:
        for rev in self.revisions.values():
            if rev.status == "submitted":
                return rev
        return None


@dataclass
class Receipt:
    receipt_id: str
    content_hash: str
    status: str  # accepted | disputed
    duplicates: int = 0


@dataclass
class CertificateState:
    cert_id: str
    package_id: str = ""
    equipment_code: str = ""
    receipts: dict[str, Receipt] = field(default_factory=dict)
    status: str = "pending"  # pending | active | disputed | revoked
    revoked_at: str | None = None
    revoke_reason: str | None = None
    version: int = 0


@dataclass
class VenueState:
    venue_id: str
    name: str = ""
    locks: list[dict[str, Any]] = field(default_factory=list)  # 全部历史锁定
    version: int = 0

    def lock_at(self, as_of: str | None = None) -> dict[str, Any] | None:
        """as_of 时点（含）生效的锁定快照；缺省取最新。"""
        chosen = None
        for lock in self.locks:
            if as_of is None or parse_time(lock["locked_at"]) <= parse_time(as_of):
                chosen = lock
        return chosen


@dataclass
class FixtureState:
    fixture_id: str
    venue_id: str = ""
    package_id: str = ""
    scheduled_start: str | None = None
    cert_refs: list[str] = field(default_factory=list)
    status: str = "scheduled"  # scheduled | started | finished
    frozen: bool = False
    frozen_reasons: list[dict[str, str]] = field(default_factory=list)
    version: int = 0

    def affected_by_revoke(self, cert_id: str) -> bool:
        return self.status == "scheduled" and not self.frozen and cert_id in self.cert_refs


@dataclass
class Registry:
    sports: dict[str, dict[str, Any]] = field(default_factory=dict)
    stages: dict[str, dict[str, Any]] = field(default_factory=dict)
    packages: dict[str, PackageState] = field(default_factory=dict)
    certificates: dict[str, CertificateState] = field(default_factory=dict)
    venues: dict[str, VenueState] = field(default_factory=dict)
    fixtures: dict[str, FixtureState] = field(default_factory=dict)
    judges: dict[str, dict[str, Any]] = field(default_factory=dict)
    cases: dict[str, dict[str, Any]] = field(default_factory=dict)
    supplements: dict[str, dict[str, Any]] = field(default_factory=dict)


def fold(events: list[dict]) -> Registry:
    reg = Registry()
    for e in events:
        etype = e["event_type"]
        data = e.get("data", {})
        at = e["occurred_at"]

        if etype == "SPORT_REGISTERED":
            reg.sports[data["code"]] = {"code": data["code"], "name": data["name"], "at": at}

        elif etype == "COMPETITION_STAGE_DEFINED":
            reg.stages[data["stage_id"]] = dict(data)

        elif etype == "RULE_PACKAGE_DRAFTED":
            pid = e["aggregate_id"]
            reg.packages[pid] = PackageState(
                package_id=pid,
                sport_code=data.get("sport_code", ""),
                stage_id=data.get("stage_id", ""),
                title=data.get("title", ""),
            )

        elif etype == "RULE_SUBMITTED":
            pkg = reg.packages[e["aggregate_id"]]
            rev = Revision(
                number=int(data["revision"]),
                kind=data.get("kind", "initial"),
                title=data.get("title", pkg.title),
                summary=data.get("summary", ""),
                contents=data.get("contents", {}),
                submitted_by=data["submitted_by"],
                submitted_at=at,
                qualification_changed=bool(data.get("qualification_changed", False)),
                scoring_changed=bool(data.get("scoring_changed", False)),
                safety_changed=bool(data.get("safety_changed", False)),
                impact_fixture_ids=list(data.get("impact_fixture_ids", [])),
            )
            pkg.revisions[rev.number] = rev

        elif etype == "REVIEW_SIGNED":
            pkg = reg.packages[e["aggregate_id"]]
            rev = pkg.revisions[int(data["revision"])]
            rev.signatures[data["role"]] = data["signer"]

        elif etype == "PACKAGE_PUBLISHED":
            pkg = reg.packages[e["aggregate_id"]]
            rev = pkg.revisions[int(data["revision"])]
            rev.status = "published"
            rev.published_at = at
            pkg.current_revision = rev.number

        elif etype == "PACKAGE_VERSION_SUPERSEDED":
            pkg = reg.packages[e["aggregate_id"]]
            old = pkg.revisions[int(data["revision"])]
            old.status = "superseded"
            old.superseded_at = at

        elif etype == "VENUE_LOCKED":
            venue = reg.venues.get(data["venue_id"])
            if venue is None:
                venue = VenueState(venue_id=data["venue_id"], name=data.get("name", ""))
                reg.venues[data["venue_id"]] = venue
            venue.locks.append(
                {
                    "package_id": data["package_id"],
                    "revision": int(data["revision"]),
                    "snapshot": data["snapshot"],
                    "locked_at": at,
                }
            )

        elif etype == "CLARIFICATION_APPENDED":
            pkg = reg.packages[e["aggregate_id"]]
            pkg.clarifications.append(
                Clarification(
                    clarification_id=data["clarification_id"],
                    venue_id=data["venue_id"],
                    locked_revision=int(data["locked_revision"]),
                    title=data["title"],
                    text=data["text"],
                    references=list(data.get("references", [])),
                    author=data["author"],
                    appended_at=at,
                )
            )

        elif etype in ("CERTIFICATE_RECEIVED", "CERTIFICATE_DUPLICATE_RECEIVED", "RECEIPT_DISPUTED"):
            if e["aggregate_id"] not in reg.certificates:
                reg.certificates[e["aggregate_id"]] = CertificateState(
                    cert_id=e["aggregate_id"],
                    package_id=data.get("package_id", ""),
                    equipment_code=data.get("equipment_code", ""),
                )
            cert = reg.certificates[e["aggregate_id"]]
            rid = data["receipt_id"]
            if etype == "CERTIFICATE_RECEIVED":
                cert.receipts[rid] = Receipt(rid, data["content_hash"], "accepted")
                # 争议状态必须显式处置，新回执不得自动“洗白”证书
                if cert.status != "disputed":
                    cert.status = "active"
            elif etype == "CERTIFICATE_DUPLICATE_RECEIVED":
                cert.receipts[rid].duplicates += 1
            else:
                receipt = cert.receipts.get(rid)
                if receipt is not None:
                    receipt.status = "disputed"
                cert.status = "disputed"

        elif etype == "CERTIFICATE_REVOKED":
            cert = reg.certificates[e["aggregate_id"]]
            cert.status = "revoked"
            cert.revoked_at = at
            cert.revoke_reason = data.get("reason", "")

        elif etype == "FIXTURE_DECLARED":
            fid = e["aggregate_id"]
            reg.fixtures[fid] = FixtureState(
                fixture_id=fid,
                venue_id=data["venue_id"],
                package_id=data["package_id"],
                scheduled_start=data.get("scheduled_start"),
                cert_refs=list(data.get("cert_refs", [])),
            )

        elif etype == "FIXTURE_STARTED":
            reg.fixtures[e["aggregate_id"]].status = "started"

        elif etype == "FIXTURE_FROZEN":
            fx = reg.fixtures[e["aggregate_id"]]
            fx.frozen = True
            fx.frozen_reasons.append({"cert_id": data["cert_id"], "at": at, "reason": data.get("reason", "")})

        elif etype == "FIXTURE_UNFROZEN":
            fx = reg.fixtures[e["aggregate_id"]]
            fx.frozen = False
            fx.frozen_reasons = [r for r in fx.frozen_reasons if r["cert_id"] != data["cert_id"]]

        elif etype == "JUDGE_LEVEL_GRANTED":
            reg.judges[data["judge_id"]] = dict(data)

        elif etype == "INTERPRETATION_CASED":
            reg.cases[data["case_id"]] = dict(data)

        elif etype == "LOCAL_SUPPLEMENT_ADDED":
            reg.supplements[data["supplement_id"]] = dict(data)

    # 记录每个聚合的最新版本号（用于命令侧取下一版本）
    counts: dict[str, int] = {}
    for e in events:
        key = f"{e['aggregate_type']}:{e['aggregate_id']}"
        counts[key] = max(counts.get(key, 0), int(e["version"]))
    for pid, pkg in reg.packages.items():
        pkg.version = counts.get(f"rule_package:{pid}", 0)
    for cid, cert in reg.certificates.items():
        cert.version = counts.get(f"equipment_certificate:{cid}", 0)
    for vid, venue in reg.venues.items():
        venue.version = counts.get(f"venue_adoption:{vid}", 0)
    for fid, fx in reg.fixtures.items():
        fx.version = counts.get(f"fixture:{fid}", 0)
    reg._counts = counts  # type: ignore[attr-defined]
    return reg


def package_at(reg: Registry, package_id: str, as_of: str) -> Revision | None:
    """返回 as_of 时点处于生效状态的已发布修订。"""
    pkg = reg.packages.get(package_id)
    if pkg is None:
        return None
    moment = parse_time(as_of)
    candidates = []
    for rev in pkg.revisions.values():
        if rev.published_at is None or parse_time(rev.published_at) > moment:
            continue
        if rev.superseded_at is not None and parse_time(rev.superseded_at) <= moment:
            continue
        candidates.append(rev)
    return max(candidates, key=lambda r: r.number) if candidates else None
