"""领域事件类型、信封构造与校验。

事件是规则认证库中唯一的事实来源：业务修订通过追加事件表达，
原始记录永不原地修改。事件负载经规范化后取 SHA-256，用于离线
送达场景下识别“同编号重复”与“同编号异内容”。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

# --- 签署角色：一份规则包必须三方各自签署才能生效 ---
ROLE_TECHNICAL = "technical"            # 技术
ROLE_MEDICAL_SAFETY = "medical_safety"  # 医疗安全
ROLE_COMPETITION_OPS = "competition_ops"  # 竞赛运营
SIGNOFF_ROLES = (ROLE_TECHNICAL, ROLE_MEDICAL_SAFETY, ROLE_COMPETITION_OPS)

# --- 事件类型 ---
SPORT_REGISTERED = "SPORT_REGISTERED"                # 项目登记
PACKAGE_CREATED = "PACKAGE_CREATED"                  # 规则包（版本）创建
REVISION_SUBMITTED = "REVISION_SUBMITTED"            # 修订提交（待审）
REVIEW_SIGNED = "REVIEW_SIGNED"                      # 某一角色签署
PACKAGE_PUBLISHED = "PACKAGE_PUBLISHED"              # 三方齐备，规则包版本生效
PACKAGE_LOCKED = "PACKAGE_LOCKED"                    # 赛区锁定规则包
CLARIFICATION_APPENDED = "CLARIFICATION_APPENDED"    # 非破坏性澄清追加引用
NEW_VERSION_DEMANDED = "NEW_VERSION_DEMANDED"        # 破坏性勘误必须转新版本
IMPACT_LISTED = "IMPACT_LISTED"                      # 新版本列出受影响的未开始场次
CERTIFICATE_ISSUED = "CERTIFICATE_ISSUED"            # 器材认证签发（离线回执受理）
CERTIFICATE_REVOKED = "CERTIFICATE_REVOKED"          # 器材证书撤销
VENUE_ARRANGEMENT_REGISTERED = "VENUE_ARRANGEMENT_REGISTERED"  # 场地安排登记
VENUE_ARRANGEMENT_FROZEN = "VENUE_ARRANGEMENT_FROZEN"  # 冻结实际引用撤销证书的场地安排
SESSION_SCHEDULED = "SESSION_SCHEDULED"              # 场次排定
SESSION_STARTED = "SESSION_STARTED"                  # 场次开赛（规则随之固定）
SESSION_FINISHED = "SESSION_FINISHED"                # 场次结束
SESSION_REPINNED = "SESSION_REPINNED"                # 未开始场次改引新版本
RECEIPT_DEDUPLICATED = "RECEIPT_DEDUPLICATED"        # 离线回执重复，幂等丢弃
DISPUTE_OPENED = "DISPUTE_OPENED"                    # 同编号异内容进入争议
DISPUTE_RESOLVED = "DISPUTE_RESOLVED"                # 争议裁决

ALL_EVENT_TYPES = {
    SPORT_REGISTERED, PACKAGE_CREATED,
    REVISION_SUBMITTED, REVIEW_SIGNED, PACKAGE_PUBLISHED, PACKAGE_LOCKED,
    CLARIFICATION_APPENDED, NEW_VERSION_DEMANDED, IMPACT_LISTED,
    CERTIFICATE_ISSUED, CERTIFICATE_REVOKED,
    VENUE_ARRANGEMENT_REGISTERED, VENUE_ARRANGEMENT_FROZEN,
    SESSION_SCHEDULED, SESSION_STARTED, SESSION_FINISHED, SESSION_REPINNED,
    RECEIPT_DEDUPLICATED, DISPUTE_OPENED, DISPUTE_RESOLVED,
}

AGGREGATE_TYPES = {
    "sport_rulebook", "rule_clause", "equipment_certificate", "venue_adoption",
    "rule_revision", "rule_package", "dispute",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_payload(payload: dict[str, Any]) -> bytes:
    """规范化负载字节：键排序、无空白，保证哈希跨进程一致。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def content_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_payload(payload)).hexdigest()


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    aggregate_type: str
    aggregate_id: str
    occurred_at: str
    version: int
    summary: str
    payload: dict[str, Any]

    @property
    def hash(self) -> str:
        return content_hash(self.payload)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
            "occurred_at": self.occurred_at,
            "version": self.version,
            "summary": self.summary,
            "payload": self.payload,
            "content_hash": self.hash,
        }

    @staticmethod
    def from_dict(record: dict[str, Any]) -> "Event":
        return Event(
            event_id=record["event_id"],
            event_type=record["event_type"],
            aggregate_type=record["aggregate_type"],
            aggregate_id=record["aggregate_id"],
            occurred_at=record["occurred_at"],
            version=int(record["version"]),
            summary=record["summary"],
            payload=record.get("payload", {}),
        )


def make_event(event_type: str, aggregate_type: str, aggregate_id: str,
               payload: dict[str, Any], *, event_id: str | None = None,
               occurred_at: str | None = None, version: int = 1,
               summary: str = "") -> Event:
    if event_type not in ALL_EVENT_TYPES:
        raise ValueError(f"未知事件类型：{event_type}")
    if aggregate_type not in AGGREGATE_TYPES:
        raise ValueError(f"未知聚合类型：{aggregate_type}")
    ts = occurred_at or now_iso()
    parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("occurred_at 必须包含时区")
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise ValueError("version 必须是正整数")
    return Event(
        event_id=event_id or f"{event_type.lower()}-{aggregate_id}-{version}",
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        occurred_at=ts,
        version=version,
        summary=summary or event_type,
        payload=payload,
    )
