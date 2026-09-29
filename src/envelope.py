"""领域事件信封的基础校验。

事件类型覆盖规则包的提交、签署、发布、锁定、澄清、新版本、
证书送达与撤销、判罚冻结等领域事实。业务修订只通过追加事件表达，
原始记录始终保留用于追溯。
"""

from __future__ import annotations

from datetime import datetime

REQUIRED = (
    "event_id",
    "event_type",
    "aggregate_type",
    "aggregate_id",
    "occurred_at",
    "version",
    "summary",
)

EVENT_TYPES = (
    "SPORT_REGISTERED",
    "COMPETITION_STAGE_DEFINED",
    "RULE_PACKAGE_DRAFTED",
    "RULE_SUBMITTED",
    "REVIEW_SIGNED",
    "PACKAGE_PUBLISHED",
    "VENUE_LOCKED",
    "CLARIFICATION_APPENDED",
    "PACKAGE_VERSION_SUPERSEDED",
    "CERTIFICATE_RECEIVED",
    "CERTIFICATE_DUPLICATE_RECEIVED",
    "RECEIPT_DISPUTED",
    "CERTIFICATE_REVOKED",
    "FIXTURE_DECLARED",
    "FIXTURE_STARTED",
    "FIXTURE_FROZEN",
    "FIXTURE_UNFROZEN",
    "JUDGE_LEVEL_GRANTED",
    "INTERPRETATION_CASED",
    "LOCAL_SUPPLEMENT_ADDED",
)

AGGREGATE_TYPES = (
    "sport",
    "competition_stage",
    "rule_package",
    "rule_clause",
    "equipment_certificate",
    "venue_adoption",
    "fixture",
    "judge",
    "interpretation_case",
    "local_supplement",
)


def parse_time(value: object) -> datetime:
    """解析带时区的 ISO 8601 时间，缺时区或格式非法时抛 ValueError。"""
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("occurred_at 必须包含时区")
    return parsed


def validate_event(record: object) -> list[str]:
    if not isinstance(record, dict):
        return ["事件必须是 JSON 对象"]
    errors = [f"缺少字段：{name}" for name in REQUIRED if name not in record]
    if "version" in record and (
        not isinstance(record["version"], int)
        or isinstance(record["version"], bool)
        or record["version"] < 1
    ):
        errors.append("version 必须是正整数")
    if "event_type" in record and record["event_type"] not in EVENT_TYPES:
        errors.append(f"未知 event_type：{record['event_type']}")
    if "aggregate_type" in record and record["aggregate_type"] not in AGGREGATE_TYPES:
        errors.append(f"未知 aggregate_type：{record['aggregate_type']}")
    if "occurred_at" in record:
        try:
            parse_time(record["occurred_at"])
        except ValueError:
            errors.append("occurred_at 必须是带时区的 ISO 8601 时间")
    return errors
