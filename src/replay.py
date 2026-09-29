"""确定性重放一组发布与撤销事件。

输入为 JSONL（每行一个领域事件信封）。离线送达可能乱序或重复，重放时：

1. 按 ``(occurred_at, aggregate_id, version, event_id)`` 做确定性排序，
   结果与送达顺序无关；
2. 同一 event_id 且内容完全一致：计为重复，只落地一次；
3. 同一 event_id 内容不同：列入争议，不覆盖先到（按确定性顺序）的内容；
4. 折叠出最终状态，并对落地序列计算 sha256 指纹——同一输入多次重放
   （包括在另一台机器、另一个进程里）得到逐字节一致的报告。

用法：

    python3 -m src.replay data/demo_events.jsonl
    python3 -m src.replay data/demo_events.jsonl --db rule_cert.db
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from .envelope import validate_event
from .service import RuleCertService
from .state import fold
from .store import EventStore, canonical_json

ORDER_KEY = ("occurred_at", "aggregate_id", "version", "event_id")


def load_jsonl(path: Path) -> list[dict]:
    events: list[dict] = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"第 {lineno} 行不是合法 JSON：{error}") from error
        errors = validate_event(record)
        if errors:
            raise ValueError(f"第 {lineno} 行事件无效：{'；'.join(errors)}")
        events.append(record)
    return events


def replay_events(events: list[dict], store: EventStore | None = None) -> dict[str, Any]:
    """重放事件并返回确定性报告。store 缺省使用全新内存库。"""
    own_store = store is None
    store = store or EventStore(":memory:")
    try:
        # 确定性排序：送达顺序不影响最终状态。
        ordered = sorted(
            events,
            key=lambda e: (e["occurred_at"], e["aggregate_id"], e["version"], e["event_id"]),
        )

        seen: dict[str, dict] = {}
        disputes: list[dict] = []
        duplicates = 0
        to_append: list[dict] = []
        for event in ordered:
            prior = seen.get(event["event_id"])
            if prior is not None:
                if canonical_json(prior) == canonical_json(event):
                    duplicates += 1
                    continue
                disputes.append({
                    "event_id": event["event_id"],
                    "kept_occurred_at": prior["occurred_at"],
                    "rejected_occurred_at": event["occurred_at"],
                    "reason": "同一 event_id 内容不一致，按争议处理，保留确定性排序中先到的内容",
                })
                continue
            seen[event["event_id"]] = event
            to_append.append(event)

        results = store.append_many(to_append)
        newly_appended = sum(1 for r in results if r["status"] == "appended")
        folded = fold(store.load_events())
        fingerprint_src = "\n".join(canonical_json(e) for e in to_append)
        fingerprint = hashlib.sha256(fingerprint_src.encode("utf-8")).hexdigest()

        counts: dict[str, int] = {}
        for event in to_append:
            counts[event["event_type"]] = counts.get(event["event_type"], 0) + 1

        svc = RuleCertService(store)
        packages = [svc.get_package(pid) for pid in sorted(folded.packages)]
        venues = [svc.get_venue(vid) for vid in sorted(folded.venues)]
        certificates = [svc.get_certificate(cid) for cid in sorted(folded.certificates)]
        receipt_disputes = [
            {"cert_id": cid, "receipt_id": r["receipt_id"],
             "reason": "同一回执编号收到不同内容，证书进入争议"}
            for cid, cert in folded.certificates.items()
            if cert.status == "disputed"
            for r in (
                {"receipt_id": rid}
                for rid, receipt in cert.receipts.items()
                if receipt.status == "disputed"
            )
        ]
        disputes = disputes + receipt_disputes

        report: dict[str, Any] = {
            "ordering": list(ORDER_KEY),
            "input_events": len(events),
            "appended_events": newly_appended,
            "duplicate_events": duplicates,
            "disputes": disputes,
            "event_counts": dict(sorted(counts.items())),
            "fingerprint_sha256": fingerprint,
            "packages": packages,
            "venues": venues,
            "certificates": certificates,
            "fixtures": svc.list_fixtures(),
        }
        return report
    finally:
        if own_store:
            store.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="确定性重放发布与撤销事件（JSONL）")
    parser.add_argument("input", type=Path, help="JSONL 事件文件")
    parser.add_argument("--db", default=":memory:", help="可选：同时落入的 SQLite 路径")
    args = parser.parse_args(argv)

    try:
        events = load_jsonl(args.input)
        store = EventStore(args.db)
        report = replay_events(events, store)
        store.close()
    except (OSError, ValueError) as error:
        print(f"重放失败：{error}", file=sys.stderr)
        return 2

    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
