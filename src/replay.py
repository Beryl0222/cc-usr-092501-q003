"""确定性重放一组发布与撤销事件的命令入口。

用法：

    python3 -m src.replay data/replay_sample.json
    python3 -m src.replay batch.json --db run.sqlite
    python3 -m src.replay batch.json --check <报告中的 event_stream_hash>

输入是一个 JSON 对象：

    {"commands": [
      {"command_id": "c1", "occurred_at": "2026-09-25T09:00:00+08:00",
       "command": "register_sport", "args": {"sport": "teqball", ...}}, ...]}

乱序送达没有影响：重放前按 ``(occurred_at, command_id)`` 稳定排序；
重复的 ``receive_certificate`` 幂等丢弃，同编号异内容进入争议。
报告中的 ``event_stream_hash`` 是最终事件流规范化后的 SHA-256，
同一批命令无论文件内排列如何，哈希必须一致，可用于比对与 ``--check``。

``--db`` 把结果写入 SQLite（文件已存在则在其上继续重放），
用于验证进程恢复：命令全部以固定 occurred_at 落库，重启后状态可重建。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .domain import DomainError, RuleCertificationService
from .events import canonical_payload
from .store import InMemoryEventStore, SqliteEventStore


def _dispatch(svc: RuleCertificationService, name: str, args: dict[str, Any]) -> Any:
    """命令名到服务方法的映射；返回事件或事件列表。"""
    m = getattr(svc, name)
    return m(**args)


def _flatten(result: Any) -> list[dict[str, str]]:
    if result is None:
        return []
    if isinstance(result, list):
        return [{"event_id": e.event_id, "event_type": e.event_type} for e in result]
    return [{"event_id": result.event_id, "event_type": result.event_type}]


def replay_batch(batch: dict[str, Any], store: Any | None = None,
                 *, fail_fast: bool = False) -> dict[str, Any]:
    commands = list(batch.get("commands", []))
    # 确定性排序：先按发生时间，再按命令编号；文件内先后次序不影响结果。
    ordered = sorted(
        enumerate(commands),
        key=lambda pair: (pair[1].get("occurred_at", ""), pair[1].get("command_id", f"#{pair[0]}")),
    )

    svc = RuleCertificationService(store or InMemoryEventStore())
    results: list[dict[str, Any]] = []
    for sort_index, (original_index, command) in enumerate(ordered, start=1):
        cid = command.get("command_id", f"#{original_index}")
        ts = command.get("occurred_at")
        name = command.get("command")
        args = dict(command.get("args", {}))
        entry: dict[str, Any] = {
            "order": sort_index, "command_id": cid, "command": name,
            "occurred_at": ts, "file_index": original_index,
        }
        if not ts:
            # 没有确定时间戳的命令无法产生可复现的事件流哈希。
            entry["status"] = "rejected"
            entry["error"] = "确定性重放要求每条命令携带 occurred_at"
            results.append(entry)
            continue
        # 固定该命令的时钟，保证重放与进程恢复后时间戳一致。
        svc.clock = lambda t=ts: t
        try:
            produced = _dispatch(svc, name, args)
            events = _flatten(produced)
            entry["status"] = "deduplicated" if not events else "applied"
            entry["events"] = events
        except DomainError as error:
            entry["status"] = "rejected"
            entry["error"] = str(error)
            if fail_fast:
                raise
        except TypeError as error:
            entry["status"] = "rejected"
            entry["error"] = f"命令参数错误：{error}"
        results.append(entry)

    events = svc.store.all_events()
    stream = [e.to_dict() for e in events]
    stream_hash = __import__("hashlib").sha256(canonical_payload(stream)).hexdigest()
    state = svc.get_state()
    report = {
        "command_count": len(ordered),
        "applied": sum(1 for r in results if r["status"] == "applied"),
        "deduplicated": sum(1 for r in results if r["status"] == "deduplicated"),
        "rejected": sum(1 for r in results if r["status"] == "rejected"),
        "event_stream_hash": stream_hash,
        "results": results,
        "state": {
            "packages": {
                pid: {"version_no": p.version_no, "status": p.status,
                      "locked_by_zones": sorted(p.locked_by),
                      "successor_id": p.successor_id,
                      "clarification_count": len(p.clarifications)}
                for pid, p in sorted(state.packages.items())
            },
            "pending_revisions": sorted(
                rid for rid, r in state.revisions.items() if r.status == "pending"),
            "certificates": {
                cid: {"status": c.status, "dispute_ids": c.dispute_ids}
                for cid, c in sorted(state.certificates.items())
            },
            "frozen_arrangements": sorted(
                aid for aid, a in state.arrangements.items() if a.status == "frozen"),
            "disputes": {
                did: {"event_id": d.event_id, "status": d.status,
                      "variant_count": len(d.variants)}
                for did, d in sorted(state.disputes.items())
            },
        },
    }
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="确定性重放发布与撤销事件批次")
    parser.add_argument("batch", help="命令批次 JSON 文件")
    parser.add_argument("--db", help="写入/续写的 SQLite 路径（默认内存重放）")
    parser.add_argument("--check", metavar="HASH", help="断言事件流哈希等于该值")
    parser.add_argument("--fail-fast", action="store_true", help="首条拒绝即失败退出")
    ns = parser.parse_args(argv)
    try:
        batch = json.loads(Path(ns.batch).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"无法读取批次：{error}", file=sys.stderr)
        return 2
    store = SqliteEventStore(ns.db) if ns.db else None
    report = replay_batch(batch, store, fail_fast=ns.fail_fast)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if ns.check and report["event_stream_hash"] != ns.check:
        print(f"哈希不一致：期望 {ns.check}，实际 {report['event_stream_hash']}",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
