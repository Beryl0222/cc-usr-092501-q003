"""SQLite 追加型事件存储。

只提供事件的追加与读取，业务规则不在这一层。硬性并发保证由数据库
约束承担：

- ``event_id`` 主键：同一编号只可能落地一次，同编号异内容抛冲突；
- ``(aggregate_id, version)`` 唯一：并发提交同一聚合的下一版本时，
  只有一个事务能成功，另一个收到版本冲突；
- 一次命令产生的多个事件在同一事务内提交，进程崩溃时要么全部可见、
  要么全部不可见，因此待审修订不会半截丢失。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from .envelope import validate_event

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT NOT NULL UNIQUE,
    event_type     TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    version        INTEGER NOT NULL,
    occurred_at    TEXT NOT NULL,
    data           TEXT NOT NULL,
    UNIQUE (aggregate_id, version)
);
"""


class EventConflict(Exception):
    """同一 event_id 已以不同内容落地（同编号异内容争议）。"""


class VersionConflict(Exception):
    """聚合版本已被占用，典型于并发命令竞争同一聚合的下一版本。"""


def canonical_json(record: Any) -> str:
    return json.dumps(record, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


class EventStore:
    def __init__(self, path: str | Path = ":memory:") -> None:
        # check_same_thread=False：写操作由 self._lock 串行化。
        self._conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)
        self._lock = threading.RLock()

    @property
    def lock(self) -> threading.RLock:
        """命令侧在一个锁内完成“读状态—判定—追加”，避免读后写竞态。"""
        return self._lock

    def close(self) -> None:
        self._conn.close()

    def append(self, record: dict) -> dict:
        """追加单个事件，返回落地状态。

        返回 ``{"status": "appended"|"duplicate"}``；同编号异内容抛
        :class:`EventConflict`，版本被并发占用抛 :class:`VersionConflict`。
        """
        return self.append_many([record])[0]

    def append_many(self, records: list[dict]) -> list[dict]:
        normalized: list[dict] = []
        for record in records:
            errors = validate_event(record)
            if errors:
                raise ValueError("；".join(errors))
            normalized.append(dict(record))

        results: list[dict] = []
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for record in normalized:
                    existing = self._conn.execute(
                        "SELECT data FROM events WHERE event_id = ?",
                        (record["event_id"],),
                    ).fetchone()
                    if existing is not None:
                        if canonical_json(json.loads(existing["data"])) == canonical_json(record):
                            results.append({"status": "duplicate", "event_id": record["event_id"]})
                            continue
                        raise EventConflict(record["event_id"])
                    try:
                        cursor = self._conn.execute(
                            "INSERT INTO events (event_id, event_type, aggregate_type, "
                            "aggregate_id, version, occurred_at, data) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (
                                record["event_id"],
                                record["event_type"],
                                record["aggregate_type"],
                                record["aggregate_id"],
                                record["version"],
                                record["occurred_at"],
                                canonical_json(record),
                            ),
                        )
                    except sqlite3.IntegrityError as error:
                        raise VersionConflict(
                            f"{record['aggregate_type']}:{record['aggregate_id']} 版本 "
                            f"{record['version']} 已存在"
                        ) from error
                    results.append(
                        {"status": "appended", "event_id": record["event_id"], "seq": cursor.lastrowid}
                    )
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            self._conn.execute("COMMIT")
        return results

    def load_events(self, aggregate_id: str | None = None) -> list[dict]:
        with self._lock:
            if aggregate_id is None:
                rows = self._conn.execute("SELECT data FROM events ORDER BY seq").fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT data FROM events WHERE aggregate_id = ? ORDER BY seq",
                    (aggregate_id,),
                ).fetchall()
        return [json.loads(row["data"]) for row in rows]

    def latest_seq(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COALESCE(MAX(seq), 0) AS s FROM events").fetchone()
            return int(row["s"])
