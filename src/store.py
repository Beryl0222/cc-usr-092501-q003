"""事件存储：SQLite 持久化日志与内存实现。

事件按追加顺序获得单调递增的 ``seq``，重放时严格按 ``seq`` 排序，
因此发布与撤销的处理结果与送达顺序无关、可确定性复现。

追加同一 ``event_id`` 时：
- 负载哈希相同 → 幂等重复（``duplicate``），不产生新事实；
- 负载哈希不同 → 同号异内容（``conflict``），交由领域层开启争议。
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Iterable, Protocol

from .events import Event, canonical_payload

APPENDED = "appended"
DUPLICATE = "duplicate"
CONFLICT = "conflict"


class EventStore(Protocol):
    def append(self, event: Event) -> str: ...
    def all_events(self) -> list[Event]: ...
    def events_for_aggregate(self, aggregate_id: str) -> list[Event]: ...
    def reset(self) -> None: ...


class InMemoryEventStore:
    def __init__(self) -> None:
        self._rows: list[tuple[int, Event, str]] = []
        self._seq = 0
        self._by_id: dict[str, str] = {}
        self._lock = threading.Lock()

    def append(self, event: Event) -> str:
        with self._lock:
            known = self._by_id.get(event.event_id)
            if known is not None:
                return DUPLICATE if known == event.hash else CONFLICT
            self._seq += 1
            self._rows.append((self._seq, event, event.hash))
            self._by_id[event.event_id] = event.hash
            return APPENDED

    def all_events(self) -> list[Event]:
        with self._lock:
            return [event for _, event, _ in sorted(self._rows, key=lambda r: r[0])]

    def events_for_aggregate(self, aggregate_id: str) -> list[Event]:
        return [e for e in self.all_events() if e.aggregate_id == aggregate_id]

    def reset(self) -> None:
        with self._lock:
            self._rows.clear()
            self._by_id.clear()
            self._seq = 0


class SqliteEventStore:
    """进程崩溃后事件仍在；待审修订作为事件同样持久化。"""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS event_log (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        aggregate_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
        occurred_at TEXT NOT NULL,
        version INTEGER NOT NULL,
        summary TEXT NOT NULL,
        payload BLOB NOT NULL,
        payload_hash TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_event_id ON event_log(event_id);
    """

    def __init__(self, path: str | Path):
        self.path = str(path)
        self._lock = threading.Lock()
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.executescript(self.SCHEMA)
            conn.commit()
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def append(self, event: Event) -> str:
        import json
        payload_bytes = canonical_payload(event.payload)
        conn = self._connect()
        try:
            with self._lock, conn:
                row = conn.execute(
                    "SELECT payload_hash FROM event_log WHERE event_id = ? LIMIT 1",
                    (event.event_id,),
                ).fetchone()
                if row is not None:
                    return DUPLICATE if row["payload_hash"] == event.hash else CONFLICT
                conn.execute(
                    "INSERT INTO event_log (event_id, event_type, aggregate_type, "
                    "aggregate_id, occurred_at, version, summary, payload, payload_hash) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (event.event_id, event.event_type, event.aggregate_type,
                     event.aggregate_id, event.occurred_at, event.version,
                     event.summary, payload_bytes, event.hash),
                )
        finally:
            conn.close()
        return APPENDED

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> Event:
        import json
        return Event(
            event_id=row["event_id"], event_type=row["event_type"],
            aggregate_type=row["aggregate_type"], aggregate_id=row["aggregate_id"],
            occurred_at=row["occurred_at"], version=row["version"],
            summary=row["summary"], payload=json.loads(row["payload"]),
        )

    def all_events(self) -> list[Event]:
        conn = self._connect()
        try:
            rows = conn.execute("SELECT * FROM event_log ORDER BY seq").fetchall()
        finally:
            conn.close()
        return [self._row_to_event(r) for r in rows]

    def events_for_aggregate(self, aggregate_id: str) -> list[Event]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM event_log WHERE aggregate_id = ? ORDER BY seq",
                (aggregate_id,),
            ).fetchall()
        finally:
            conn.close()
        return [self._row_to_event(r) for r in rows]

    def reset(self) -> None:
        conn = self._connect()
        try:
            with self._lock, conn:
                conn.execute("DELETE FROM event_log")
        finally:
            conn.close()
