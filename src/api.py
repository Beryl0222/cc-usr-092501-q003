"""规则查询与锁定的 HTTP API（仅依赖标准库）。

线程模型：``ThreadingHTTPServer`` 并发接收，命令在服务的单把锁内完成
“折叠 → 校验 → 追加”，因此并发签署不会出现两个请求同时通过角色检查。

端点（JSON）：
  POST /sports                                 登记项目
  POST /packages                               创建规则包
  GET  /packages/{id}                          查询规则包（?as_of=ISO 时点还原）
  POST /packages/{id}/revisions                提交修订（change_class 决定走向）
  POST /revisions/{id}/sign                    某角色签署（不能复核本人提交）
  POST /zones/{zone}/locks                     赛区锁定规则包
  POST /sessions                               排定场次
  POST /sessions/{id}/start | /finish          开赛 / 完赛
  POST /sessions/{id}/repin                    未开始场次改引新版本
  GET  /sessions/{id}/rules                    还原场次当时固定的规则（?as_of=）
  POST /certificates/receipts                  离线认证回执受理（乱序/重复安全）
  POST /certificates/{id}/revoke               撤销证书（精确冻结引用场地）
  GET  /certificates                           证书状态与争议
  POST /arrangements                           登记场地安排
  GET  /arrangements                           场地安排与冻结影响范围
  GET  /disputes                               争议列表
  GET  /events                                 事件审计流（?since_seq=）
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlsplit

from .domain import DomainError, RuleCertificationService
from .store import SqliteEventStore


def _json_response(handler: BaseHTTPRequestHandler, status: int, body: Any) -> None:
    data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


class NotFound(Exception):
    """路由未命中，映射为 404。"""


class _Handler(BaseHTTPRequestHandler):
    server_version = "RuleCertification/1.0"

    @property
    def service(self) -> RuleCertificationService:
        return self.server.service  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        pass  # 测试环境静默

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as error:
            raise DomainError(f"请求体不是合法 JSON：{error}")
        if not isinstance(body, dict):
            raise DomainError("请求体必须是 JSON 对象")
        return body

    def _query(self) -> dict[str, str]:
        return {k: v[0] for k, v in parse_qs(urlsplit(self.path).query).items()}

    def _serve(self, fn: Callable[[], Any]) -> None:
        try:
            result = fn()
        except NotFound as error:
            _json_response(self, 404, {"error": str(error)})
        except DomainError as error:
            _json_response(self, 409, {"error": str(error)})
        except KeyError as error:
            _json_response(self, 400, {"error": f"缺少字段：{error.args[0]}"})
        except (TypeError, ValueError) as error:
            _json_response(self, 400, {"error": str(error)})
        else:
            _json_response(self, 200 if result is not None else 201,
                           result if result is not None else {"ok": True})

    def do_GET(self) -> None:
        parts = [p for p in urlsplit(self.path).path.split("/") if p]
        q = self._query()

        def route() -> Any:
            if parts == ["events"]:
                events = self.service.store.all_events()
                since = int(q.get("since_seq", 0) or 0)
                return {"events": [e.to_dict() for e in events[since:]]}
            if len(parts) == 2 and parts[0] == "packages":
                return self.service.get_package(
                    parts[1], as_of=q.get("as_of"))
            if len(parts) == 3 and parts[0] == "sessions" and parts[2] == "rules":
                return self.service.session_rules(parts[1], as_of=q.get("as_of"))
            if parts == ["certificates"]:
                st = self.service.get_state()
                return {"certificates": [
                    {"cert_id": c.cert_id, "status": c.status,
                     "revoked_at": c.revoked_at, "dispute_ids": c.dispute_ids,
                     "detail": c.detail}
                    for c in sorted(st.certificates.values(), key=lambda c: c.cert_id)]}
            if parts == ["arrangements"]:
                st = self.service.get_state()
                return {"arrangements": [
                    {"arrangement_id": a.arrangement_id, "zone": a.zone,
                     "session_id": a.session_id, "cert_refs": a.cert_refs,
                     "status": a.status, "frozen_for": a.frozen_for}
                    for a in sorted(st.arrangements.values(), key=lambda a: a.arrangement_id)]}
            if parts == ["disputes"]:
                st = self.service.get_state()
                return {"disputes": [
                    {"dispute_id": d.dispute_id, "event_id": d.event_id,
                     "status": d.status, "variants": d.variants}
                    for d in sorted(st.disputes.values(), key=lambda d: d.dispute_id)]}
            if parts == ["sessions"]:
                st = self.service.get_state()
                return {"sessions": [
                    {"session_id": s.session_id, "zone": s.zone, "sport": s.sport,
                     "stage": s.stage, "status": s.status,
                     "package_id": s.package_id,
                     "started_at": s.started_at, "finished_at": s.finished_at}
                    for s in sorted(st.sessions.values(), key=lambda s: s.session_id)]}
            raise NotFound(f"未知端点：/{'/'.join(parts)}")

        try:
            self._serve(route)
        except Exception as error:  # pragma: no cover - 兜底
            _json_response(self, 500, {"error": str(error)})

    def do_POST(self) -> None:
        parts = [p for p in urlsplit(self.path).path.split("/") if p]
        try:
            body = self._read_json()
        except DomainError as error:
            _json_response(self, 400, {"error": str(error)})
            return
        svc = self.service

        def route() -> Any:
            if parts == ["sports"]:
                e = svc.register_sport(body["sport"], name=body.get("name", ""))
                return {"event": e.to_dict()}
            if parts == ["packages"]:
                e = svc.create_package(
                    body["package_id"], body["sport"], body["stage"],
                    clauses=body.get("clauses"), version_no=body.get("version_no", 1),
                    parent_id=body.get("parent_id"))
                return {"event": e.to_dict()}
            if len(parts) == 3 and parts[0] == "packages" and parts[2] == "revisions":
                evs = svc.submit_revision(
                    parts[1], body["revision_id"],
                    submitted_by=body["submitted_by"],
                    submitter_role=body["submitter_role"],
                    change_class=body.get("change_class", "initial"),
                    summary=body.get("summary", ""), content=body.get("content"))
                return {"events": [e.to_dict() for e in evs]}
            if len(parts) == 3 and parts[0] == "revisions" and parts[2] == "sign":
                evs = svc.sign_revision(
                    parts[1], signer=body["signer"], role=body["role"])
                return {"events": [e.to_dict() for e in evs]}
            if len(parts) == 3 and parts[0] == "zones" and parts[2] == "locks":
                e = svc.lock_package(parts[1], body["package_id"])
                return {"event": e.to_dict()}
            if parts == ["sessions"]:
                e = svc.schedule_session(
                    body["session_id"], zone=body["zone"], sport=body["sport"],
                    stage=body["stage"], package_id=body.get("package_id"))
                return {"event": e.to_dict()}
            if len(parts) == 3 and parts[0] == "sessions" and parts[2] == "start":
                return {"event": svc.start_session(parts[1]).to_dict()}
            if len(parts) == 3 and parts[0] == "sessions" and parts[2] == "finish":
                return {"event": svc.finish_session(parts[1]).to_dict()}
            if len(parts) == 3 and parts[0] == "sessions" and parts[2] == "repin":
                return {"event": svc.repin_session(parts[1], body["package_id"]).to_dict()}
            if parts == ["certificates", "receipts"]:
                evs = svc.receive_certificate(
                    body["cert_id"], event_id=body["event_id"],
                    detail=body.get("detail"), occurred_at=body.get("occurred_at"))
                return {"events": [e.to_dict() for e in evs],
                        "deduplicated": len(evs) == 0}
            if len(parts) == 3 and parts[0] == "certificates" and parts[2] == "revoke":
                evs = svc.revoke_certificate(parts[1], reason=body.get("reason", ""))
                return {"events": [e.to_dict() for e in evs],
                        "frozen_arrangements":
                            [e.payload["arrangement_id"] for e in evs
                             if e.event_type == "VENUE_ARRANGEMENT_FROZEN"]}
            if parts == ["arrangements"]:
                e = svc.register_arrangement(
                    body["arrangement_id"], zone=body["zone"],
                    session_id=body["session_id"], cert_refs=body.get("cert_refs", []))
                return {"event": e.to_dict()}
            if len(parts) == 3 and parts[0] == "disputes" and parts[2] == "resolve":
                return {"event": svc.resolve_dispute(
                    parts[1], resolution=body["resolution"]).to_dict()}
            raise NotFound(f"未知端点：/{'/'.join(parts)}")

        self._serve(route)


def build_server(db_path: str, *, host: str = "127.0.0.1", port: int = 0) -> ThreadingHTTPServer:
    store = SqliteEventStore(db_path)
    server = ThreadingHTTPServer((host, port), _Handler)
    server.service = RuleCertificationService(store)  # type: ignore[attr-defined]
    server.daemon_threads = True
    return server


def serve(db_path: str, *, host: str = "127.0.0.1", port: int = 8080) -> None:  # pragma: no cover
    server = build_server(db_path, host=host, port=port)
    print(f"规则认证库监听 http://{host}:{server.server_address[1]}（数据库 {db_path}）")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def _main() -> int:  # pragma: no cover
    import sys
    if not 2 <= len(sys.argv) <= 4:
        print("用法：python3 -m src.api <sqlite路径| :memory:> [host] [port]", file=sys.stderr)
        return 2
    db_path = sys.argv[1]
    host = sys.argv[2] if len(sys.argv) >= 3 else "127.0.0.1"
    port = int(sys.argv[3]) if len(sys.argv) >= 4 else 8080
    serve(db_path, host=host, port=port)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
