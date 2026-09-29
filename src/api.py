"""规则查询与锁定用的 HTTP API（仅依赖标准库）。

端点一览：

    POST   /sports                                 登记项目
    POST   /stages                                 定义竞赛阶段
    POST   /judges                                 授予裁判等级
    POST   /interpretation-cases                   记录解释案例
    POST   /local-supplements                      追加本地补充规定
    POST   /packages                               起草规则包
    POST   /packages/<id>/revisions                提交修订（首版/边界新版本）
    POST   /packages/<id>/signatures               职能方签署（三方签齐即发布）
    POST   /packages/<id>/clarifications           锁定后追加非破坏性澄清
    POST   /venues/<venue_id>/lock                 赛区锁定当前已发布修订
    GET    /venues/<venue_id>?as_of=               查看锁定快照（可按历史时点还原）
    POST   /certificates/<id>/receipts             登记离线送达回执（幂等/争议）
    POST   /certificates/<id>/revoke               撤销证书并冻结引用场次
    GET    /certificates/<id>                      查询证书与撤销影响范围
    POST   /fixtures                               登记场次
    POST   /fixtures/<id>/start                    开赛（此后判罚不受追改）
    POST   /fixtures/<id>/unfreeze                 解除冻结
    GET    /fixtures                               列出场次与冻结原因
    GET    /packages/<id>?as_of=                   规则查询（可按历史时点还原）
    GET    /health
"""

from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .service import ConcurrentUpdate, DomainError, RuleCertService
from .store import EventConflict, EventStore

NOT_FOUND_HINTS = ("不存在", "从未", "尚未锁定任何")


class _Handler(BaseHTTPRequestHandler):
    server_version = "RuleCert/0.1"

    # ---- 框架辅助 -------------------------------------------------------

    def _send(self, status: int, payload: object) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise DomainError(f"请求体不是合法 JSON：{error}")
        if not isinstance(body, dict):
            raise DomainError("请求体必须是 JSON 对象")
        return body

    @staticmethod
    def _require(body: dict, names: tuple[str, ...]) -> None:
        missing = [n for n in names if n not in body]
        if missing:
            raise DomainError(f"缺少字段：{', '.join(missing)}")

    def log_message(self, fmt: str, *args) -> None:  # 测试输出保持安静
        return

    # ---- 路由 -----------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def _dispatch(self, method: str) -> None:
        parts = urlsplit(self.path)
        query = {k: v[0] for k, v in parse_qs(parts.query).items()}
        path = parts.path.rstrip("/") or "/"
        svc: RuleCertService = self.server.service  # type: ignore[attr-defined]
        try:
            body = self._read_body() if method == "POST" else {}
            self._route(svc, method, path, body, query)
        except DomainError as error:
            status = 404 if any(h in str(error) for h in NOT_FOUND_HINTS) else 400
            self._send(status, {"error": str(error)})
        except (ConcurrentUpdate, EventConflict) as error:
            self._send(409, {"error": str(error), "type": type(error).__name__})

    def _route(self, svc: RuleCertService, method: str, path: str,
               body: dict, query: dict) -> None:
        as_of = query.get("as_of")

        if method == "GET" and path == "/health":
            return self._send(200, {"status": "ok"})

        if method == "POST" and path == "/sports":
            self._require(body, ("code", "name"))
            return self._send(201, svc.register_sport(body["code"], body["name"], body.get("at")))
        if method == "POST" and path == "/stages":
            self._require(body, ("stage_id", "name"))
            return self._send(201, svc.define_stage(body["stage_id"], body["name"], body.get("at")))
        if method == "POST" and path == "/judges":
            self._require(body, ("judge_id", "sport_code", "level"))
            return self._send(201, svc.grant_judge_level(
                body["judge_id"], body["sport_code"], body["level"], body.get("at")))
        if method == "POST" and path == "/interpretation-cases":
            self._require(body, ("case_id", "package_id", "clause_ref", "ruling"))
            return self._send(201, svc.add_interpretation_case(
                body["case_id"], body["package_id"], body["clause_ref"],
                body["ruling"], body.get("at")))
        if method == "POST" and path == "/local-supplements":
            self._require(body, ("supplement_id", "package_id", "scope", "text"))
            return self._send(201, svc.add_local_supplement(
                body["supplement_id"], body["package_id"], body["scope"],
                body["text"], body.get("effective_from"), body.get("at")))

        if method == "POST" and path == "/packages":
            self._require(body, ("package_id", "sport_code", "stage_id", "title"))
            return self._send(201, svc.draft_package(
                body["package_id"], body["sport_code"], body["stage_id"],
                body["title"], body.get("at")))

        match = re.fullmatch(r"/packages/([^/]+)/revisions", path)
        if method == "POST" and match:
            pid = match.group(1)
            self._require(body, ("submitted_by", "title", "summary", "contents"))
            return self._send(201, svc.submit_revision(
                pid, body["submitted_by"], body["title"], body["summary"], body["contents"],
                qualification_changed=body.get("qualification_changed", False),
                scoring_changed=body.get("scoring_changed", False),
                safety_changed=body.get("safety_changed", False),
                at=body.get("at")))

        match = re.fullmatch(r"/packages/([^/]+)/signatures", path)
        if method == "POST" and match:
            pid = match.group(1)
            self._require(body, ("role", "signer"))
            return self._send(200, svc.sign_revision(
                pid, body["role"], body["signer"], body.get("at")))

        match = re.fullmatch(r"/packages/([^/]+)/clarifications", path)
        if method == "POST" and match:
            pid = match.group(1)
            self._require(body, ("venue_id", "title", "text", "references", "author"))
            return self._send(201, svc.append_clarification(
                pid, body["venue_id"], body["title"], body["text"],
                body["references"], body["author"], body.get("at")))

        match = re.fullmatch(r"/packages/([^/]+)", path)
        if method == "GET" and match:
            return self._send(200, svc.get_package(match.group(1), as_of))

        match = re.fullmatch(r"/venues/([^/]+)/lock", path)
        if method == "POST" and match:
            self._require(body, ("package_id",))
            return self._send(201, svc.lock_venue(
                match.group(1), body.get("name", match.group(1)),
                body["package_id"], body.get("at")))

        match = re.fullmatch(r"/venues/([^/]+)", path)
        if method == "GET" and match:
            return self._send(200, svc.get_venue(match.group(1), as_of))

        match = re.fullmatch(r"/certificates/([^/]+)/receipts", path)
        if method == "POST" and match:
            self._require(body, ("package_id", "equipment_code", "receipt_id", "content_hash"))
            return self._send(200, svc.receive_certificate_receipt(
                match.group(1), body["package_id"], body["equipment_code"],
                body["receipt_id"], body["content_hash"], body.get("at")))

        match = re.fullmatch(r"/certificates/([^/]+)/revoke", path)
        if method == "POST" and match:
            self._require(body, ("reason",))
            return self._send(200, svc.revoke_certificate(
                match.group(1), body["reason"], body.get("at")))

        match = re.fullmatch(r"/certificates/([^/]+)", path)
        if method == "GET" and match:
            return self._send(200, svc.get_certificate(match.group(1)))

        if method == "POST" and path == "/fixtures":
            self._require(body, ("fixture_id", "venue_id", "package_id",
                                 "scheduled_start", "cert_refs"))
            return self._send(201, svc.declare_fixture(
                body["fixture_id"], body["venue_id"], body["package_id"],
                body["scheduled_start"], body["cert_refs"], body.get("at")))

        match = re.fullmatch(r"/fixtures/([^/]+)/start", path)
        if method == "POST" and match:
            return self._send(200, svc.start_fixture(match.group(1), body.get("at")))

        match = re.fullmatch(r"/fixtures/([^/]+)/unfreeze", path)
        if method == "POST" and match:
            self._require(body, ("cert_id",))
            return self._send(200, svc.unfreeze_fixture(
                match.group(1), body["cert_id"], body.get("at")))

        if method == "GET" and path == "/fixtures":
            return self._send(200, {"fixtures": svc.list_fixtures()})

        self._send(404, {"error": f"未找到端点：{method} {path}"})


def build_server(host: str, port: int, store: EventStore) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), _Handler)
    server.service = RuleCertService(store)  # type: ignore[attr-defined]
    return server


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="新兴赛项规则认证库 HTTP 服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--db", default="rule_cert.db", help="SQLite 路径，:memory: 为内存库")
    args = parser.parse_args(argv)

    store = EventStore(args.db)
    server = build_server(args.host, args.port, store)
    print(f"规则认证库监听 http://{args.host}:{args.port}（db={args.db}）")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
