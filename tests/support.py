"""测试共享夹具：真实线程 HTTP 服务 + 临时 SQLite。"""

from __future__ import annotations

import json
import tempfile
import threading
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path

from src.api import build_server


def request(base: str, method: str, path: str, body: dict | None = None):
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json; charset=utf-8"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8"))


@contextmanager
def server_harness(*, cleanup: bool = True):
    import shutil
    tmp_dir = tempfile.mkdtemp()
    db_path = str(Path(tmp_dir) / "events.sqlite")
    httpd = build_server(db_path, host="127.0.0.1", port=0)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{httpd.server_address[1]}"
    try:
        yield base, db_path, httpd.service
    finally:
        httpd.shutdown()
        httpd.server_close()
        if cleanup:
            shutil.rmtree(tmp_dir, ignore_errors=True)


def publish_package(base: str, *, sport: str = "teqball", stage: str = "qualification",
                    package_id: str = "teq-v1", clauses: dict | None = None,
                    submitter: str = "alice",
                    signers=("bob", "carol", "dave"),
                    zones: tuple[str, ...] = ("north",)) -> None:
    """走完整流程：登记项目 → 建包 → 三权签署发布 → 赛区锁定。"""
    request(base, "POST", "/sports", {"sport": sport, "name": sport})
    request(base, "POST", "/packages",
            {"package_id": package_id, "sport": sport, "stage": stage,
             "clauses": clauses or {"touch_limit": 3}})
    request(base, "POST", f"/packages/{package_id}/revisions",
            {"revision_id": "rev-1", "submitted_by": submitter,
             "submitter_role": "technical", "change_class": "initial",
             "summary": "首版", "content": clauses or {"touch_limit": 3}})
    for signer, role in zip(signers, ("technical", "medical_safety", "competition_ops")):
        status, _ = request(base, "POST", "/revisions/rev-1/sign",
                            {"signer": signer, "role": role})
        assert status == 200
    for zone in zones:
        status, _ = request(base, "POST", f"/zones/{zone}/locks",
                            {"package_id": package_id})
        assert status == 200
