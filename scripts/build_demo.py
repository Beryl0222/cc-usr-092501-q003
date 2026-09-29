"""生成 data/demo_events.jsonl：

走真实命令服务构造事件，再按固定随机种子打乱送达顺序、插入重复行与
同编号异内容回执，模拟离线送达的乱序与重复。
"""

from __future__ import annotations

import json
import random
from pathlib import Path

from src.replay import canonical_json
from src.service import RuleCertService
from src.store import EventStore

OUT = Path(__file__).resolve().parents[1] / "data" / "demo_events.jsonl"


def t(day: int, hour: int = 10) -> str:
    return f"2026-09-{day:02d}T{hour:02d}:00:00+08:00"


def main() -> None:
    store = EventStore(":memory:")
    svc = RuleCertService(store)

    # 项目、阶段、裁判等级
    svc.register_sport("tecqball", "台克球", t(1))
    svc.register_sport("padel", "板式网球", t(1))
    svc.register_sport("mma", "综合格斗", t(1))
    svc.define_stage("finals", "总决赛", t(1))
    svc.grant_judge_level("judge-07", "tecqball", "international", t(2))

    # 台克球规则包 r1：触球限制
    svc.draft_package("tecq-rules", "tecqball", "finals", "台克球总决赛规则包", t(2))
    svc.submit_revision(
        "tecq-rules", "editor-li", "台克球规则 r1", "每回合最多三次触球",
        {"touch_limit": 3, "double_touch": "forbidden"},
        at=t(2),
    )
    svc.sign_revision("tecq-rules", "technical", "tech-wang", t(3, 9))
    svc.sign_revision("tecq-rules", "medical_safety", "med-chen", t(3, 10))
    svc.sign_revision("tecq-rules", "competition_operations", "ops-zhao", t(3, 11))
    svc.add_interpretation_case(
        "case-touch-01", "tecq-rules", "touch_limit",
        "拦网触球计入三次触球限制", t(3, 14),
    )
    svc.add_local_supplement(
        "supp-bj-01", "tecq-rules", "beijing-zone",
        "热身场地同样适用触球限制计数演练要求", effective_from=t(4), at=t(3, 15),
    )

    # 赛区锁定 r1
    svc.lock_venue("zone-a", "北京赛区", "tecq-rules", t(4))

    # 场次：101 未开始、102 已开始、103 不引用该证书
    svc.declare_fixture("fx-101", "zone-a", "tecq-rules", t(20, 19), ["cert-tecq-table"], t(5))
    svc.declare_fixture("fx-102", "zone-a", "tecq-rules", t(6, 19), ["cert-tecq-table"], t(5))
    svc.declare_fixture("fx-103", "zone-a", "tecq-rules", t(21, 19), ["cert-padel-glass"], t(5))

    # 离线回执：首收、重复（同内容）、争议（同编号异内容）
    svc.receive_certificate_receipt(
        "cert-tecq-table", "tecq-rules", "tecq-table", "R-1001", "hash-aaa", t(5, 12))
    svc.receive_certificate_receipt(
        "cert-tecq-table", "tecq-rules", "tecq-table", "R-1001", "hash-aaa", t(5, 18))
    svc.receive_certificate_receipt(
        "cert-padel-glass", "tecq-rules", "padel-glass", "R-2002", "hash-bbb", t(5, 12))
    svc.receive_certificate_receipt(
        "cert-padel-glass", "tecq-rules", "padel-glass", "R-2002", "hash-ccc", t(5, 20))

    # fx-102 开赛，此后判罚不被追改
    svc.start_fixture("fx-102", t(6, 18))

    # 锁定后非破坏性澄清：只追加引用，不动快照
    svc.append_clarification(
        "tecq-rules", "zone-a", "触球计数口径澄清",
        "拦网触球与救球触球合并计数，本澄清不改变计分与安全边界。",
        ["case-touch-01", "R-1001"], "ops-zhao", t(7),
    )

    # 勘误改变计分边界 → 必须形成 r2 新版本，列出受影响未开始场次
    svc.submit_revision(
        "tecq-rules", "editor-li", "台克球规则 r2",
        "拦网触球从第三次触球中剔除，改变计分判定",
        {"touch_limit": 3, "double_touch": "forbidden", "block_exempt": True},
        scoring_changed=True, at=t(10),
    )
    svc.sign_revision("tecq-rules", "technical", "tech-wang", t(11, 9))
    svc.sign_revision("tecq-rules", "medical_safety", "med-chen", t(11, 10))
    svc.sign_revision("tecq-rules", "competition_operations", "ops-zhao", t(11, 11))

    # 赛区重新锁定 r2
    svc.lock_venue("zone-a", "北京赛区", "tecq-rules", t(12))

    # 撤销球台证书：只冻结实际引用它的未开始场次（fx-101）
    svc.revoke_certificate("cert-tecq-table", "台面回弹系数抽检不合格", t(13))

    events = store.load_events()

    # 模拟离线乱序送达
    random.Random(42).shuffle(events)
    # 物理重复一行，模拟同一条回执被投递两次
    events.append(dict(events[0]))

    lines = [canonical_json(e) for e in events]
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"写入 {OUT}：{len(lines)} 行（含 1 行物理重复），唯一事件 {len(lines) - 1} 条")


if __name__ == "__main__":
    main()
