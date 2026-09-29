# 新兴赛项规则认证库

为综合运动会的新兴项目（台克球、板式网球、综合格斗等）维护**按适用期组织**的规则认证库：
项目、竞赛阶段、规则条款、器材认证、裁判等级、解释案例与本地补充规定全部以追加型
领域事件记录；规则包经技术、医疗安全、竞赛运营三方各自签署后生效；赛区锁定后
非破坏性澄清只能追加引用，改变资格/计分/安全边界的勘误必须形成新版本；离线送达的
认证回执乱序、重复、同编号异内容都有确定处理；器材证书撤销只冻结实际引用它的
未开始场次。只依赖 Python 3.11 标准库。

## 目录

- `contracts/domain.schema.json`：领域对象与事件名称约定。
- `data/sample.json`：信封校验示例事件。
- `data/demo_events.jsonl`：覆盖锁定、澄清、新版本、乱序回执、争议与撤销的演示事件流。
- `src/envelope.py`：事件信封校验（事件类型、聚合类型、版本、带时区时间）。
- `src/store.py`：SQLite 追加型事件存储（event_id 唯一、聚合版本唯一、事务批量提交）。
- `src/state.py`：事件流的纯函数折叠（当前状态与历史时点还原）。
- `src/service.py`：领域命令服务（签署回避、锁定、澄清、新版本、回执争议、撤销冻结）。
- `src/api.py`：规则查询与锁定用 HTTP API。
- `src/replay.py`：发布与撤销事件的确定性重放命令。
- `scripts/build_demo.py`：生成演示事件流（含乱序与重复行）。
- `tests/`：并发签署、历史还原、撤销范围、进程恢复等回归测试。

## 核心规则如何落地

- **三方签署，互不越权**：`technical`、`medical_safety`、`competition_operations`
  各签一次；签署人等于修订提交人一律拒绝（不能复核自己提交的修订）；第三方签署
  在同一事务内连带 `PACKAGE_PUBLISHED`（新版本另带 `PACKAGE_VERSION_SUPERSEDED`）。
- **按适用期组织**：每次发布记录 `effective_from`，旧版本记录 `effective_until`，
  查询带 `as_of` 即可还原任意时点的生效修订；赛区每次锁定保存不可变快照。
- **锁定后**：非破坏性澄清走 `CLARIFICATION_APPENDED`，只引用既有文件/回执，
  不改快照、不出版本；标注 `qualification_changed / scoring_changed / safety_changed`
  的修订才形成新版本，并在提交时列出受影响的**尚未开始**场次。
- **判罚不被追改**：场次开赛后状态为 `started`，新版本与证书撤销都不再作用于它；
  其当时锁定的快照与澄清可按开赛时点还原。
- **离线回执**：同编号同内容幂等重复；同编号异内容生成 `RECEIPT_DISPUTED`，
  保留先到内容、证书转入争议；重放层对送达顺序做确定性排序。
- **撤销影响范围**：`CERTIFICATE_REVOKED` 与 `FIXTURE_FROZEN` 同事务提交，
  冻结集合 = 引用该证书 ∩ 状态 scheduled ∩ 未被冻结；已开赛、未引用、已冻结的都不动。
- **并发与恢复**：命令在存储写锁内“重新折叠—判定—追加”，由
  `(aggregate_id, version)` 唯一约束兜底；批量事件单事务提交，进程重启后
  待审修订与其已有签署完整保留。

## HTTP API

启动：

```bash
python3 -m src.api --host 127.0.0.1 --port 8080 --db rule_cert.db
```

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/sports` `/stages` `/judges` `/interpretation-cases` `/local-supplements` | 登记项目、阶段、裁判等级、解释案例、本地补充 |
| POST | `/packages` | 起草规则包 |
| POST | `/packages/<id>/revisions` | 提交修订（首版或边界新版本，返回受影响场次） |
| POST | `/packages/<id>/signatures` | 职能方签署，三方签齐返回 `published` |
| POST | `/packages/<id>/clarifications` | 锁定后追加非破坏性澄清 |
| GET | `/packages/<id>?as_of=2026-09-08T10:00:00%2B08:00` | 规则查询/历史时点还原 |
| POST | `/venues/<venue_id>/lock` | 赛区锁定当前已发布修订（保存快照） |
| GET | `/venues/<venue_id>?as_of=...` | 查看锁定快照与当时可见的澄清 |
| POST | `/certificates/<id>/receipts` | 登记离线回执（accepted/duplicate/disputed） |
| POST | `/certificates/<id>/revoke` | 撤销证书，返回精确的冻结场次列表 |
| GET | `/certificates/<id>` | 证书状态、回执、引用与冻结范围 |
| POST | `/fixtures` `/fixtures/<id>/start` `/fixtures/<id>/unfreeze` | 场次登记/开赛/解冻 |
| GET | `/fixtures` | 场次、状态与冻结原因 |
| GET | `/health` | 健康检查 |

## 确定性重放

```bash
python3 -m src.replay data/demo_events.jsonl
```

重放器按 `(occurred_at, aggregate_id, version, event_id)` 排序，重复事件去重、
异内容列入 `disputes`，输出含 `fingerprint_sha256` 的状态报告；同一输入任意送达
顺序、任意进程多次运行，报告逐字节一致。加 `--db path.db` 可同时落入持久库。

## 本地运行

```bash
# 信封示例校验
python3 -m src.cli data/sample.json

# 重新生成演示事件流（固定种子打乱送达顺序并插入重复行）
python3 -m scripts.build_demo

# 确定性重放
python3 -m src.replay data/demo_events.jsonl

# 全部测试
python3 -m unittest discover -s tests

# 编译检查
python3 -m compileall -q src tests scripts
```
