# 新兴赛项规则认证库

新兴项目（台克球、板式网球、综合格斗等）首次进入综合运动会后，国际规则、器材规格与裁判解释的勘误可能在报名结束后才到达。本服务把**项目、竞赛阶段、规则条款、器材认证、裁判签署、解释案例与本地补充规定**按适用期组织为只追加的事件流，保证：

- 规则包必须经 **技术 / 医疗安全 / 竞赛运营**三方各自签署才生效，**签署人不能复核自己提交的修订**；
- 赛区锁定规则包后，非破坏性澄清只能**追加引用**；改变资格、计分或安全边界的勘误必须**形成新版本**，并列出受影响的**尚未开始场次**，已开始/结束场次的既有判罚不可追改；
- 离线送达的认证回执乱序或重复都安全：同编号同内容幂等丢弃，**同编号异内容进入争议**；
- 器材证书撤销**只冻结实际引用它的场地安排**。

系统只有事件日志这一份事实来源（SQLite），所有状态由事件流确定性折叠得到，因此支持时点还原与进程崩溃恢复。

## 目录

- `contracts/domain.schema.json`：领域事件名称与聚合类型约定。
- `src/events.py`：事件信封、规范化内容哈希、签署角色常量。
- `src/store.py`：事件存储（`SqliteEventStore` 持久化 / `InMemoryEventStore` 测试用）。
- `src/domain.py`：领域内核（纯函数 `fold` + 命令服务，全部并发命令在单把锁内“折叠→校验→追加”）。
- `src/api.py`：规则查询与锁定的 HTTP API。
- `src/replay.py`：确定性重放一组发布与撤销事件的命令入口。
- `src/envelope.py` / `src/cli.py`：单事件信封校验。
- `data/sample.json`：单事件样例；`data/replay_sample.json`：含乱序与重复回执的重放批次。
- `tests/`：四类性质的证明测试。

## 运行

只依赖 Python 3.11+ 标准库。

```bash
# 启动 HTTP 服务
python3 -m src.api ./data/rulecert.sqlite 127.0.0.1 8080

# 确定性重放一批发布/撤销命令（乱序、重复安全）
python3 -m src.replay data/replay_sample.json

# 校验单事件信封
python3 -m src.cli data/sample.json

# 测试
python3 -m unittest discover -s tests
python3 -m compileall -q src tests
```

## HTTP API

请求与响应均为 JSON；违反领域规则返回 `409 {"error": ...}`，字段缺失 `400`，路由不存在 `404`。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/sports` | 登记项目 `{sport, name?}` |
| POST | `/packages` | 创建规则包版本 `{package_id, sport, stage, clauses?, parent_id?}` |
| GET | `/packages/{id}` | 查询规则包；`?as_of=<ISO8601>` 还原任意时点 |
| POST | `/packages/{id}/revisions` | 提交修订，`change_class` 为 `initial` / `clarification` / `boundary` |
| POST | `/revisions/{id}/sign` | 角色签署 `{signer, role}`，role ∈ technical / medical_safety / competition_ops |
| POST | `/zones/{zone}/locks` | 赛区锁定规则包 `{package_id}`（仅已生效包） |
| POST | `/sessions` | 排定场次 `{session_id, zone, sport, stage, package_id?}` |
| POST | `/sessions/{id}/start` / `/finish` | 开赛（规则随之固定）/ 完赛 |
| POST | `/sessions/{id}/repin` | 仅未开赛场次可改引新版本 `{package_id}` |
| GET | `/sessions/{id}/rules` | 还原场次当时固定的规则；`?as_of=` 显式指定时点 |
| POST | `/certificates/receipts` | 离线器材认证回执 `{cert_id, event_id, detail?, occurred_at?}` |
| POST | `/certificates/{id}/revoke` | 撤销证书，返回精确的 `frozen_arrangements` |
| POST | `/arrangements` | 登记场地安排 `{arrangement_id, zone, session_id, cert_refs[]}` |
| GET | `/arrangements` / `/certificates` / `/disputes` / `/sessions` | 状态查询 |
| POST | `/disputes/{id}/resolve` | 裁决争议 `{resolution}` |
| GET | `/events` | 不可变事件审计流，`?since_seq=n` 增量拉取 |

### 端到端示例

```bash
curl -s localhost:8080/sports -d '{"sport":"teqball","name":"台克球"}'
curl -s localhost:8080/packages -d '{"package_id":"teq-v1","sport":"teqball","stage":"qualification","clauses":{"touch_limit":3}}'
curl -s localhost:8080/packages/teq-v1/revisions -d '{"revision_id":"r1","submitted_by":"alice","submitter_role":"technical","change_class":"initial","summary":"首版","content":{"touch_limit":3}}'
curl -s localhost:8080/revisions/r1/sign -d '{"signer":"bob","role":"technical"}'
curl -s localhost:8080/revisions/r1/sign -d '{"signer":"carol","role":"medical_safety"}'
curl -s localhost:8080/revisions/r1/sign -d '{"signer":"dave","role":"competition_ops"}'   # → 自动 PACKAGE_PUBLISHED
curl -s localhost:8080/zones/north/locks -d '{"package_id":"teq-v1"}'
```

锁定后再提交 `change_class:"boundary"` 的勘误会自动产生继任版本（如 `teq-v2`）、`NEW_VERSION_DEMANDED` 与 `IMPACT_LISTED` 事件；`affected_sessions` 只含引用该版本谱系且尚未开赛的场次。`clarification` 则只在原包上追加引用，不动条款。

## 确定性重放

批次文件为 `{"commands":[{command_id, occurred_at, command, args}, ...]}`，`command` 即服务方法名。重放器：

1. 按 `(occurred_at, command_id)` 稳定排序——**文件内送达顺序不影响结果**；
2. 以该命令的 `occurred_at` 作为事件时钟逐条受理，重复回执返回 `deduplicated`、违反规则返回 `rejected`（不中断后续命令）；
3. 输出每条命令结果、最终状态摘要和整个事件流的 `event_stream_hash`（规范化 JSON 的 SHA-256）。

同一批命令无论怎样打乱，哈希一致；`--check <hash>` 可用于回归断言，`--db <path>` 写入 SQLite 并可跨进程续写（重复命令全部幂等）。

## 性质与对应测试

| 要证明的性质 | 测试 |
| --- | --- |
| 并发签署不会越权（同角色只一人成功、本人不得复核、同一人不得签两个角色、只发布一次） | `tests/test_concurrent_signoff.py` |
| 旧赛程仍能还原当时规则（开赛后澄清/新版本不入旧场次、影响清单只列未开赛场次） | `tests/test_historical_rules.py` |
| 撤销影响范围准确（只冻实际引用方、并发重复回执幂等、同号异内容进争议） | `tests/test_revocation_scope.py` |
| 进程恢复后待审修订/未裁决争议不丢失，事件流重建状态逐字段一致 | `tests/test_recovery.py` |
| 重放确定性、越权拒绝、SQLite 续写幂等、CLI 哈希校验 | `tests/test_replay.py` |

## 领域约定

事件标识一旦接收不得原地复用为另一份内容；版本为正整数；时间采用带时区的 ISO 8601。业务修订通过新事件表达，原始记录继续用于追溯。
