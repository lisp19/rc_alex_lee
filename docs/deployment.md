# 双实例部署与端到端验收报告

## 1. 本轮交付

已实际完成部署文件/初始化/健康检查检查、依赖镜像拉取、应用镜像构建、MariaDB/Redis/RabbitMQ 初始化、两个 Notifier 实例启动、业务场景配置和真实端到端验收。测试结束后服务继续保留运行。

环境：Linux amd64，Docker Engine 29.8.0、Compose v5.5.1、Go 1.27.1。原始运行记录使用主机 UTC 时间，基础冒烟窗口为 `2026-09-12T16:54:33Z` 至 `16:54:55Z`；随后执行恢复检查，于 `16:58Z` 完成。这里记录实际环境时间，不更改主机时钟。

| 组件 | 实际版本/配置 |
|---|---|
| MariaDB Community | `11.8.9-MariaDB-ubu2404`，由设计允许的 `mariadb:11.8` 标签解析 |
| Redis | `8.10.1`，仅内部网络，非权威短期数据 |
| RabbitMQ | `4.3.5-management`，9 个 durable Quorum Queue，7 个固定 TTL/DLX Bucket |
| Notifier | 同一 `notifier:local` 镜像，两个独立容器，均启用 api/worker/outbox/recovery |
| 运行用户 | distroless nonroot；Notifier 只读 RootFS、drop ALL capabilities |
| 健康探针 | 原生 Go healthcheck；各依赖具备 Docker healthcheck 和启动依赖条件 |

这是本地单节点中间件 + 双应用实例验收，不是生产三副本 RabbitMQ/MariaDB HA 验收。

## 2. 初始化核验

- `001_business.sql` 实际创建 Notification、Batch、Attempt、Outbox 四张业务表及唯一/调度/租约索引。
- `002_control.sql` 实际创建 revision/snapshot 两张管理表和 global 初始记录。
- `deploy/local/init-users.sh` 在 MariaDB 临时初始化进程中创建独立账户并授权。
- 实测 runtime control 账户执行 UPDATE 被拒绝；runtime business 账户访问管理库被拒绝。
- `config-init` 独立管理作业退出码为 0；业务配置经事务生效，两实例通过 polling 收敛到相同 revision。
- RabbitMQ 拓扑由运行进程幂等声明；实测两个 Consumer 都要求手动 ACK，各 Delay Queue 的 TTL、DLX、at-least-once dead-letter 配置正确。
- Redis 不需要数据库 Schema；Lua 原子限流、并发 token、SET NX 锁和 TTL 由应用按需使用，已通过真实配额/故障场景验证。

## 3. 代表性业务上下文与基础冒烟

基础冒烟 **20/20 通过**。使用唯一批次前缀区分每次验收，不清空已有数据。

| 业务/能力 | 验证结果 |
|---|---|
| 库存预占同步 A | 持久化后异步投递，另一实例查询可见；无 Hook 的 Attempt 正常返回 |
| 双实例幂等竞争 | 8 个并发相同请求得到同一 Notification；数据库和 Mock 均只有一次逻辑首次执行 |
| 幂等键冲突 | 修改正文后重用相同 Key 返回 409 |
| 身份与隔离 | 缺少 JWT/错误 audience 返回 401；其他 Client 查任务 404；无人工重试权限返回 403 |
| Target 路径 | 完整 URL、路径穿越拒绝 |
| 非原子批次 | 合法项成功、非法 Target 独立失败；batch_id 关联查询正确 |
| 库存服务 5xx | 首次 500，约 5.061s 后重试成功，两个完整 Attempt |
| CRM 业务码 B | HTTP 200/code=1001 由 CEL 判为 retry，约 5.046s 后 code=0 成功，report 正确 |
| 广告接口 D | 429 + Retry-After=5，约 5.043s 后重试，不早于允许时间 |
| 对外幂等注入 | Header 与 JSON Pointer 的 key 在每次尝试中保持一致 |
| CRM 终止与人工重试 | code=2000 进入 failed；有权限人工重试推进 generation、保留 ID/key、累计 Attempt 递增 |
| 非幂等结算 C | 连接 reset 后 `uncertain_retry_disabled`，只产生一次尝试 |
| Proxy | 先持久化，最终成功且只执行一次 |
| 入口共享 Quota | 两实例同 Client 并发不同 Key，得到一次 202 和一次 429 |
| 出口共享 Quota | 每秒一条限制下三项均完成；等待配额不消耗 Attempt |
| Cursor 分页 | 跨实例读取两页无重复 |
| 单实例退出 | 第二实例优雅退出期间，第一实例完成新任务；重启后恢复健康 |
| 实际 Worker 参与 | 两个容器日志分别有成功投递记录，不仅是两个 API 进程存活 |
| 数据库收尾 | 无重复逻辑任务、无未结束 Attempt |

表格聚合了脚本内的 20 个检查节点，完整原始断言和结果在 `scripts/smoke.py` 与忽略目录 `.runtime/smoke-results.json`。

## 4. 恢复冒烟

恢复检查 **3/3 通过**：

1. **停止 Redis**：两实例报告 Redis 不可用/Quota degraded；fail-open 下仍 Ready，并通过数据库租约完成投递。重新启动后恢复正常。
2. **停止 RabbitMQ**：两实例 readiness 503；直接访问 API 仍可事务受理，任务保持 pending、Outbox 存在。启动 RabbitMQ 后自动重连、恢复消费，任务成功。
3. **HTTP 执行中 kill -9 Worker**：通过 worker_active 找到实际持有者并强制终止；重新启动后等待数据库 Lease 过期。旧 Attempt 标记 `unknown/lease_expired`，新 generation 投递成功，总计两次尝试，没有旧结果覆盖。

所有临时停止的依赖/实例均恢复运行。故障窗口内的 MQ consume/publish 错误日志为预期现象；恢复后未观察到持续错误。

## 5. 本轮修复与脚本补全

- **运行时缺陷**：无 Hook 的 `hook_result_json=NULL` 直接扫描到 `json.RawMessage` 导致查询 503。改为先扫描 `[]byte` 再转换；完整基础冒烟重跑通过。
- **本地端口冲突**：原 Mock 18080 被主机已有进程占用；默认改为可配置的 18090，未停止或修改已有进程。
- **探针缺失**：增加 distroless 可直接执行的健康检查命令，补全 Redis/RabbitMQ/Mock/两个应用实例的健康依赖顺序。
- **配置收敛**：业务 Seed 等待两个实例达到目标 revision 后再开始验收。
- **Mock 可观测性**：提供仅用于本地测试的请求记录接口，核对外部调用次数与幂等注入；请求读取不占用全局记录锁。
- **产物隔离**：增加 `.runtime/`、环境文件变体和 Python 缓存忽略规则，并同步排除 Docker 构建上下文。

## 6. 复现和检查入口

```sh
sh scripts/deploy-local.sh
python3 scripts/seed-smoke.py
python3 scripts/smoke.py
python3 scripts/recovery-smoke.py
python3 scripts/health.py
python3 scripts/collect-runtime.py
```

| 地址 | 用途 |
|---|---|
| `http://127.0.0.1:8080`、`:8082` | 两实例业务 API，需 JWT |
| `http://127.0.0.1:8081/health/detail`、`:8083/health/detail` | 实例/依赖/配置状态 |
| `http://127.0.0.1:18090` | Mock Target，含本地验收记录端点 |
| `http://127.0.0.1:15672` | RabbitMQ 管理，用户名 notifier，密码在忽略的 `.env` |

原始日志、任务标识、实例身份、镜像 digest、失败尝试证据保存在 `.runtime/`；凭证和实际配置保存在 `.env` / `configs/secrets/`。提交到 Git 的是通用脚本、模板、SQL、修复和本脱敏汇总。

尚未执行：生产 Kubernetes 部署、TLS 网关集成、多副本中间件 HA、容量/长时间压测、完整故障矩阵。它们不影响本轮本地双实例部署及 23 项已完成检查的结论。
