# 非运行时审查报告

审查范围：本仓库实现、SQL、配置、部署模板、脚本与 API 契约。方法：人工静态代码路径审查、Go 编译与 vet、依赖校验、文件语法/引用检查。没有启动应用或中间件；没有执行单元、集成、故障或性能测试。

## 1. 静态检查结果

| 检查 | 结果 |
|---|---|
| Go 工具链 | `go1.27.1 linux/amd64` |
| `make build` | notifier、notify-admin、mock-target 三个命令构建通过，CGO_ENABLED=0 |
| `go vet ./...` | 通过 |
| `go mod verify` | `all modules verified` |
| gofmt / `git diff --check` | 通过 |
| 四个 shell 脚本 `sh -n` | 通过 |
| 两个 JSON 配置语法 | 通过 |
| Compose、Kubernetes、OpenAPI YAML | 语法解析通过 |
| OpenAPI 本地 `$ref` | 25 个引用均可解析 |

YAML 检查使用环境已有 PyYAML 6.0.3，未新增运行时依赖。这只是语法和引用检查，不代表 Kubernetes API Server、Compose 或 OpenAPI 全规格验证器已经验收。

## 2. 核心链路审查

| 链路 | 代码定位 | 静态结论 |
|---|---|---|
| 受理持久化 | `application/service.go`、`repository/business/store.go` | Notification 与首次 Outbox 同事务；Commit 失败不返回成功 |
| 并发幂等 | `Store.Accept` | 唯一索引兜底；冲突后读取原任务并比较 SHA-256；Header/正文编码规范化 |
| Batch | `api/server.go` | envelope 限额；每项独立解析/事务；相同 Key 冲突不回滚其他 item |
| 客户隔离 | `auth/jwt.go`、`Store.Get/List` | JWT 显式算法/issuer/audience/exp/sub/client；查询包含 client 条件 |
| Outbox | `business/outbox.go`、`outbox/dispatcher.go` | 短事务领取、事务外发布、mandatory + Confirm、token/lease 条件更新 |
| MQ | `mq/rabbitmq` | durable direct exchange、quorum、persistent、Manual ACK、非法消息 reject/DLX、重连重建拓扑 |
| Claim/Finish | `business/delivery.go` | SQL 锁不跨 HTTP；CAS 依赖状态/generation/token/有效 lease；Attempt 与最终状态/重试 Outbox 同事务 |
| Retry | `retry/policy.go` | 次数、deadline、Retry-After 与固定 Bucket 同时约束；不确定结果受显式许可限制 |
| Recovery | `business/recovery.go`、`recovery/loop.go` | expired lease fencing；unknown Attempt；Outbox lease 回收；published 消息丢失也能补发 |
| 人工重试 | `application/retry.go` | 客户权限、归属、failed 状态、安全配置、历史请求/config；generation CAS 防并发重复操作 |
| 配置 | `config/manager.go`、`cmd/notify-admin` | immutable snapshot；revision 同事务推进；LKG；退回版本拒绝；当前快照不依赖历史缓存命中 |
| SSRF | `target/http.go` | 固定 Origin；相对路径限制；直接 dial 经校验 IP；禁用代理/redirect；安全 revision 改变更换连接池 |
| Hook | `config/validate.go`、`hook/evaluate.go` | 限制表达式长度、递归深度、AST 节点、cost、deadline；结构化输出；不能覆盖 transport/security failure |
| Secret/脱敏 | `config/manager.go`、`target.Redact` | Secret 只在内存注入；请求认证 Header 不落库；预览及 report 脱敏；API 不返回请求体/敏感 Header |
| Quota/Lock | `quota`、`lock` | Redis TIME + Lua 原子限额；本地并发兜底；token compare-delete；崩溃后 TTL 回收 |
| Proxy | `api.submit`、`delivery.Worker` | 先持久化；复用相同 Claim/Attempt/Retry；与 MQ 竞争时仅有效 claimant 执行，无法取得第一次尝试时返回 202 |
| Shutdown/Health | `cmd/notifier`、`observability` | 先 Not Ready、停止新请求与消费，等待提交/ACK；管理库故障不丢弃 LKG；Health 不等 MQ reconnect mutex |

## 3. 本轮发现并修正的问题

1. **CEL module 路径不匹配**：v0.30.0 仍声明 `github.com/google/cel-go`，修正 go.mod 路径并保持版本。
2. **已 Confirm 的 MQ 消息丢失无法恢复**：对 published 记录加入确认宽限窗口，过期 pending 推进 generation 补发。
3. **过期租约的旧 Worker 可抢先写结果**：Finish 不仅匹配 token，还检查数据库 lease_until；即使 Recovery 尚未运行，也拒绝过期结果。
4. **崩溃后非幂等任务盲目重试**：Lease Recovery 纳入相同次数/deadline/uncertain-retry 规则。
5. **失联连接导致发布/关闭无限等待**：AMQP dial/handshake、channel setup、socket write、publish deadline 和 connection close 添加界限。
6. **健康检查被重连锁阻塞**：MQ 连接状态改用原子观测指针；各依赖探测并行，管理库超时不会抢占 Redis 探测预算。
7. **Outbox-only 空队列实例从不连接 MQ**：增加角色级连接维护 loop，空队列也初始化拓扑并在断线后恢复。
8. **文件监听失败同时终止 DB Watcher**：监听失效只关闭事件通道，保留定时 Secret 内容校验与 DB revision polling。
9. **当前配置从历史缓存逐出后依赖管理库**：History 优先返回当前 active snapshot，保持已加载配置在管理库故障中的可用性。
10. **不同 policy 的 global quota 含义冲突**：配置校验要求各 policy 全局上限一致。
11. **JSON Pointer 不支持数组**：补充现存数组索引、尾部追加与 RFC 6901 escape 校验。
12. **非结构化响应及根字符串可能泄漏 Secret**：无法解析的预览不保存，根字符串/数组/report 也执行 Secret 替换。
13. **全部非法批次绕过受理额度创建元数据**：Batch envelope 单独计一次额度，合法新 item 再逐个计费。
14. **配额持续不足使过期任务无限 pending**：deadline 到期时跳过配额等待，进入有 fenced Attempt 的终止路径。

静态审查发现的上述问题均已在当前代码中修正。静态分析不能确认跨组件运行语义；下面的运行验收仍是交付后的必要验证步骤。

## 4. 待后续环境验收

全部为**未执行**，与本阶段完成状态分开记录：

- MariaDB 11.8 实际 DDL/JSON/UTC/事务/`SKIP LOCKED` 兼容性；运行账户授权与隔离。
- RabbitMQ 4.3.5 Quorum 参数、at-least-once TTL dead-letter、mandatory return/Confirm 顺序、重连、broker flow control。
- CEL 对真实 JSON 数值/缺字段/巨大响应的判定；实际 runtime cost/deadline 触发；敏感字段脱敏。
- Redis 8.10.1 Lua 返回类型、各维度额度与 TTL、并发 token 回收、fail-open/closed 故障行为。
- 单条/重复/409/批次/分页/跨 Client 查询/Proxy/人工重试 API 闭环。
- A/B/C/D Mock 场景、Retry-After 秒数与日期、HTTP timeout/reset、DNS/IP/redirect SSRF。
- publish confirm 后进程退出、HTTP 中 kill -9、MQ 消息丢失、Redis/DB 中断与恢复。
- ConfigMap/Secret 符号链接切换、非法配置 LKG、连续 revision 更新、Target/Client 禁用与凭证切换。
- SIGTERM drain、未 ACK 重投、滚动升级和 Pod 漂移。
- 真实负载下 QPS、延迟、DB 锁等待、MQ backlog、goroutine/连接/内存、资源与保留策略。

## 5. Review 导航

优先看 `internal/repository/business/{delivery,recovery,outbox}.go` 的事务条件，再看 `internal/delivery/worker.go` 和 `internal/target/http.go`，最后核对 `docs/implementation.md` 的实现选择与 `docs/operations.md` 的部署/管理契约。
