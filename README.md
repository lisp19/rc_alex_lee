# API 通知投递系统

基于 [设计文档](docs/design.md) 的 Go 1.27.1 实现。MariaDB 为权威状态源，RabbitMQ 承担至少一次调度，Redis 提供非权威锁和配额。单一进程支持 `api,worker,outbox,recovery` 角色拆分。

## 当前交付状态

已完成代码开发、三个命令的构建与非运行时 review。本阶段未拉取容器镜像、未启动中间件、未执行单元测试或集成/故障/性能验收。构建所需 Go modules 已解析并锁定。

- [实现选择与设计差异](docs/implementation.md)
- [静态审查与后续验收](docs/review.md)
- [OpenAPI 3.1](docs/openapi.yaml)
- [运行及管理手册](docs/operations.md)

## 构建

```sh
make build
make check
```

产物：`bin/notifier`、`bin/notify-admin`、`bin/mock-target`。`make check` 只执行 `go vet`、模块校验和 diff 空白检查；不会启动系统。产物已被 Git 忽略。

## 主要模块

| 路径 | 职责 |
|---|---|
| `internal/api`, `application` | JWT 之后的受理、幂等、Batch、查询、Proxy、人工重试 |
| `internal/config`, `auth` | 完整不可变配置 revision、CEL 编译、JWT 公钥、Secret 热刷新 |
| `internal/repository/business` | Notification、Attempt、Outbox 的事务及租约 fencing |
| `internal/mq/rabbitmq`, `outbox` | Quorum/TTL/DLX 拓扑、确认发布、手动 ACK、重连 |
| `internal/delivery`, `target`, `hook` | 投递编排、连接池、DNS/IP 白名单、幂等注入、CEL、脱敏 |
| `internal/retry`, `recovery` | 有界 Retry-After、Lease 与 MQ/Outbox 修复 |
| `internal/quota`, `lock` | Redis Lua 原子配额、并发租约和 compare-delete 锁 |
| `internal/observability`, `cmd/notifier` | JSON 日志、Health、角色启动和 graceful drain |
| `migrations`, `deploy`, `scripts` | 独立 Schema、部署模板、后续本地配置/故障工具 |

## API 概览

所有业务 API 使用 Bearer JWT。Client 仅能查询自己的任务，Target 必须在 Client binding 中。

```text
POST /api/v1/notifications
POST /api/v1/notifications:batch
GET  /api/v1/notifications/{id}
GET  /api/v1/notifications?status=failed&target=...&batch_id=...&cursor=...
POST /api/v1/notifications/{id}:retry
```

单条提交使用 `Idempotency-Key` Header；批次使用每个 item 的 `idempotency_key`。批次仅异步、逐项独立受理。相同 Client + Key + 规范化请求返回原任务；请求不同返回 409。

```json
{
  "target": "target-a",
  "delivery_mode": "async",
  "request": {
    "method": "POST",
    "path": "/a",
    "query": {"failures": "2"},
    "headers": {"Content-Type": "application/json"},
    "body_encoding": "utf8",
    "body": "{\"order_id\":\"demo-1\"}"
  }
}
```

只有数据库事务提交成功才返回受理成功。`failed` 表示本系统停止自动处理，不表示供应商一定未执行。所有重试保留原 Notification ID 和对外幂等键。
