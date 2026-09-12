# 运行与管理手册

以下启动和故障命令留待本轮 review 后使用，本阶段未执行。

## 1. 本地准备与启动

要求 Go 1.27.1、OpenSSL、Python 3、Docker Compose。在仓库根目录：

```sh
make build
sh scripts/local-setup.sh
docker compose --env-file .env -f deploy/compose.yaml up -d --build
TOKEN=$(sh scripts/dev-token.sh)
```

`local-setup` 只生成被忽略的 `.env` 和 `configs/secrets/`，不下载依赖。包含随机数据库/MQ 密码、开发 JWT 私钥和带公钥的管理配置。重复执行会拒绝覆盖。示例中的 `issuers: {}` 是默认拒绝所有 JWT 的模板；生成后才具有本地签发公钥。

Compose 在全新数据库卷中初始化 Schema 和账户，然后通过独立 `config-init` 作业写管理配置。运行进程只有管理库 SELECT 权限。生产迁移不能依赖应用启动或 Compose init，必须使用独立 Job/DBA 操作。

访问：API `127.0.0.1:8080`、内部 Health `127.0.0.1:8081`、Mock `127.0.0.1:18080`、RabbitMQ 管理 `127.0.0.1:15672`。数据库和 Redis 不发布主机端口。

```sh
curl -i http://127.0.0.1:8080/api/v1/notifications \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Idempotency-Key: demo-order-1' \
  -H 'Content-Type: application/json' \
  --data '{"target":"target-a","request":{"method":"POST","path":"/a","query":{"failures":"2"},"body":"{}"}}'
```

## 2. 进程配置

| 输入 | 用途 |
|---|---|
| `NOTIFIER_CONFIG` | JSON 进程文件；默认 `configs/notifier.example.json` |
| `NOTIFIER_ROLES` | 逗号分隔角色；覆盖文件的 roles |
| `NOTIFIER_BUSINESS_DSN` | 业务库 DML 账户 |
| `NOTIFIER_CONTROL_DSN` | 管理库只读账户 |
| `NOTIFIER_AMQP_URL` | AMQP/AMQPS；worker/outbox 角色必需 |
| `NOTIFIER_REDIS_URL` | Redis/rediss URL，托管 HA endpoint |
| `NOTIFIER_ADMIN_DSN` | 仅管理 CLI 使用的写账户 |

连接池启用 UTC、parseTime、连接/读写超时和连接生命周期。API TLS 由企业 Ingress/网关终止，后端业务监听器是 HTTP；生产需将业务监听器限制在网关可访问的网络。Target HTTPS 使用系统 CA，也可通过 Go 标准 `SSL_CERT_FILE`/`SSL_CERT_DIR` 指向只读企业 CA 挂载，CA 变化需重启进程。

进程文件的端口、角色、连接、并发参数是启动时配置，修改后需滚动重启。`secrets` 映射及其文件内容支持热刷新：父目录 fsnotify + 300ms debounce + 30 秒内容 hash 兜底。环境变量 Secret 使用进程启动时环境；环境值变更需重新创建容器。

```json
{"secrets":{"vendor-a-token":{"file":"/etc/notifier-secrets/vendor-a-token"}}}
```

这只是 `secrets` 片段，应合入完整进程文件。Target 对应 `auth` 为 `{"type":"static_header","header":"Authorization","secret_ref":"vendor-a-token"}`。文件内容包含完整 Header 值，例如 `Bearer ...`。更新步骤：先分发新 Secret，再应用引用新 Secret 的配置；撤销时先禁用 Target 或修改认证引用，再删除旧 Secret，避免 Last-Known-Good 恢复旧的引用。

## 3. 管理配置

采用 `configs/management.example.json` 的 JSON 契约；YAML 示例在设计文档中用于展示模型，本实现输入为 JSON。

```sh
bin/notify-admin -file management.json
NOTIFIER_ADMIN_DSN='...' bin/notify-admin -file management.json -apply -actor change-123
```

默认只做离线解析、引用/白名单/配额和 CEL 编译校验。`-apply` 先锁定 `config_revision.global`，验证 Target/Retry/Hook 的 revision 不回退且内容变化必须递增，插入完整不可变 snapshot 并同事务推进 global revision；输出 actor/revision 审计。运行时两秒轮询 revision，失败保留 Last-Known-Good。不要直接修改历史 `config_snapshot`；也不要删除仍有任务引用的版本。

行为：Retry、Hook、幂等注入使用任务创建时版本。安全：Client 禁用/binding、Target 禁用、Origin/Method/Path/IP 白名单和认证字段使用投递开始时最新有效快照。修改 Origin 后，旧任务会停止向旧 Origin 投递，而不会自动改向新站点。

当前编译注册 `generic_http` Adapter。编译期 SDK 的扩展点是 `target.Adapter`，接入新供应商时须添加受控注册和配置校验，并继续通过 `delivery.Worker` 的 Claim、Quota、Attempt、Retry 链路。

## 4. 调度与恢复

- 默认七个 TTL Bucket：5s、30s、2m、10m、30m、2h、6h。
- Outbox 发布 lease 30s；发布确认后条件更新；不确定发布允许重复。
- Notification lease 60s；HTTP 最长可配 30s，结果提交另有 5s 上限。
- 重试先映射 Bucket，再检查 deadline；Retry-After 无法映射或超过 deadline 则终止。
- 5 秒 Recovery Loop 回收 expired lease/outbox，并修复到期超 60 秒且无有效触发记录的 pending 任务。
- Broker 已 confirm 但消息丢失也可修复：published 记录仅有 60 秒有效确认窗口。修复推进 generation，使旧消息失效。
- Lease 过期记录 Attempt 为 `unknown`。自动再次执行同样受非幂等网络不确定策略、次数和 deadline 限制。
- 人工重试仅允许 failed；重开一个有限周期，不清零累计 Attempt 序号，不改请求快照。
- API-only 角色 readiness 不依赖 MQ；worker/outbox 依赖 MQ。已加载配置的实例不因管理库临时断开而失去快照。Redis fail-open 保留进程固定并发限制；fail-closed 拒绝新受理或延后投递。

Quota 是固定一秒窗口；配置 0 表示不启用该维度的分布式限制。所有 policy 的全局限制必须一致。Redis concurrency token 使用服务器时间和 60s TTL；释放使用 token 删除，避免崩溃永久占额。不同 Pod 配置 revision 切换窗口仍是最终一致。

批次 envelope 消耗一次受理额度，每个新受理 item 再独立消耗一次；幂等重放不消耗 item 额度。这样全部非法的批次也不能无限创建批次元数据。

## 5. Mock 与故障工具

| Target | 路径 | 参数与行为 |
|---|---|---|
| A | `/a` | `failures=N`：同幂等键前 N 次 500，之后 200 |
| B | `/b` | 前 N 次 code=1001，之后 code=0；`terminal=true` 返回 code=2000 |
| C | `/c` | 不支持幂等，显式禁用不确定结果的自动重试 |
| D | `/d` | 前 N 次 429，`retry_after=5` 或 HTTP 日期 |
| 全部 | 上述路径 | `sleep=15` 模拟超时；`reset=true` 断开连接 |

```sh
sh scripts/fault.sh kill-notifier
sh scripts/fault.sh start-notifier
sh scripts/fault.sh restart-rabbitmq
sh scripts/fault.sh stop-redis
sh scripts/fault.sh start-redis
sh scripts/fault.sh stop-mariadb
sh scripts/fault.sh start-mariadb
```

配置原子更新：准备完整 JSON 临时文件后 `mv` 覆盖进程文件；管理库更新反复调用 `notify-admin -apply`。应同时观察 config revision、数据库任务/Attempt、日志和 MQ unacked，不能仅根据 API 返回判断投递完成。

## 6. 云上部署输入

`deploy/kubernetes.yaml` 是应用 Deployment/Service/PDB 模板，需平台提供：实际镜像、`notifier-process` ConfigMap（其中 health_listen 必须为 `:8081`）、`notifier-connections` Secret、`notifier-target-secrets`、TLS Ingress、网络策略和 HA 依赖。Health 端口不加入业务 Service；平台应仅允许 kubelet/内部管理网络访问 Pod 的 8081。

SIGTERM：先 Not Ready，取消 consumer 拉取及周期 loop，HTTP Shutdown 停止新请求，等待进行中的 Attempt 完成并落库/ACK，45s 后取消剩余工作并关闭 MQ。Pod grace 55s 留出收尾时间。HA、备份/PITR、Quorum 副本和滚动漂移必须在后续环境验收中验证。

历史保留期由运营策略决定，本实现不自动清除 Notification/Attempt/Outbox/config 历史。清理应由独立维护 Job 执行，必须同时保证上游幂等保留期和所有非终态任务的配置/请求仍存在。
