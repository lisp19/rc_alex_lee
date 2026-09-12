# 实现记录

本阶段：代码开发、Go 构建、静态审查；不启动服务、中间件或容器，不执行运行时验收。

## 实现选择

- Go 1.27.1 及六个直接库依赖按设计基线锁定。
- CEL v0.30.0 仓库自身声明 module 为 `github.com/google/cel-go`，使用此实际 module 路径替代文档中的 `github.com/cel-expr/cel-go`；版本不变。
- 管理配置使用版本化 JSON 完整文档（`config_snapshot`），`config_revision.global` 同事务推进。文档包含 Client、Binding、Target、Retry、Quota、Issuer、Hook；历史版本不可修改。全量构建后原子替换，避免跨实体读取不一致。历史任务按创建时的完整配置 revision 加载行为配置；最新安全配置独立校验。
- 请求快照用 `request_json` 保存 method/path/query/headers/body；二进制正文按 base64 保存。相对于拆列不改变事务或幂等语义。
- `cycle_attempt_count` 支持人工重试重新开启有限自动重试周期；总 attempt 序号不重置。
- Outbox 补充发布 token/lease；发布超时可回收，旧发布者不得覆盖新发布者状态。
- 部署文件只是待后续 review 的输入，镜像和外部基础设施未拉取。

## 提交阶段

1. 工程、领域模型、业务/管理 Schema。
2. 配置版本、监听、JWT、白名单、管理工具。
3. 幂等受理、批量、查询、API。
4. Outbox、MQ、Claim、HTTP、结果事务。
5. Retry、Recovery、人工重试。
6. CEL、Quota、Proxy、生命周期、交付及静态审查。

## 详细契约补充

- 全局管理文档代替逐实体管理表；watcher 每两秒读取 global revision，版本变化后一次加载全量。历史缓存最多 64 个版本；未缓存版本从只读管理库加载。因此管理库故障期间可执行已缓存历史版本的任务，未缓存版本会等待恢复。
- 管理输入为严格 JSON，不额外引入 YAML 解析依赖。进程 Secret 支持目录监听与内容 hash 热刷新；端口、角色、连接、并发和根 CA 是启动配置。
- 单条支持 Proxy；Batch 明确仅异步。重复批次创建新的关联 ID，重复 item 返回已有任务 ID，已有任务保留其原始 batch_id，不迁移归属。
- 手工重试重开有限周期，保留累计 attempt_count；不提供修改历史请求的能力。
- Lease Recovery 将原 started Attempt 标记 unknown，再按相同 retry policy 调度；非幂等且禁止 uncertain retry 时进入 failed，避免自动重复不可逆操作。
- MQ confirmed 记录不能永久阻止 reconciliation；到期 pending 在 60 秒确认宽限期后推进 generation 补发。
- 同一逻辑 key 的 UTF-8/base64 等价正文规范为相同字节表达；Header 名规范化，map 稳定 JSON 编码。正文 JSON 内部空白/键顺序保持原字节语义，不重排供应商签名内容。省略 delivery_mode 保留在请求 hash 中以稳定跨默认配置更新的重放；显式 mode 与省略 mode 视为不同提交。
- Path 接受已解码的 `/...`，拒绝 `%`、反斜杠、fragment/query 和 `.`/`..` segment；Query 独立编码。Origin 改变会禁止旧任务继续访问旧站点。连接使用最新安全配置对应的池和经过 IP 校验的直接 Dial，禁用环境代理、压缩和 redirect。
- 响应读取完整且在上限内才计算完整 SHA-256；超限/读取中断不伪装成完整 hash。非 JSON 或截断到不能解析的预览不保留；可解析 JSON 按敏感字段与当前 Secret 脱敏，Hook 的输入则为限长原始预览。Hook runtime failure 终止，不能覆盖传输/安全失败。
- SDK 保留编译期 Adapter interface；当前注册 generic_http，不加入任何供应商特定 SDK。公共配置仅接受已注册类型。
- 企业入口 HTTPS 由 Ingress 终止；独立 Health listener 通过内部网络隔离。配置示例和部署模板见运行手册。
- 限制值包含设计默认值；API/body/batch/lease/bucket/loop 周期为 v1 工程常量，HTTP timeout/响应上限、Retry、Quota、并发和连接池可配置。变更固定 Bucket 需代码与拓扑协调发布。

## 本阶段验证范围

Go 编译构建、`go vet`、`go mod verify`、格式与 diff 检查，以及人工逐路径静态审查。容器镜像、外部数据库/MQ/Redis、Mock HTTP 服务均未启动。运行时可靠性、版本兼容、故障注入和性能结论留待后续验收；本阶段不将静态审查等同于运行成功。
