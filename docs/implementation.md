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
