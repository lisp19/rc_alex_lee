# 运行时 E2E 用例

用例设计与级别/正交矩阵见 [DESIGN.md](DESIGN.md)。总计 **51 case：P0 12、P1 21、P2 18**。

在仓库根目录执行：

```sh
# 仅查看已提交的用例定义、优先级和正交覆盖证明，不启动测试环境
python3 tests/e2e/run.py --list

# 使用上一阶段已经部署的双实例环境执行全部真实 E2E
python3 tests/e2e/run.py

# 单级、单组或修复后的定向回归
python3 tests/e2e/run.py --priority P0
python3 tests/e2e/run.py --cases O1-,O2-
python3 tests/e2e/run.py --cases P2-01,P0-02,P0-05
```

前置：已完成 `scripts/deploy-local.sh`，本机有 Go/Docker/Compose/Python/OpenSSL；本地生成配置与 JWT 私钥存在，供应商观测端口 18091 空闲。应在此项目独占的本地验收环境运行：故障用例会临时停止项目中间件/实例，配置用例会热更新测试配置，结束后恢复。

执行器按 P0→P1→P2 串行运行；默认记录失败后继续，用 `--fail-fast` 提前终止。case 内的并发用于验证竞争条件。供应商是编译运行的真实 HTTP 服务，未使用任何应用内部单元测试替身。不要调用 `go test` 来运行此测试集。

每轮生成 `.runtime/e2e/<run-id>/results.json`，包括用例提交 hash、工作树是否存在修复、每个 case 的 PASS/FAIL、耗时、证据、未执行列表及环境恢复结果。文件锁防止两个全局故障组同时操作同一部署。

该目录只包含可复用的测试定义及源码。环境文件、实际管理配置、临时挂载、密钥、供应商可执行文件、日志、通知 ID 和结果 JSON 由运行时创建在已忽略目录中。不要将 `.runtime/`、`.env` 或 `configs/secrets/` 添加到 Git。

用例应先提交再执行；发现业务缺陷后修复工作树并进行运行时回归，按照本轮“仅提交用例”的要求保留业务修复待单独 review。
