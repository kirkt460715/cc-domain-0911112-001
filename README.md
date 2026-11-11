# registry_bridge 数据接入工作台

保留现有领域校验、持久化与操作入口。Python 3.11 标准库和 SQLite；运行时无需下载依赖。测试覆盖已有领域行为、错误路径和恢复。本地联调材料不会自动采集到业务库。

## 本地运行

    python3 -m compileall -q registry_bridge
    python3 -m unittest discover -s tests -v
    python3 -m registry_bridge --db :memory: demo --fixture fixtures/demo.json
    python3 -m synthetic_exchange.smoke

## 容器运行

    docker build -t local/registry_bridge .
    docker run --rm --network none local/registry_bridge

构建时准备基础镜像，正式运行不下载。run_checks.sh 是完整的测试、编译检查及实际产品演示入口，正常在 90 秒内结束。每次演示使用临时数据或内存库，不改写源样本。

## 采集联调

GET /changes?cursor=start、p2、expired、loop。next 为不透明游标；410 给出重放起点。每页 items 是登记事件原文，损坏行也属于交接内容。/_requests 可查看请求记录。

    python3 -m synthetic_exchange.server --port 8765

原有操作参数可通过 python3 -m registry_bridge --help 查询。
