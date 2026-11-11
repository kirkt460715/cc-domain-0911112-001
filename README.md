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

poll 入口从持久化 checkpoint 续采，管理员可用 --source 限定来源、--issuer 限定签发方、--pages 限定页数。游标只定位网络页，各签发方连续生效的 sequence 由账本接收与 drain 机制独立核对，日报继续以 sequence 为准。坏行连同原页游标与原文写入 dead_letters 供 deadletter.replay 重放，不卡住同页其他签发方；整页在一个事务内核清后才确认下一游标。429 按 Retry-After 有界重试，游标重复时停采等待处理，410 按服务给定起点重放且不清空登记。同源并发采集以租约代际 fencing，旧进程的迟到提交不能覆盖新持有者的 checkpoint。

    python3 -m registry_bridge --db feed.db init
    python3 -m registry_bridge --db feed.db call issuer.add --json '{"issuer":"north","display_name":"Synthetic North"}'
    python3 -m registry_bridge --db feed.db call issuer.add --json '{"issuer":"south","display_name":"Synthetic South"}'
    python3 -m registry_bridge --db feed.db poll --source registry --url http://127.0.0.1:8765/changes --pages 4
    python3 -m registry_bridge --db feed.db poll --source registry --pages 2 --issuer north
    python3 -m registry_bridge --db feed.db checkpoints
    python3 -m registry_bridge --db feed.db report

原有操作参数可通过 python3 -m registry_bridge --help 查询。
