# 本地协议服务

GET /changes?cursor=start、p2、expired、loop。next 为不透明游标；410 给出重放起点。每页 items 是登记事件原文，损坏行也属于交接内容。/_requests 可查看请求记录。

场景是响应脚本，不包含采集客户端。可以复制场景文件并改变响应次序、状态码或样本内容来联调。服务只监听 loopback，数据均为合成。

    python3 -m synthetic_exchange.server --port 8765
    python3 -m synthetic_exchange.smoke
