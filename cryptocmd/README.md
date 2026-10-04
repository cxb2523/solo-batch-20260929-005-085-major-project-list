# cryptocmd 常驻采集服务

- `cryptocmd/__init__.py` — `CryptoData`, 对外稳定入口, 公开方法与返回结构不变
- `cryptocmd/service.py` — `CollectorService`, 后台线程按 symbol 定时刷新, 构造时注入 parser / adapter
- `cryptocmd/parser.py` — 默认 parser, 沿用 `CryptoData` 行为
- `cryptocmd/adapter.py` — 默认 SQLite adapter, 事务写入, 失败回滚旧历史
- `cryptocmd/server.py` — `/status` JSON 端点 + 可视化页面
- `cryptocmd/health.py` — 健康检查 CLI

## 用法

```bash
# 启动常驻采集 + 状态页 (http://127.0.0.1:8000/)
python -m cryptocmd.server --symbols BTC,ETH --port 8000 --interval 60

# 健康检查, 退出码非零即不合格
python -m cryptocmd.health --symbol BTC --watch 3
```

## 关键行为

- 同一 symbol 并发请求 single-flight: 只触发一次真实抓取、只落一次库;
  超时按指数退避重试, 重试窗口内并发请求继续复用同一次抓取
- 写库失败回滚到旧历史, 绝不写坏
- 解析失败 / 网络超时 / 缓存未命中统一为 `CryptoDataError`,
  `raise ... from` 保留原始原因; 取不到数据返回空列表而非 None
