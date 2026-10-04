"""HTTP 端点: /status 输出 JSON, / 输出一页可视化面板。

用法: python -m cryptocmd.server --symbols BTC,ETH --port 8000
"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .service import CollectorService

_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>cryptocmd 采集状态</title>
<style>
  body { font-family: "Segoe UI", "Microsoft YaHei", sans-serif; margin: 2rem; background: #0f1420; color: #e6e9f0; }
  h1 { font-size: 1.3rem; }
  table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
  th, td { padding: .5rem .8rem; border-bottom: 1px solid #2a3350; text-align: left; font-size: .92rem; }
  th { color: #8fa3c8; font-weight: 600; }
  .ok { color: #4ade80; }
  .bad { color: #f87171; font-weight: 700; }
  .meta { color: #8fa3c8; font-size: .85rem; margin-top: .6rem; }
  .pill { padding: .1rem .5rem; border-radius: .6rem; background: #1d2742; }
</style>
</head>
<body>
<h1>cryptocmd 常驻采集服务 <span class="pill" id="uptime"></span></h1>
<table>
  <thead><tr>
    <th>Symbol</th><th>命中来源</th><th>走缓存</th><th>上次抓取耗时 (ms)</th>
    <th>连续失败次数</th><th>缓存行数</th><th>抓取中</th><th>上次成功 (UTC)</th><th>最近错误</th>
  </tr></thead>
  <tbody id="rows"></tbody>
</table>
<div class="meta" id="meta"></div>
<script>
async function refresh() {
  const res = await fetch('/status');
  const data = await res.json();
  document.getElementById('uptime').textContent = 'uptime ' + data.uptime_s + 's';
  const tbody = document.getElementById('rows');
  tbody.innerHTML = '';
  for (const [sym, s] of Object.entries(data.symbols)) {
    const tr = document.createElement('tr');
    const failCls = s.consecutive_failures > 0 ? 'bad' : 'ok';
    tr.innerHTML =
      '<td><b>' + sym + '</b></td>' +
      '<td>' + s.last_source + '</td>' +
      '<td>' + (s.cache_hit ? '是' : '否') + '</td>' +
      '<td>' + s.last_fetch_ms + '</td>' +
      '<td class="' + failCls + '">' + s.consecutive_failures + '</td>' +
      '<td>' + s.rows_cached + '</td>' +
      '<td>' + (s.inflight ? '是' : '否') + '</td>' +
      '<td>' + (s.last_success_at || '-') + '</td>' +
      '<td>' + (s.last_error || '-') + '</td>';
    tbody.appendChild(tr);
  }
  document.getElementById('meta').textContent =
    '刷新间隔 ' + data.interval_s + 's · 缓存 TTL ' + data.cache_ttl_s + 's · 每 3s 自动刷新';
}
refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>
"""


def make_handler(service):
    class StatusHandler(BaseHTTPRequestHandler):
        def _send(self, body, content_type, status=200):
            payload = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path.startswith("/status"):
                self._send(
                    json.dumps(service.status(), ensure_ascii=False, indent=2),
                    "application/json; charset=utf-8",
                )
            elif self.path in ("/", "/index.html"):
                self._send(_PAGE, "text/html; charset=utf-8")
            else:
                self._send("not found", "text/plain; charset=utf-8", status=404)

        def log_message(self, *args):
            pass

    return StatusHandler


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m cryptocmd.server")
    parser.add_argument("--symbols", default="BTC,ETH", help="逗号分隔的 symbol 列表")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--interval", type=float, default=60.0, help="后台刷新间隔(秒)")
    parser.add_argument("--db", default="cryptocmd_history.db")
    args = parser.parse_args(argv)

    from .adapter import SQLiteAdapter

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    service = CollectorService(
        symbols, adapter=SQLiteAdapter(args.db), interval=args.interval
    )
    service.start()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(service))
    print("serving /status on http://%s:%d/ (symbols: %s)"
          % (args.host, args.port, ", ".join(symbols)))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        service.stop()
        server.server_close()


if __name__ == "__main__":
    main()
