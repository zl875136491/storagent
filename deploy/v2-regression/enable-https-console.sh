#!/bin/sh
# 给 18088 控制台增加 18443。HTTPS server 复制 HTTP 的区域反代，并只在 HTTPS 上转发虚拟存储 API。
set -eu
python3 /opt/storagent-v2reg/scripts/rewrite_https_nginx.py
python3 - <<'PY'
from pathlib import Path
for name in ("docker-compose.a.yml", "docker-compose.b.yml"):
    path = Path("/opt/storagent-v2reg") / name
    text = path.read_text()
    if "18443:18443" not in text:
        text = text.replace('- "18088:80"', '- "18088:80"\n      - "18443:18443"')
    if "/certs:/certs:ro" not in text:
        text = text.replace(
            "./nginx-a/proxy-remote.inc:/etc/nginx/proxy-remote.inc:ro",
            "./nginx-a/proxy-remote.inc:/etc/nginx/proxy-remote.inc:ro\n      - /opt/storagent-vs/certs:/certs:ro",
        )
        text = text.replace(
            "./nginx-b/proxy-remote.inc:/etc/nginx/proxy-remote.inc:ro",
            "./nginx-b/proxy-remote.inc:/etc/nginx/proxy-remote.inc:ro\n      - /opt/storagent-vs/certs:/certs:ro",
        )
    path.write_text(text)
    print(f"compose {name}")
PY
echo https-console-config-ready
