from pathlib import Path

for path, host in (
  (Path("/opt/storagent-v2reg/nginx-a/default.conf"), "10.32.12.110"),
  (Path("/opt/storagent-v2reg/nginx-b/default.conf"), "10.32.12.111"),
):
  text = path.read_text()
  login_location = (
    "    location = /login_by_code {\n"
    "        access_log off;\n"
    "        include /etc/nginx/proxy-common.inc;\n"
    "        proxy_pass http://frontend:80;\n"
    "    }\n\n"
  )
  if "location = /login_by_code" not in text:
    text = text.replace("    location / {", login_location + "    location / {")
  marker = "\nserver {\n    listen 18443"
  http = text.split(marker)[0]
  start = http.find("server {")
  block = http[start:]
  https = block.replace(
    "listen 80 default_server;",
    "listen 18443 ssl;\n    ssl_certificate /certs/cert.pem;\n    ssl_certificate_key /certs/key.pem;\n    ssl_protocols TLSv1.2 TLSv1.3;",
    1,
  )
  https = https.replace("listen [::]:80 default_server;\n", "")
  vs = (
    "    location ^~ /api/virtual-storage/ {\n"
    f"        proxy_pass https://{host}:8443;\n"
    "        proxy_ssl_verify off;\n"
    "        proxy_ssl_server_name on;\n"
    "        proxy_set_header Host $host;\n"
    "        proxy_set_header Authorization $http_authorization;\n"
    "        proxy_set_header Idempotency-Key $http_idempotency_key;\n"
    "        proxy_set_header If-Match $http_if_match;\n"
    "        proxy_set_header X-API-Key $http_x_api_key;\n"
    "        proxy_set_header Content-Type $http_content_type;\n"
    "    }\n\n"
  )
  https = https.replace("    location ^~ /api/ {", vs + "    location ^~ /api/ {", 1)
  https = https.replace('return 200 "ok-a\\n";', 'return 200 "ok-https\\n";')
  https = https.replace('return 200 "ok-b\\n";', 'return 200 "ok-https\\n";')
  path.write_text(http.rstrip() + "\n\n" + https)
  print("rewrote", path.name)
