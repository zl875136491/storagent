import os
from pathlib import Path

admin = Path("/opt/storagent-v2reg/admin.pass").read_text().strip()
env = {}
for line in Path("/opt/storagent-vs/env/api.env").read_text().splitlines():
  if "=" in line and not line.startswith("#"):
    key, value = line.split("=", 1)
    env[key] = value
run_id = "ui" + os.urandom(3).hex()
Path("/opt/storagent-v2reg/ui.env").write_text(
  "\n".join([
    "UI_HTTP_BASE=http://10.32.12.111:18088",
    "UI_SHOT_DIR=/shots",
    f"RUN_ID={run_id}",
    "V2_ADMIN_USER=vsreg-admin",
    f"V2_ADMIN_PASSWORD={admin}",
    f"VS_BOOTSTRAP_APP_NAME={env['VS_BOOTSTRAP_APP_NAME']}",
    f"VS_BOOTSTRAP_CREDENTIAL={env['VS_BOOTSTRAP_CREDENTIAL']}",
    "",
  ])
)
os.chmod("/opt/storagent-v2reg/ui.env", 0o600)
print("ui env ready", run_id)
