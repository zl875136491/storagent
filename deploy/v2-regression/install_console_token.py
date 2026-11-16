"""Install the console bridge token. Prints only that it finished."""
import secrets
import sys
from pathlib import Path

mode = sys.argv[1] if len(sys.argv) > 1 else "create"
token_path = Path("/tmp/vs-console.token")
if mode == "create":
  token = secrets.token_urlsafe(32)
  token_path.write_text(token + "\n")
  token_path.chmod(0o600)
else:
  token = token_path.read_text().strip()

def upsert(path: Path, extras: list[str]) -> None:
  if not path.exists():
    print("missing", path.name)
    return
  kept = []
  for line in path.read_text().splitlines():
    if line.startswith("VS_CONSOLE_TOKEN=") or line.startswith("VS_INTERNAL_BASE_URL="):
      continue
    kept.append(line)
  kept.extend(extras)
  path.write_text("\n".join(kept) + "\n")
  print("updated", path.name)

upsert(Path("/opt/storagent-vs/env/api.env"), [f"VS_CONSOLE_TOKEN={token}"])
for name, host in (("backend-a.env", "10.32.12.110"), ("backend-b.env", "10.32.12.111")):
  upsert(
    Path("/opt/storagent-v2reg/env") / name,
    [f"VS_CONSOLE_TOKEN={token}", f"VS_INTERNAL_BASE_URL=https://{host}:8443"],
  )
print("console token installed")
