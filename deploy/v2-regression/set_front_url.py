"""Point the test console's OA link at the HTTPS entry. Does not print secrets."""
import json
from pathlib import Path

HTTPS = {
  "backend-a.env": "https://10.32.12.110:18443",
  "backend-b.env": "https://10.32.12.111:18443",
}
ORIGINS = list(HTTPS.values())

for name, front in HTTPS.items():
  path = Path("/opt/storagent-v2reg/env") / name
  if not path.exists():
    print("missing", name)
    continue
  lines = []
  for line in path.read_text().splitlines():
    if line.startswith("FRONT_URL="):
      lines.append(f"FRONT_URL={front}")
    elif line.startswith("BACKEND_CORS_ORIGINS="):
      origins = json.loads(line.split("=", 1)[1])
      for origin in ORIGINS:
        if origin not in origins:
          origins.append(origin)
      lines.append("BACKEND_CORS_ORIGINS=" + json.dumps(origins, separators=(",", ":")))
    else:
      lines.append(line)
  path.write_text("\n".join(lines) + "\n")
  print("updated", name)
