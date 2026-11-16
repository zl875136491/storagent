"""把单节点地址改成两节点，不输出连接串。"""
from pathlib import Path

needle = "10.32.12.110:27018"
insert = "10.32.12.110:27018,10.32.12.111:27018"
for path in Path("/opt/storagent-vs/env").glob("*.env"):
  text = path.read_text()
  if needle in text and "10.32.12.111:27018" not in text:
    path.write_text(text.replace(needle, insert))
    print(f"updated {path.name}")
  else:
    print(f"unchanged {path.name}")
