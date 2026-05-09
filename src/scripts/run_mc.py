import json
import subprocess
from minio import Minio
from typing import List


def _run_cmd(cmd):
  """
  执行 shell 命令并返回结果
  
  Args:
    cmd: 命令

  Returns:
    Tuple[bool, str]: 执行结果
    True: 执行成功
    False: 执行失败
    str: 执行结果
  """
  try:
    result = subprocess.run(
      cmd, shell=True, check=True, 
      capture_output=True, text=True
    )
    return True, result.stdout
  except subprocess.CalledProcessError as e:
    return False, e.stderr

def check_server_bucket_existed(server_name: str, bucket_name: str) -> bool:
  """
  检查存储桶是否存在
  """
  success, output = _run_cmd(f"mc ls {server_name}/{bucket_name}")
  if not success:
    return False
  return True

def get_remote_bucket_endpoint(server: str, bucket: str):
  success, output = _run_cmd(f"mc replicate ls {server}/{bucket}")
  if success:
    for line in output.splitlines():
      if "Remote Bucket:" in line:
        # 使用 strip 移除两端空格，split 分割后取最后一部分
        return line.split("Remote Bucket:")[-1].strip()
  return None


if __name__ == "__main__":
  # result = check_server_bucket_existed("tianjin", "sss")
  # print(result)
  result = get_remote_bucket_endpoint("beijing", "test1")
  print(result)