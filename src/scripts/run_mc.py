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

if __name__ == "__main__":
  result = check_server_bucket_existed("tianjin", "sss")
  print(result)