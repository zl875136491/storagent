# 为项目设置根目录
from os.path import dirname as os_dirname
from os.path import abspath as os_abspath
current_path = os_abspath(__file__)
if "src" not in current_path:
  raise ValueError("请保证 src 目录在项目根目录下")
APP_ROOT =os_dirname(os_abspath(__file__)).split("/src")[0]