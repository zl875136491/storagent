#! /bin/bash

# venv
# source ../venv/bin/activate

action=$1

module_arg=$2

source="https://pypi.tuna.tsinghua.edu.cn/simple"

# 启动项目
function run() {
  uvicorn main:app --host 0.0.0.0 --port=$SERVER_PORT --timeout-graceful-shutdown 1 --reload --reload-exclude '*/tests/*'
}

# 安装包
function install() {
  if [ -z "$module_arg" ]; then
    echo "Usage: $0 install <package_name>"
    exit 1
  fi
  pip install -i $source $module_arg
  add_package_to_requirements
}

# 卸载包
function uninstall() {
  if [ -z "$module_arg" ]; then
    echo "Usage: $0 uninstall <package_name>"
    exit 1
  fi
  pip uninstall $module_arg
  remove_package_from_requirements
}

# 从 requirements.txt 中删除包
function remove_package_from_requirements() {
  # 如果存在, 从 requirements.txt 中删除
  if grep $module_arg requirements.txt; then
    sed -i "/$module_arg/d" requirements.txt
  fi
}

# 将包添加到 requirements.txt 中
function add_package_to_requirements() {
  pip freeze | grep $module_arg >> requirements.txt
}

# 运行脚本
function run_scripts() {
  if [ -z "$module_arg" ]; then
    echo "Usage: $0 run_scripts <script_name>"
    echo "Script names: < gen_salt | gen_jwt_secret >"
    exit 1
  fi
  python src/scripts/$module_arg.py
}

# 帮助
function help() {
  echo "Usage: $0 < run | install | uninstall | scripts | help >"
  exit 1
}

if [ "$action" == "run" ]; then
  run
elif [ "$action" == "install" ]; then
  install
elif [ "$action" == "uninstall" ]; then
  uninstall
elif [ "$action" == "scripts" ]; then
  run_scripts
else 
  help
fi