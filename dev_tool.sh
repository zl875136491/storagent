#! /bin/bash

# venv
source ../venv/bin/activate

# pip install
action=$1

package_name=$2

source="https://pypi.tuna.tsinghua.edu.cn/simple"

if [ "$action" == "install" ]; then
  pip install -i $source $package_name
  # 如果不存在, 追加到 requirements.txt
  if ! grep $package_name requirements.txt; then
    echo "" >> requirements.txt
    pip freeze | grep $package_name >> requirements.txt
  fi

elif [ "$action" == "uninstall" ]; then
  pip uninstall $package_name
  # 如果存在, 从 requirements.txt 中删除
  if grep $package_name requirements.txt; then
    sed -i "/$package_name/d" requirements.txt
  fi
elif [ "$action" == "upgrade" ]; then
  # 丢弃所有未提交的修改
  git checkout .
  # 拉取最新代码
  git pull
elif [ "$action" == "test" ]; then
  python -m pytest tests/ -v
elif [ "$action" == "migrate" ]; then
  python -m src.scripts.migrations
elif [ "$action" == "drop_index" ]; then
  python -m src.scripts.drop_db_indexes
else
  echo "Usage: $0 < install | uninstall | upgrade | test | migrate | drop_index >"
  exit 1
fi