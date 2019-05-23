#!/usr/bin/env bash

[[ -L $0 ]] && SCRIPT_FILE=`readlink -fn $0` || SCRIPT_FILE=$0
cd `dirname "${SCRIPT_FILE}"` >/dev/null
BASE_DIR=`pwd`

VIRTUAL_ENV="${BASE_DIR}/.virtual"

if [[ ! -d "${VIRTUAL_ENV}" ]]; then
    echo "virtual env not exists." >&2
    exit 1
fi

source "${VIRTUAL_ENV}/bin/activate"

nohup python market_maker.py $* &

echo -n $! > market_marker.pid
