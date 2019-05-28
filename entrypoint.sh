#!/usr/bin/env bash

[[ -L $0 ]] && SCRIPT_FILE=`readlink -fn $0` || SCRIPT_FILE=$0
cd `dirname "${SCRIPT_FILE}"` >/dev/null
BASE_DIR=`pwd`

VIRTUAL_ENV="${BASE_DIR}/.virtual"
NAME="market_maker"
PID_FILE="${BASE_DIR}/market_marker.pid"
PID=

function find_pid {
    if [[ -f "${PID_FILE}" ]]; then
        PID=`cat "${PID_FILE}"`

        kill -0 ${PID} &>/dev/null && return

        PID=
        rm -f "${PID_FILE}"
    fi

    PID=`ps -ef | \
        grep "${NAME}.py" | \
        grep -Ev "grep|vi|nano|bash|awk|ssh" | \
        awk '{printf($2)}'`

    if [[ "x${PID}" == "x" ]]; then
        return 1
    fi

    echo -n ${PID} > "${PID_FILE}"
}

function status {
    find_pid
    if [[ $? -eq 0 ]]; then
        echo "${NAME}[${PID}] is running."
    else
        echo "${NAME} is stopped."
    fi
}

function start {
    find_pid
    if [[ $? -eq 0 && $1 != "cancel" ]]; then
        echo "${NAME}[${PID}] is already started." >&2
        return 1
    fi

    if [[ ! -d "${VIRTUAL_ENV}" ]]; then
        echo "virtual env not exists." >&2
        return 1
    fi

    source "${VIRTUAL_ENV}/bin/activate"

    echo "Starting ${NAME}..."

    nohup python "${NAME}.py" $* &

    echo -n $! > "${PID_FILE}"

    sleep 1

    find_pid

    if [[ $? -eq 0 ]]; then
        echo "${NAME}[${PID}] started."
        return
    fi

    echo "${NAME} start failed." >&2
    return 1
}

function stop {
    find_pid

    if [[ $? -ne 0 ]]; then
        echo "${NAME} is already stopped." >&2
        return 1
    fi

    local _STOP_COUNT=0

    echo "Stopping ${NAME}[${PID}]..."
    while true; do
        if [[ ${_STOP_COUNT} -eq 0 ]]; then
            kill ${PID}
        fi

        sleep 1

        find_pid || break

        if [[ ${_STOP_COUNT} -eq 15 ]]; then
            echo "Normally stop failed, force killing ${NAME}[${PID}]." >&2
            kill -9 ${PID}
        fi

        _STOP_COUNT=$((_STOP_COUNT+1))

        if [[ ${_STOP_COUNT} -ge 300 ]]; then
            echo "${NAME}[${PID}] stop failed." >&2
            return 1
        fi
    done

    echo "${NAME} stopped."
}

COMMAND_PATTERN="start|stop|status|restart"

if [[ ! $1 =~ ${COMMAND_PATTERN} ]]; then
    echo "unknown command, valid: ${COMMAND_PATTERN}" >&2
    exit 1
fi

COMMAND=$1
shift

case ${COMMAND} in
    start)
        start $*
        exit $?
    ;;
    stop)
        stop
        exit $?
    ;;
    status)
        status
        exit $?
    ;;
    restart)
        stop && start $*
        exit $?
    ;;
esac
