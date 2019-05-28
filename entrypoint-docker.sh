#! /bin/bash

[[ -L $0 ]] && SCRIPT_FILE=`readlink -fn $0` || SCRIPT_FILE=$0
pushd `dirname "${SCRIPT_FILE}"` >/dev/null
BASE_DIR=`pwd`
popd >/dev/null

source "${BASE_DIR}/common.sh" || exit 1

LOCUST_FILE="locustfile.py"
MODE=
WEB=1
RUN_ARGS=
DRY_RUN=0

function _help() {
    local _NAME
    _NAME=`basename ${SCRIPT_FILE}`
    
    printf "${_NAME} [-h -D]\n"
    printf "% ${#_NAME}s -H \${HOST} [-f \${LOCUST_FILE:=locustfile.py}]\n" ""
    printf "% ${#_NAME}s [-W -c \${CLIENTS_NUM} -r \${HATCH_RATE}]\n" ""
    printf "% ${#_NAME}s [-m \${MODE} -M \${MASTER_HOST} -P \${MASTER_PORT}]\n" ""
    printf "% ${#_NAME}s -t \${RUN_TIME}\n" ""
}

function _check_mode() {
    if [[ ${MODE} == "master" || -z ${MODE} ]]; then
        if [[ ${WEB} -eq 1 ]]; then
            if [[ -n ${CLIENT_NUM} || -n ${HATCH_RATE} ]]; then
                warning "neither {CLIENTS_NUM} nor {HATCH_RATE} is necessary in web mode, ignoring args."
            fi

            [[ -n ${MODE} ]] && RUN_ARGS="${RUN_ARGS} --master"
        fi

        if [[ ${WEB} -eq 0 ]]; then
            if [[ -z ${CLIENT_NUM} || -z ${HATCH_RATE} ]]; then
                error "please specify {CLIENT_NUM} & {HATCH_RATE} in no-web mode."
                exit 1
            fi

            RUN_ARGS="${RUN_ARGS} --no-web --master -c${CLIENT_NUM} -r${HATCH_RATE}"
        fi
    fi

    if [[ ${MODE} == "slave" ]]; then
        if [[ -n ${CLIENT_NUM} || -n ${HATCH_RATE} ]]; then
            warning "neither {CLIENTS_NUM} nor {HATCH_RATE} is necessary in ${MODE} mode, ignoring args."
        fi

        if [[ -z ${MASTER_HOST} || -z ${MASTER_PORT} ]]; then
            error "please specify {MASTER_HOST} & {MASTER_PORT} in ${MODE} mode."
            exit 1
        fi

        RUN_ARGS="${RUN_ARGS} --slave --master-host ${MASTER_HOST} --master-port ${MASTER_PORT}"
    fi
}

function _deal_args() {
    if [[ -z ${HOST} ]]; then
        error "host missing."
        exit 1
    fi

    RUN_ARGS="-H${HOST} -f${LOCUST_FILE}"
    
    _check_mode
}

function _start() {
    _deal_args
    if [[ ${DRY_RUN} -eq 1 ]]; then
        echo locust ${RUN_ARGS}
    else
        locust ${RUN_ARGS}
    fi
}

while getopts :H:f:m:c:r:t:M:P:DWh FLAG; do
    case ${FLAG} in
        H)
            HOST=${OPTARG}
        ;;
        f)
            LOCUST_FILE=${OPTARG}
        ;;
        m)
            MODE=${OPTARG}
        ;;
        c)
            CLIENT_NUM=${OPTARG}
        ;;
        r)
            HATCH_RATE=${OPTARG}
        ;;
        t)
            RUN_TIME=${OPTARG}
        ;;
        M)
            MASTER_HOST=${OPTARG}
        ;;
        P)
            MASTER_PORT=${OPTARG}
        ;;
        D)
            DRY_RUN=1
        ;;
        W)
            WEB=0
        ;;
        h)
            _help
            exit
        ;;
        *)
            error "invalid args: $*"
            _help >&2
            exit 1
        ;;
    esac
done
shift $((OPTIND-1))

_start
