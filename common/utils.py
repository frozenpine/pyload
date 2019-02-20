# coding: utf-8

import os
import sys
import logging
import requests


def _format_path(_path):
    return os.path.abspath(_path).replace('/', os.sep)


def _get_caller():
    caller = getattr(sys, '_getframe')(2)
    while 'utils.py' in caller.f_code.co_filename:
        caller = caller.f_back
    return caller


def _get_caller_file():
    return _get_caller().f_code.co_filename


def _get_caller_path():
    return os.path.abspath(os.path.dirname(_get_caller_file()))


def path(file_path):
    """Convert formatted path string to os absolute path.

    Arguments:
        file_path {string} -- a formatted path string

    Raises:
        ValueError -- raise when invalid path format

    Returns:
        string -- os absolute path string
    """

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))

    if os.path.isabs(file_path):
        return file_path
    elif file_path.startswith('@/'):
        return _format_path(base_dir + file_path.lstrip('@'))
    elif file_path.startswith('./'):
        caller_home_dir = _get_caller_path()
        return _format_path(caller_home_dir + file_path.lstrip('.'))
    elif file_path.startswith('../'):
        caller_home_dir = _get_caller_path()
        caller_parent_dir = os.path.abspath(
            os.path.join(caller_home_dir, '../'))
        return _format_path(caller_parent_dir + file_path.lstrip('.'))
    else:
        msg = u'Invalid path["{}"].'.format(file_path)
        logging.error(msg)
        raise ValueError(msg)


def check_code(result):
    if "result" in result:
        result = result["result"]

    if isinstance(result, int):
        return 0 == result

    return "0" == result["code"]


def http_request(uri, method="POST", session=None, **kwargs):
    if not session:
        session = requests.Session()

    response = getattr(session, method.lower())(
        uri, **kwargs)

    if not response.ok:
        raise requests.RequestException(response=response)

    return response
