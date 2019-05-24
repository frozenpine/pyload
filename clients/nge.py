# coding: utf-8

import json
import time
import uuid
import re
import yaml
import os

from collections import OrderedDict
from itertools import product
from datetime import datetime

from gevent.threading import Lock
from gevent.lock import BoundedSemaphore

from bravado.client import SwaggerClient, ResourceDecorator, CallableOperation
from bravado.http_future import HttpFuture
from bravado.requests_client import RequestsClient
from bravado_core.formatter import SwaggerFormat
from bravado_core.exception import SwaggerValidationError

from BitMEXAPIKeyAuthenticator import APIKeyAuthenticator

from common.utils import path, pushd


class NGEAPIKeyAuthenticator(APIKeyAuthenticator):
    def apply(self, r):
        # 5s grace period in case of clock skew
        expires = int(round(time.time()) + 5)
        r.headers['api-expires'] = str(expires)
        r.headers['api-key'] = self.api_key
        # r.json = correct_data(OrderedDict(r.data))
        r.json = OrderedDict(r.data)
        r.data = None
        prepared = r.prepare()
        # body = prepared.body or ''
        body = json.dumps(r.json)
        url = prepared.path_url
        r.headers['api-signature'] = self.generate_signature(
            self.api_secret, r.method, url, expires, body)
        return r


def datetime_validate(value):
    return isinstance(value, (str, int))


def datetime_deserializer(value):
    if isinstance(value, int):
        ts = datetime.utcfromtimestamp(value / 1000)

        return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    return value


def datetime_serializer(value):
    if isinstance(value, str):
        ts = datetime.strptime("%Y-%m-%dT%H:%M:%S.%f", value)

        return int(time.mktime(ts.timetuple()) * 1000)

    return value


def guid_validate(guid_string):
    pattern = re.compile(r'[0-9a-f-]{32,36}|\d{19}', re.I)

    if not pattern.match(guid_string):
        raise SwaggerValidationError(
            "guid[{}] is invalid.".format(guid_string))


def guid_deserializer(guid_string):
    try:
        return uuid.UUID(guid_string)
    except ValueError:
        return guid_string


GUID_FORMATTER = SwaggerFormat(
    format="guid",
    to_wire=lambda guid_obj: str(guid_obj),
    to_python=guid_deserializer,
    description="GUID to uuid",
    validate=guid_validate
)

DATETIME_FORMATTER = SwaggerFormat(
    format="date-time",
    to_wire=datetime_serializer,
    to_python=datetime_deserializer,
    description="date-time",
    validate=datetime_validate
)


def nge(host="http://trade", config=None, api_key=None, api_secret=None):
    """

    :rtype: SwaggerClient
    """
    if not config:
        # See full config options at
        # http://bravado.readthedocs.io/en/latest/configuration.html
        config = {
            # Don't use models (Python classes) instead of dicts for
            # #/definitions/{models}
            'use_models': False,
            'validate_requests': True,
            # bravado has some issues with nullable fields
            'validate_responses': False,
            'include_missing_properties': False,
            # Returns response in 2-tuple of (body, response);
            # if False, will only return body
            'also_return_response': True,
            'formats': [GUID_FORMATTER, DATETIME_FORMATTER]
        }

    spec_dir = path("@/swagger")
    spec_name = ("nge", "bitmex")
    spec_extension = ("yaml", "yml", "json")

    load_method = {
        "yaml": yaml.safe_load,
        "yml": yaml.safe_load,
        "json": json.dumps
    }

    with pushd(spec_dir):
        spec_file = ""

        for name, ext in product(spec_name, spec_extension):
            spec_file = ".".join([name, ext])

            if os.path.isfile(spec_file):
                break

        if not spec_file:
            raise RuntimeError("no valid swagger api define file found.")

        with open(spec_file, encoding="utf-8") as f:
            spec_dict = load_method[ext](f.read())

    if api_key and api_secret:
        request_client = RequestsClient()

        request_client.authenticator = NGEAPIKeyAuthenticator(
            host=host, api_key=api_key, api_secret=api_secret)

        return SwaggerClient.from_spec(
            spec_dict, origin_url=host, config=config,
            http_client=request_client)

    else:
        return SwaggerClient.from_spec(
            spec_dict, origin_url=host, config=config)


class NGEClientPool(object):
    class OperationWrapper(object):
        def __init__(self, origin_attr, semaphore):
            self._origin_attr = origin_attr
            self._semaphore = semaphore

        def __call__(self, *args, **kwargs):
            origin_result = self._origin_attr(*args, **kwargs)

            if isinstance(origin_result, HttpFuture):
                return NGEClientPool.OperationWrapper(
                    origin_result, self._semaphore)

            return origin_result

        def __getattr__(self, item):
            origin_attr = getattr(self._origin_attr, item)

            def wrapper(*args, **kwargs):
                try:
                    return origin_attr(*args, **kwargs)
                finally:
                    self._semaphore.release()

            if callable(origin_attr):
                return wrapper

            return origin_attr

    class ResourceWrapper(object):
        def __init__(self, origin_attr, semaphore):
            self._origin_attr = origin_attr
            self._semaphore = semaphore

        def __getattr__(self, item):
            origin_attr = getattr(self._origin_attr, item)

            if isinstance(origin_attr, ResourceDecorator):
                return NGEClientPool.ResourceWrapper(origin_attr,
                                                     self._semaphore)

            if isinstance(origin_attr, CallableOperation):
                return NGEClientPool.OperationWrapper(origin_attr,
                                                      self._semaphore)

            return origin_attr

    def __init__(self, host="http://trade", config=None, size=200):
        if not config:
            # See full config options at
            # http://bravado.readthedocs.io/en/latest/configuration.html
            config = {
                # Don't use models (Python classes) instead of dicts for
                # #/definitions/{models}
                'use_models': False,
                'validate_requests': True,
                # bravado has some issues with nullable fields
                'validate_responses': False,
                'include_missing_properties': False,
                # Returns response in 2-tuple of (body, response);
                # if False, will only return body
                'also_return_response': True,
                'formats': [GUID_FORMATTER, DATETIME_FORMATTER]
            }

        self._pool_size = size

        self._instance_list = list()

        ins = nge(host=host, config=config)

        self._instance_list.append(ins)

        for _ in range(self._pool_size-1):
            new_instance = SwaggerClient(
                swagger_spec=ins.swagger_spec,
                also_return_response=config.get("also_return_response"))
            self._instance_list.append(new_instance)

        self._semaphore = BoundedSemaphore(self._pool_size)

        self._lock = Lock()

        self._idx = 0

    def _next_instance(self):
        with self._lock:
            ins = self._instance_list[self._idx % self._pool_size]

            self._idx += 1

            return ins

    def __getattr__(self, item):
        self._semaphore.acquire()

        origin_attr = getattr(self._next_instance(), item)

        if not origin_attr or not isinstance(origin_attr,
                                             ResourceDecorator):
            return origin_attr

        return NGEClientPool.ResourceWrapper(origin_attr, self._semaphore)


class NGEWebsocket(object):
    def __init__(self, host="http://trade", symbol="XBTUSD",
                 api_key=None, api_secret=None):
        self._host = host
        self._symbol_list = [symbol]

        self._api_key = api_key
        self._api_secret = api_secret

        self._order_book = None
