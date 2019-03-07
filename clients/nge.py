# coding: utf-8

import json
import time
import uuid
import re
import yaml
import os

from collections import OrderedDict
from itertools import product
from threading import Lock

from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient
from bravado_core.formatter import SwaggerFormat
from bravado_core.exception import SwaggerValidationError

from BitMEXAPIKeyAuthenticator import APIKeyAuthenticator

# noinspection PyPackageRequirements
from locust import Locust, events
# noinspection PyPackageRequirements
from locust.exception import StopLocust

from common.utils import path, pushd


def correct_data(json_data):
    if json_data["orderQty"] == 0:
        raise ValueError("invalid orderQty[{}]".format(json_data["orderQty"]))

    check_qty = {
        "Buy": lambda qty: qty > 0,
        "Sell": lambda qty: qty < 0
    }

    if "side" in json_data:
        if not check_qty[json_data["side"]](json_data["orderQty"]):
            raise ValueError("orderQty[{}] mismatch with side[{}]".format(
                json_data["orderQty"], json_data["side"]))

        return json_data

    if json_data["orderQty"] > 0:
        json_data["side"] = "Buy"
    else:
        json_data["side"] = "Sell"
        json_data["orderQty"] = -json_data["orderQty"]

    return json_data


class NGEAPIKeyAuthenticator(APIKeyAuthenticator):
    def apply(self, r):
        # 5s grace period in case of clock skew
        expires = int(round(time.time()) + 5)
        r.headers['api-expires'] = str(expires)
        r.headers['api-key'] = self.api_key
        r.json = correct_data(OrderedDict(r.data))
        r.data = None
        prepared = r.prepare()
        # body = prepared.body or ''
        body = json.dumps(r.json)
        url = prepared.path_url
        r.headers['api-signature'] = self.generate_signature(
            self.api_secret, r.method, url, expires, body)
        return r


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


def nge(host="http://trade", config=None, api_key=None, api_secret=None):
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
            'formats': [GUID_FORMATTER]
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

        spec_dict = load_method[ext](
            open(spec_file, encoding="utf-8").read())

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


class LocustWrapper(object):
    def __init__(self, client):
        self._client = client

    def __getattr__(self, item):
        origin_attr = getattr(self._client, item)

        if not callable(origin_attr):
            return LocustWrapper(origin_attr)

        def wrapper(*args, **kwargs):
            start_time = time.time()

            try:
                result, response = origin_attr(*args, **kwargs).result()
            except Exception as e:
                end_time = time.time()
                total_ms = int((end_time - start_time) * 1000)

                events.request_failure.fire(
                    request_type=self._client.__class__.__name__,
                    name=item, response_time=total_ms,
                    exception=e)
                raise

            end_time = time.time()
            total_ms = int((end_time - start_time) * 1000)

            if 200 != response.status_code:
                events.request_failure.fire(
                    request_type=self._client.__class__.__name__,
                    name=item, response_time=total_ms,
                    exception=Exception(response.text))
            else:
                events.request_success.fire(
                    request_type=self._client.__class__.__name__,
                    name=item, response_time=total_ms,
                    response_length=0)

            return result

        return wrapper


class LazyLoader(object):
    from clients.sso import User

    _user_api_dict = dict()

    def __init__(self, host=None):
        host_pattern = re.compile(
            r"(?P<scheme>https?)://"
            r"(?P<host>\w[\w.-]*)(?::(?P<port>\d+))?/?")

        self._locker = Lock()

        if not host:
            self._sso = self.User()
        else:
            match = host_pattern.match(host)
            if not match:
                raise StopLocust("Invalid host.")

            result = match.groupdict()

            self._sso = self.User(
                schema=result["scheme"],
                host=(result["host"],
                      int(result["port"]) if result["port"] else 80))

    @property
    def logged(self):
        return self._sso.logged

    def get_swagger_client(self, identity, password,
                           api_key="", api_secret=""):
        if identity not in self._user_api_dict:
            if api_key and api_secret:
                self._user_api_dict[identity] = LocustWrapper(
                    nge(host=self._sso.host(),
                        api_key=api_key,
                        api_secret=api_secret))
            else:
                with self._locker:
                    if not self._sso.login(identity, password):
                        raise ValueError(
                            "invalid identity or password: {}".format(
                                identity))

                    self._user_api_dict[identity] = LocustWrapper(
                        nge(host=self._sso.host(),
                            api_key=self._sso.api_key,
                            api_secret=self._sso.api_secret))

        return self._user_api_dict[identity]


class NGELocust(Locust):
    def __init__(self):
        super(NGELocust, self).__init__()

        self.client = LazyLoader(host=self.host)
