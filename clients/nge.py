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

from bravado.client import SwaggerClient
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
            'formats': [SwaggerFormat(
                            format="guid",
                            to_wire=lambda guid_obj: str(guid_obj),
                            to_python=guid_deserializer,
                            description="GUID to uuid",
                            validate=guid_validate),
                        SwaggerFormat(
                            format="date-time",
                            to_wire=datetime_serializer,
                            to_python=datetime_deserializer,
                            description="date-time",
                            validate=datetime_validate
                        )]
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
