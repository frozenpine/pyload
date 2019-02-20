# coding: utf-8

import json
import time

from collections import OrderedDict

from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient

from BitMEXAPIKeyAuthenticator import APIKeyAuthenticator

from common.utils import path


def correct_data(json_data):
    if json_data["orderQty"] == 0:
        raise ValueError("invalid orderQty[{}]".format(json_data["orderQty"]))

    check_qty = {
        "Buy": lambda qty: qty > 0,
        "Sell": lambda qty: qty < 0
    }

    if "side" in json_data:
        if check_qty[json_data["side"]](json_data["orderQty"]):
            return json_data

        raise ValueError("orderQty[{}] mismatch with side[{}]".format(
            json_data["orderQty"], json_data["side"]))

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


def nge(test=True, config=None, api_key=None, api_secret=None):
    if test:
        host = 'http://trade'
    else:
        host = 'https://www.bitmex.com'

    if not config:
        # See full config options at
        # http://bravado.readthedocs.io/en/latest/configuration.html
        config = {
            # Don't use models (Python classes) instead of dicts for
            # #/definitions/{models}
            'use_models': False,
            # bravado has some issues with nullable fields
            'validate_responses': False,
            # Returns response in 2-tuple of (body, response);
            # if False, will only return body
            'also_return_response': True
        }

    spec_dict = json.loads(open(path("@/swagger/bitmex.json"),
                                encoding="utf-8").read())

    api_key = api_key
    api_secret = api_secret

    if api_key and api_secret:
        request_client = RequestsClient()

        request_client.authenticator = NGEAPIKeyAuthenticator(
            host, api_key, api_secret)

        return SwaggerClient.from_spec(
            spec_dict, origin_url=host, config=config,
            http_client=request_client)

    else:
        return SwaggerClient.from_spec(
            spec_dict, origin_url=host, config=config)