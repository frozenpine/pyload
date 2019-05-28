# encoding: utf-8
import logging
import re
import time

from threading import Lock

# noinspection PyPackageRequirements
from locust import Locust, events
# noinspection PyPackageRequirements
from locust.exception import StopLocust
from bravado.client import ResourceDecorator, CallableOperation

# noinspection PyUnresolvedReferences
from clients.nge import nge, NGEAPIKeyAuthenticator
from clients.hub import NGEClientPool


class LocustWrapper(object):
    def __init__(self, client):
        self._client_instance = client

    @property
    def authenticator(self):
        return self._client_instance.swagger_spec.http_client.authenticator

    @authenticator.setter
    def authenticator(self, value):
        if not isinstance(value, NGEAPIKeyAuthenticator):
            raise TypeError("authenticator must be NGEAPIKeyAuthenticator")

        self._client_instance.swagger_spec.http_client.authenticator = value

    @property
    def origin_url(self):
        return self._client_instance.swagger_spec.origin_url

    def __getattr__(self, item):
        origin_attr = getattr(self._client_instance, item)

        logging.debug("get %s from LocustWrapper with type %s",
                      item, type(origin_attr))

        def wrapper(*args, **kwargs):
            logging.info("new order.")

            start_time = time.time()

            try:
                result, response = origin_attr(*args, **kwargs).result()
            except Exception as e:
                end_time = time.time()
                total_ms = int((end_time - start_time) * 1000)

                events.request_failure.fire(
                    request_type=self._client_instance.__class__.__name__,
                    name=item, response_time=total_ms,
                    exception=e)
                raise

            end_time = time.time()
            total_ms = int((end_time - start_time) * 1000)

            if 200 != response.status_code:
                events.request_failure.fire(
                    request_type=self._client_instance.__class__.__name__,
                    name=item, response_time=total_ms,
                    exception=Exception(response.text))
            else:
                events.request_success.fire(
                    request_type=self._client_instance.__class__.__name__,
                    name=item, response_time=total_ms,
                    response_length=0)

            return result

        if isinstance(origin_attr, ResourceDecorator):
            return LocustWrapper(origin_attr)

        if isinstance(origin_attr, CallableOperation):
            return wrapper

        if isinstance(origin_attr, NGEClientPool.ResourceWrapper):
            return LocustWrapper(origin_attr)

        if isinstance(origin_attr, NGEClientPool.OperationWrapper):
            return wrapper

        logging.debug("origin attribute returned.")

        return origin_attr


class LazyLoader(object):
    from clients.sso import User

    _authenticator_cache = dict()

    def __init__(self, host=""):
        host_pattern = re.compile(
            r"(?P<scheme>https?)://"
            r"(?P<host>\w[\w.-]*)(?::(?P<port>\d+))?/?")

        self._locker = Lock()

        if host:
            match = host_pattern.match(host)
            if not match:
                raise StopLocust("Invalid host.")

            result = match.groupdict()

            self._sso_instance = self.User(
                schema=result["scheme"],
                host=(result["host"],
                      int(result["port"]) if result["port"] else 80))
        else:
            self._sso_instance = self.User()

        self._client = LocustWrapper(
            NGEClientPool(host=self._sso_instance.host(), size=10))
        # self._client = LocustWrapper(nge(host=self._sso_instance.host()))

    @property
    def logged(self):
        return self._sso_instance.logged

    def change_auth(self, identity, password,
                    api_key="", api_secret=""):
        if api_key and api_key in self._authenticator_cache:
            self._client.authenticator = self._authenticator_cache[api_key]

            return self._authenticator_cache[api_key]

        if identity and identity in self._authenticator_cache:
            self._client.authenticator = self._authenticator_cache[identity]

            return self._authenticator_cache[identity]

        if api_key and api_secret:
            authenticator = NGEAPIKeyAuthenticator(
                host=self._client.origin_url,
                api_key=api_key, api_secret=api_secret)

            self._client.authenticator = authenticator

            self._authenticator_cache[api_key] = authenticator

            if identity:
                self._authenticator_cache[identity] = authenticator

            return authenticator

        with self._locker:
            if not self._sso_instance.login(identity, password):
                raise ValueError(
                    "invalid identity or password: {}".format(identity))

            authenticator = NGEAPIKeyAuthenticator(
                host=self._client.origin_url,
                api_key=self._sso_instance.api_key,
                api_secret=self._sso_instance.api_secret)

            self._authenticator_cache[identity] = authenticator
            self._authenticator_cache[
                self._sso_instance.api_key] = authenticator

            return authenticator

    def __getattr__(self, item):
        return getattr(self._client, item)


class NGELocust(Locust):
    def __init__(self):
        super(NGELocust, self).__init__()

        self.client = LazyLoader(host=self.host)
