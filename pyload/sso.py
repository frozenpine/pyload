# coding: utf-8
import re
import time

# noinspection PyPackageRequirements
from locust import Locust, events
# noinspection PyPackageRequirements
from locust.exception import StopLocust

from clients.sso import User


class LocustWrapper(object):
    def __init__(self, client):
        self._client = client

    def __getattr__(self, item):
        origin_attr = getattr(self._client, item)

        def wrapper(*args, **kwargs):
            start_time = time.time()

            try:
                result = origin_attr(*args, **kwargs)
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

            if not result:
                events.request_failure.fire(
                    request_type=self._client.__class__.__name__,
                    name=item, response_time=total_ms,
                    exception=Exception("{} failed.".format(item)))
            else:
                events.request_success.fire(
                    request_type=self._client.__class__.__name__,
                    name=item, response_time=total_ms,
                    response_length=0)

            return result

        return wrapper


class SSOLocust(Locust):
    def __init__(self):
        super(SSOLocust, self).__init__()

        host_pattern = re.compile(
            r"(?P<scheme>https?)://"
            r"(?P<host>\w[\w.-]*)(?::(?P<port>\d+))?/?")

        if not self.host:
            user = User()
        else:
            match = host_pattern.match(self.host)

            if not match:
                raise StopLocust("Invalid host.")

            result = match.groupdict()

            user = User(schema=result["scheme"],
                        host=(result["host"],
                              int(result["port"] if result["port"] else 80)))

        self.client = LocustWrapper(user)
