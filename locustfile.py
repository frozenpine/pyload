# coding: utf-8

import logging
import time
import os
import csv
import queue
import sentry_sdk

from gevent.threading import Lock
from random import choice, random

# noinspection PyPackageRequirements,PyUnresolvedReferences
from locust import TaskSet, events, task
from sentry_sdk.integrations.logging import LoggingIntegration

from common.utils import path, get_env_bool
from pyload.nge import NGELocust

sentry_logging = LoggingIntegration(
    level=logging.INFO,        # Capture info and above as breadcrumbs
    event_level=logging.ERROR  # Send errors as events
)
sentry_sdk.init(integrations=[sentry_logging])

logging.basicConfig(level=logging.INFO)


class Order(TaskSet):
    def __init__(self, parent):
        super(Order, self).__init__(parent=parent)

        self._lock = Lock()

    @task(1000)
    def order_new(self):
        user_data = self.locust.user_auth_queue.get()

        logging.info("new auth info retrieved: %s", user_data)

        auth = self.client.change_auth(**user_data)

        logging.info("user info changed: %s", auth)

        order_price = choice(self.locust.order_price_list)
        order_volume = choice(self.locust.order_volume_tuple)
        order_side = choice(self.locust.order_side_tuple)

        order = self.client.Order.Order_new(
            symbol="XBTUSD", side=order_side,
            price=order_price, orderQty=order_volume)

        if order:
            with self._lock:
                self.locust.order_cache[order["orderID"]] = (order, auth)

        self.locust.user_auth_queue.put_nowait(user_data)

        if get_env_bool("DELAY_LOOP"):
            time.sleep(random())

    @task(20)
    def order_cancel(self):
        with self._lock:
            origin_cache = self.locust.order_cache
            self.locust.order_cache = dict()

        for orderID in origin_cache.copy().keys():
            order, auth = origin_cache.pop(orderID, (None, None))

            if not order or not auth:
                continue

            logging.info("cancel order: %s", order)

            origin_auth = self.client.authenticator

            self.client.authenticator = auth

            self.client.Order.Order_cancel(orderID=orderID)

            self.client.authenticator = origin_auth

        del origin_cache

    @task(1)
    def order_cancel_all(self):
        with self._lock:
            origin_cache = self.locust.order_cache
            self.locust.order_cache = dict()

        for user_data in self.locust.user_auth_list:
            self.client.change_auth(**user_data)

            self.client.Order.Order_cancelAll()

        del origin_cache


class NGE(NGELocust):
    task_set = Order

    user_auth_list = list()
    user_auth_queue = queue.Queue()

    order_cache = dict()

    order_price_list = list()
    order_side_tuple = ("Sell", "Buy")
    order_volume_tuple = (1, 3, 5, 10, 15, 30)

    def setup(self):
        user_file = path("@/CSV/users.csv")

        print(user_file)

        if not os.path.isfile(user_file):
            raise ValueError("auth file missing.")

        with open(user_file) as f:
            reader = csv.DictReader(f)
            for user_data in reader:
                if (not (user_data["identity"] and user_data["password"])) or (
                        not (user_data["api_key"] and
                             user_data["api_secret"])):
                    logging.warning("invalid auth: %s".format(user_data))
                    continue

                self.user_auth_list.append(user_data)
                self.user_auth_queue.put_nowait(user_data)

        self.order_price_list.extend(
            map(lambda x: float(os.environ.get("BASE_PRICE", 10000)) + 0.5 * x,
                range(1, int(os.environ.get("LEVELS", 50)) + 1, 1)))


if __name__ == "__main__":
    NGE.host = "http://47.103.74.144"
    logging.basicConfig(level=logging.INFO)

    os.environ["BASE_PRICE"] = "10100.0"
    os.environ["LEVELS"] = "100"
    os.environ["DELAY_LOOP"] = "FALSE"

    ins = NGE()

    ins.run()
