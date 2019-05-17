# coding: utf-8

import logging
import time
import os
import csv
import queue
import sentry_sdk

from threading import Event
from collections import defaultdict
from random import choice, random

# noinspection PyPackageRequirements
from locust import TaskSet, events, task
from sentry_sdk.integrations.logging import LoggingIntegration

from common.utils import path
from pyload.nge import NGELocust

sentry_logging = LoggingIntegration(
    level=logging.INFO,        # Capture info and above as breadcrumbs
    event_level=logging.ERROR  # Send errors as events
)
sentry_sdk.init(integrations=[sentry_logging])

logging.basicConfig(level=logging.INFO)


hatching_event = Event()
hatching_event.clear()

stop_event = Event()
stop_event.clear()

USER_AUTH_QUEUE = queue.Queue()


def hatch_complete(**kwargs):
    hatching_event.set()


def start_hatching(**kwargs):
    hatching_event.clear()
    stop_event.clear()


def stop_hatching(**kwargs):
    stop_event.set()


events.hatch_complete += hatch_complete
events.locust_start_hatching += start_hatching
events.locust_stop_hatching += stop_hatching


class Order(TaskSet):
    # noinspection PyMethodMayBeStatic
    def on_start(self):
        hatching_event.wait()
        logging.info("hatch complete.")

    @task(50)
    def order_new(self):
        user_data = USER_AUTH_QUEUE.get()

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
            self.locust.order_cache[order["orderID"]] = (order, auth)

        USER_AUTH_QUEUE.put_nowait(user_data)

        time.sleep(random())

    @task(1)
    def order_cancel(self):
        for orderID in self.locust.order_cache.copy().keys():
            order, auth = self.locust.order_cache.pop(orderID, (None, None))

            if not order or not auth:
                continue

            logging.info("cancel order: %s", order)

            origin_auth = self.client.authenticator

            self.client.authenticator = auth

            self.client.Order.Order_cancel(orderID=orderID)

            self.client.authenticator = origin_auth


class NGE(NGELocust):
    task_set = Order

    order_cache = defaultdict()

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
                if not (user_data["identity"] and user_data["password"]) or \
                        not (user_data["api_key"] and user_data["api_secret"]):
                    logging.warning("invalid auth: %s".format(user_data))
                    continue

                USER_AUTH_QUEUE.put_nowait(user_data)

        self.order_price_list.extend(
            map(lambda x: 7800 + 0.5 * x, range(1, 51, 1)))
