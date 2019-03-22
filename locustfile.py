# coding: utf-8

import logging
import os
import csv
import queue
import sentry_sdk

from random import random, randint
from threading import Event

# noinspection PyPackageRequirements
from locust import TaskSet, task, events
from sentry_sdk.integrations.logging import LoggingIntegration

from common.utils import path
from clients.nge import NGELocust

sentry_logging = LoggingIntegration(
    level=logging.INFO,        # Capture info and above as breadcrumbs
    event_level=logging.ERROR  # Send errors as events
)
sentry_sdk.init(integrations=[sentry_logging])


hatching_event = Event()
hatching_event.clear()

stop_event = Event()
stop_event.clear()


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
    def on_start(self):
        user_data = self.locust.user_data_queue.get()

        self.locust.client = self.client.get_swagger_client(**user_data)

        hatching_event.wait()

        self.locust.user_data_queue.put_nowait(user_data)

    @task
    def order(self):
        while not stop_event.isSet():
            rand_price = round(random(), 2)
            rand_qty = randint(1, 10)

            for side in (1, -1):
                self.client.Order.Order_new(
                    symbol="XBTUSD",
                    orderQty=rand_qty * side,
                    price=rand_price)


class NGE(NGELocust):
    task_set = Order

    user_data_queue = queue.Queue()

    def setup(self):
        user_file = path("@/CSV/users.csv")

        if os.path.isfile(user_file):
            with open(user_file) as f:
                reader = csv.DictReader(f)
                for user_data in reader:
                    self.user_data_queue.put_nowait(user_data)
        else:
            for idx in range(1000):
                user_data = {
                    "identity": "{:05d}@qq.com".format(idx+1),
                    "password": "123456"
                }

                self.user_data_queue.put_nowait(user_data)
