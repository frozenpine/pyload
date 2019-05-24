# coding: utf-8

import unittest
import time

from datetime import datetime

from orderbook.core import Order
from orderbook.const import TimeCondition, OrderType


class OrderTest(unittest.TestCase):
    def test_create(self):
        self.assertRaises((TypeError, AttributeError), Order, foo="bar")

        order = Order(orderID="abc123",
                      timeInForce="GoodTillCancel",
                      ordType="MarketIfTouched",
                      timestamp=time.time() * 1000,
                      transactTime="2019-05-24T17:07:16.123Z")

        self.assertEqual(TimeCondition.GoodTillCancel, order["timeInForce"])
        self.assertEqual(OrderType.MarketIfTouched, order["ordType"])
        self.assertIsInstance(order.timestamp, datetime)
        self.assertIsInstance(order.transactTime, datetime)
