# coding: utf-8

import unittest
import time

from datetime import datetime

from orderbook.core import Order, PriceLevel, MBL, OrderBook
from orderbook.const import TimeCondition, OrderType, Direction


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

        # required column missing
        self.assertRaises(AttributeError, Order)


class PriceLevelTest(unittest.TestCase):
    _SYMBOL = "XBTUSD"
    _TICK_PRICE = 0.5
    _LEVEL_PRICE = 256.0

    def setUp(self) -> None:
        self.ob = OrderBook(symbol=self._SYMBOL, tick_price=self._TICK_PRICE)
        self.mbl = MBL(direction=Direction.Buy, orderbook=self.ob)
        self.level = PriceLevel(price=self._LEVEL_PRICE, mbl=self.mbl)

    def test_getitem(self):
        order1 = Order(orderID="123", price=self._LEVEL_PRICE)
        order2 = Order(orderID="456", price=self._LEVEL_PRICE)

        with self.assertRaisesRegex(IndexError,
                                    r"no order exists on current level"):
            _ = self.level[0]

        self.level.push_order(order1)
        self.level.push_order(order2)

        with self.assertRaises(ValueError):
            _ = self.level["foo"]

        self.assertEqual(order1, self.level[0])
        self.assertEqual(order2, self.level[1])

    def test_append(self):
        order1 = Order(orderID="123", price=self._LEVEL_PRICE)
        order2 = Order(orderID="456", price=self._LEVEL_PRICE+1)

        self.assertEqual(0, self.level.push_order(order1))

        self.assertEqual(1, self.level.count)

        self.assertEqual(self._LEVEL_PRICE, self.level.level_price)

        # order price mis-match with level price
        with self.assertRaisesRegex(ValueError,
                                    r"mis-match with current level"):
            self.level.push_order(order2)

        # order already exist in current level
        with self.assertRaisesRegex(ValueError, r"exists in current level"):
            self.level.push_order(order1)

    def test_modify(self):
        order1 = Order(orderID="123", price=self._LEVEL_PRICE)
        order2 = Order(orderID="123", price=self._LEVEL_PRICE)
        order3 = Order(orderID="456", price=self._LEVEL_PRICE)

        # order level is empty
        self.assertEqual(-1, self.level.modify_order(order1))

        self.level.push_order(order1)

        idx = self.level.modify_order(order2)
        self.assertEqual(order2, self.level[idx])

        # order not exist under current level
        with self.assertRaisesRegex(ValueError,
                                    "not exists under current level"):
            self.level.modify_order(order3)

    def test_pop(self):
        order1 = Order(orderID="123", price=self._LEVEL_PRICE)
        order2 = Order(orderID="456", price=self._LEVEL_PRICE)

        self.assertEqual(-1, self.level.pop_order(order1))

        self.level.push_order(order1)

        with self.assertRaisesRegex(ValueError,
                                    r"not exists under current level"):
            self.level.pop_order(order2)

        self.level.push_order(order2)

        self.assertEqual(0, self.level.pop_order(order1))

        self.assertEqual(order2, self.level[0])

    def test_trade(self):
        order1 = Order(orderID="123", price=self._LEVEL_PRICE, size=1)
        order2 = Order(orderID="456", price=self._LEVEL_PRICE, size=2)
        order3 = Order(orderID="foo", price=self._LEVEL_PRICE, size=3)
        order4 = Order(orderID="bar", price=self._LEVEL_PRICE, size=4)

        self.level.push_order(order1)
        self.level.push_order(order2)
        self.level.push_order(order3)
        self.level.push_order(order4)

        self.assertTrue(self._LEVEL_PRICE in self.mbl)

        remained, orders = self.level.trade_volume(3)
        self.assertEqual(0, remained)
        self.assertEqual([order1, order2], [r() for r in orders])

        remained, orders = self.level.trade_volume(4)
        self.assertEqual(0, remained)
        self.assertEqual([order3], [r() for r in orders])
        self.assertEqual(order4, self.level[0])
        self.assertEqual(3, self.level[0]["size"])

        remained, orders = self.level.trade_volume(5)
        self.assertEqual(2, remained)
        self.assertEqual([order4], [r() for r in orders])

        self.assertFalse(self._LEVEL_PRICE in self.mbl)
