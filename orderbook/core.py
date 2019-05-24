# coding: utf-8
__all__ = ("OrderBook", "MBL", "PriceLevel", "Order")

import heapq

from collections import defaultdict

from orderbook.const import (Direction, OrderStatus, OrderType, TimeCondition,
                             create_enum_by_name)
from orderbook.utils import normalize_price, mk_timestamp


class OrderBook(object):
    def __init__(self, symbol: str, tick_price: float, max_depth=-1):
        self._symbol = symbol

        if not isinstance(tick_price, float):
            raise ValueError(
                "invalid tick_price: {}, must be a float.".format(tick_price))

        self._tick_price = tick_price

        if max_depth <= 0:
            raise ValueError(
                "invalid max_depth: {}, must be a positive int."
                .format(max_depth))

        self._max_depth = max_depth

        self._mbl = {
            Direction.Sell: MBL(direction=Direction.Sell, orderbook=self),
            Direction.Buy: MBL(direction=Direction.Buy, orderbook=self)
        }

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def tick_price(self) -> float:
        return self._tick_price

    @property
    def max_depth(self) -> int:
        return self._max_depth

    @property
    def buy_mbl(self):
        return self._mbl[Direction.Buy]

    @property
    def sell_mbl(self):
        return self._mbl[Direction.Sell]

    def __getitem__(self, item):
        pass


class MBL(object):
    def __init__(self, direction: Direction, orderbook: OrderBook):
        self._direction = direction
        self._orderbook = orderbook

        self._price_heap = list()

        self._level_cache = dict()

        self._auth_level_map = defaultdict(list)

    def __check_depth(self):
        if len(self._level_cache) != len(self._price_heap):
            raise RuntimeError(
                "mbl depth miss match in price heap and level cache: \n"
                "price heap: {}\nlevel cache: {}"
                .format(self._price_heap, self._level_cache))

        return len(self._price_heap) > 0

    def __judge_worst_price(self, price):
        if not self._price_heap:
            return True

        switch = {
            Direction.Sell: lambda: price > max(self._price_heap),
            Direction.Buy: lambda: price < min(self._price_heap)
        }

        return switch[self._direction]()

    def __check_overlap(self, price) -> bool:
        switch = {
            Direction.Buy:
                lambda: price >= self._orderbook.sell_mbl.best_price,
            Direction.Sell:
                lambda: price <= self._orderbook.buy_mbl.best_price
        }

        return switch[self._direction]()

    @property
    def best_price(self):
        if self.__check_depth():
            return self._price_heap[0]

    @property
    def best_level(self):
        if self.__check_depth():
            return self._level_cache[self.best_price]

    @property
    def depth(self):
        if self.__check_depth():
            return len(self._price_heap)

    def add_order(self, order):
        order_price = normalize_price(order["price"],
                                      self._orderbook.tick_price)
        order["price"] = order_price

        if order_price not in self._level_cache:
            heapq.heappush(order_price, self._price_heap)

        self._level_cache[order_price].append(order)

    def pop(self):
        pass

    def extend(self, orders):
        pass

    def __contains__(self, item):
        price = normalize_price(item, self._orderbook.tick_price)

        return price in self._level_cache


class PriceLevel(object):
    def __init__(self, price: float, mbl: MBL):
        self._price = price
        self._mbl = mbl

        self._order_list = list()

        self._auth_order_map = defaultdict(list)


class Order(object):
    __slots__ = ("orderID", "clOrdID", "clOrdLinkID", "account", "symbol",
                 "side", "simpleOrderQty", "orderQty", "price",
                 "displayQty", "stopPx", "pegOffsetValue", "pegPriceType",
                 "currency", "settlCurrency", "ordType", "timeInForce",
                 "execInst", "contingencyType", "exDestination",
                 "ordStatus", "triggered", "workingIndicator",
                 "ordRejReason", "simpleLeavesQty", "leavesQty",
                 "simpleCumQty", "cumQty", "avgPx", "multiLegReportingType",
                 "text", "transactTime", "timestamp")

    _column_mapper_ = {
        "side": lambda v: create_enum_by_name(Direction, v),
        "ordStatus": lambda v: create_enum_by_name(OrderStatus, v),
        "ordType": lambda v: create_enum_by_name(OrderType, v),
        "timeInForce": lambda v: create_enum_by_name(TimeCondition, v),
        "timestamp": mk_timestamp,
        "transactTime": mk_timestamp
    }

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            if k in self._column_mapper_:
                v = self._column_mapper_[k](v)

            setattr(self, k, v)

    def __getitem__(self, item):
        try:
            return getattr(self, item)
        except AttributeError:
            raise KeyError("invalid attribute name: {}".format(item))
