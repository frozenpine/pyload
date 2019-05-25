# coding: utf-8
__all__ = ("OrderBook", "MBL", "PriceLevel", "Order")

import heapq
import sys

from collections import defaultdict
from weakref import ref

from orderbook.const import (Direction, OrderStatus, OrderType, TimeCondition,
                             create_enum_by_name)
from orderbook.utils import normalize_price, make_datetime


class DataModel(object):
    _column_mapper_ = dict()
    _required_columns_ = set()

    def __init__(self, **kwargs):
        require_check = self._required_columns_.copy()

        for k, v in kwargs.items():
            if k in self._column_mapper_:
                v = self._column_mapper_[k](v)

            setattr(self, k, v)

            require_check.discard(k)

        if require_check:
            raise AttributeError("columns[{}] is required.".format(
                ",".join(require_check)))

    def __getitem__(self, item):
        try:
            return getattr(self, item)
        except AttributeError:
            raise KeyError("invalid attribute name: {}".format(item))

    def __setitem__(self, key, value):
        try:
            return setattr(self, key, value)
        except AttributeError:
            raise KeyError("invalid attribute name: {}".format(key))


class Order(DataModel):
    __slots__ = ("orderID", "clOrdID", "clOrdLinkID",
                 "account", "symbol", "side", "simpleOrderQty", "orderQty",
                 "price", "displayQty", "stopPx", "pegOffsetValue",
                 "pegPriceType", "currency", "settlCurrency", "ordType",
                 "timeInForce", "execInst", "contingencyType", "exDestination",
                 "ordStatus", "triggered", "workingIndicator",
                 "ordRejReason", "simpleLeavesQty", "leavesQty",
                 "simpleCumQty", "cumQty", "avgPx", "multiLegReportingType",
                 "text", "transactTime", "timestamp")

    _required_columns_ = {"orderID"}

    _column_mapper_ = {
        "side": lambda v: create_enum_by_name(Direction, v),
        "ordStatus": lambda v: create_enum_by_name(OrderStatus, v),
        "ordType": lambda v: create_enum_by_name(OrderType, v),
        "timeInForce": lambda v: create_enum_by_name(TimeCondition, v),
        "timestamp": make_datetime,
        "transactTime": make_datetime
    }

    def __eq__(self, other):
        return self.orderID == other.orderID


class Trade(DataModel):
    __slots__ = ("timestamp", "symbol", "side", "size",
                 "price", "tickDirection", "trdMatchID", "grossValue",
                 "homeNotional", "foreignNotional")

    _column_mapper_ = {
        "timestamp": make_datetime,
        "tickDirection": lambda v: create_enum_by_name(Direction, v),
        "side": lambda v: create_enum_by_name(Direction, v)
    }


class OrderBook(object):
    def __init__(self, symbol: str, tick_price: float, max_depth=-1):
        self._symbol = symbol

        if not isinstance(tick_price, float):
            raise ValueError(
                "invalid tick_price: {}, must be a float.".format(tick_price))

        self._tick_price = tick_price

        # if max_depth <= 0:
        #     raise ValueError(
        #         "invalid max_depth: {}, must be a positive int."
        #         .format(max_depth))

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
        """
        :rtype MBL
        """
        return self._mbl[Direction.Buy]

    @property
    def sell_mbl(self):
        """
        :rtype MBL
        """
        return self._mbl[Direction.Sell]


class MBL(object):
    def __init__(self, direction: Direction, orderbook: OrderBook):
        self._direction = direction
        self._orderbook = orderbook

        self._price_heap = list()

        self._level_cache = defaultdict(
            lambda: PriceLevel(price=0.0, mbl=self))

        self._auth_level_map = defaultdict(list)

    def __check_depth(self):
        assert len(self._level_cache) == len(self._price_heap), \
            ("mbl depth miss match in price heap and level cache: \n"
             "price heap: {}\nlevel cache: {}").format(
                self._price_heap, self._level_cache)

        return 0 <= len(self._price_heap) < self._orderbook.max_depth

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
    def best_price(self) -> float:
        """
        Get best level's price
        :return: best level price
        :raise RuntimeError
        """

        empty_level_price = {
            Direction.Buy: 0.0,
            Direction.Sell: sys.float_info.max
        }

        if len(self._price_heap) > 0:
            return self._price_heap[0]

        return empty_level_price[self._direction]

    @property
    def best_level(self):
        """
        Get best price level in mbl
        Highest price level in Buy side
        Lowest price level in Sell side
        :return: price level
        :rtype PriceLevel
        :raise RuntimeError
        """

        if len(self._price_heap) > 0:
            return self._level_cache[self.best_price]

        return None

    @property
    def depth(self) -> int:
        """
        Level depth of mbl
        :return: level count
        :raise RuntimeError
        """
        if self.__check_depth():
            return len(self._price_heap)

    def add_level(self, level):
        if not level.level_price:
            raise ValueError(
                "invalid level price[{}]".format(level.level_price))

        if level.level_price in self._level_cache:
            raise ValueError("level with price[{}] already exists.".format(
                level.level_price))

        self._level_cache[level.level_price] = level
        heapq.heappush(self._price_heap, level.level_price)

    def delete_level(self, price):
        if price in self._level_cache:
            self._level_cache.pop(price)

            self._price_heap.remove(price)

            heapq.heapify(self._price_heap)

    def add_order(self, order: Order):
        normalized_price = normalize_price(order["price"],
                                           self._orderbook.tick_price)
        order["price"] = normalized_price

        if normalized_price not in self._level_cache:
            heapq.heappush(normalized_price, self._price_heap)

        self._level_cache[normalized_price].push_order(order)

    def __contains__(self, price):
        price = normalize_price(price, self._orderbook.tick_price)

        return price in self._level_cache

    def __getitem__(self, price):
        """
        Get price level by price
        :param price: level price
        :return: price level
        :rtype PriceLevel
        """
        try:
            return self._level_cache[price]
        except KeyError:
            return None


class PriceLevel(object):
    def __init__(self, price: float, mbl: MBL):
        self._price = price
        self._mbl = mbl

        self._order_list = list()

        self._order_index_map = dict()

        self.__add_to_mbl()

    @property
    def level_price(self) -> float:
        """
        Current level's price
        :return: level price
        """

        return self._price

    @property
    def count(self) -> int:
        """
        Order count of current price level
        :return: order count
        """

        if not self.__check_depth():
            return 0

        return len(self._order_list)

    def __add_to_mbl(self):
        if self._price and self._price not in self._mbl:
            self._mbl.add_level(self)

    def __check_depth(self) -> bool:
        assert len(self._order_list) == len(self._order_index_map), \
            ("order list size mis-match with order index map "
             "in current level[{}].\n"
             "order list: {}\norder index map:{}").format(
                self._price, self._order_list, self._order_index_map)

        if len(self._order_list) > 0:
            return True

        self._mbl.delete_level(self._price)

        return False

    def __verify_order_price(self, order):
        if not self._price:
            self._price = order.price

            self.__add_to_mbl()

            return

        if order.price != self._price:
            raise ValueError(
                "order[{}]'s price[{}] mis-match with current level[{}]"
                .format(order.orderID, order.price, self._price))

    def __get_order_idx(self, order):
        try:
            return self._order_index_map[order.orderID]
        except KeyError:
            raise ValueError(
                "order[{}] not exists under current level[{}]".format(
                    order.orderID, self._price))

    def __renew_order_index_map(self, start_index, reduce):
        for order in self._order_list[start_index:]:
            self._order_index_map[order.orderID] -= reduce

    def __slice_index(self, idx):
        order = self._order_list.pop(idx)

        self._order_index_map.pop(order.orderID)

        self.__renew_order_index_map(idx, 1)

        self.__check_depth()

        return ref(order)

    def __slice_left(self, index, keep_index=False):
        if not keep_index:
            index += 1

        sliced_orders = self._order_list[:index]
        self._order_list = self._order_list[index:]

        for order in sliced_orders:
            self._order_index_map.pop(order.orderID)

        self.__renew_order_index_map(0, index)

        self.__check_depth()

        return [ref(o) for o in sliced_orders]

    def push_order(self, order: Order) -> int:
        """
        Append order to current price level
        :param order: Order
        :return: order index in current level
        :raise ValueError
        """

        self.__verify_order_price(order)

        if order.orderID in self._order_index_map:
            raise ValueError(
                "order[{}] exists in current level[{}]\n"
                "origin order: {}\nnew order: {}".format(
                    order.orderID, self.level_price,
                    self._order_list[self._order_index_map[order.orderID]],
                    order))

        self._order_list.append(order)
        self._order_index_map[order.orderID] = len(self._order_list) - 1

        return self._order_index_map[order.orderID]

    def modify_order(self, order: Order) -> int:
        """
        Modify order under current price level
        :param order: Order
        :return:
        :raises ValueError, RuntimeError
        """
        if not self.__check_depth():
            return -1

        self.__verify_order_price(order)

        idx = self.__get_order_idx(order)

        self._order_list[idx] = order

        return idx

    def pop_order(self, order: Order) -> int:
        """
        Pop a order from current level
        :param order: Order
        :return: order index in level
        """
        if not self.__check_depth():
            return -1

        self.__verify_order_price(order)

        idx = self.__get_order_idx(order)

        self.__slice_index(idx)

        return idx

    def trade_volume(self, volume: int) -> (int, list):
        """
        Trade specified volume size
        :param volume: volume size to be traded
        :return: remained volume size, traded order's weakref list
        """
        remained_volume = volume

        idx = 0

        for order in self._order_list:
            remained_volume -= order.size

            if remained_volume <= 0:
                break

            idx += 1

        traded_orders = self.__slice_left(index=idx,
                                          keep_index=remained_volume < 0)
        if remained_volume < 0:
            self._order_list[0].size = abs(remained_volume)

        return max(0, remained_volume), traded_orders

    def __getitem__(self, idx) -> Order:
        if not self.__check_depth():
            raise IndexError(
                "no order exists on current level[{}]"
                .format(self._price))

        if not isinstance(idx, int):
            raise ValueError("index type must be integer.")

        if idx >= len(self._order_list):
            raise IndexError("index[{}] out of range.".format(idx))

        return self._order_list[idx]
