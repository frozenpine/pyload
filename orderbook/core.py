# coding: utf-8
__all__ = ("OrderBook", "MBL", "PriceLevel", "PriceHeap")

import heapq
import sys

from typing import List, Dict, Optional
from collections import defaultdict
from functools import reduce
from weakref import ref, ReferenceType

from orderbook import logger
from orderbook.const import (Direction, create_enum_by_name)
from orderbook.utils import normalize_price
from orderbook.structure import Order


class PriceHeap(object):
    def __init__(self, direction: Direction):
        self._direction = -direction.value

        self._worst_price = None

        self._heap = list()

    @property
    def best_price(self) -> float:
        if self._heap:
            return self._heap[0] * self._direction

        if self._direction > 0:
            return sys.float_info.max
        else:
            return 0.0

    @property
    def worst_price(self) -> float:
        if self._worst_price is not None:
            return self._worst_price * self._direction

        if self._direction > 0:
            return 0.0
        else:
            return sys.float_info.max

    def push(self, price: float):
        price = self._direction * price

        if self._worst_price is None or price > self._worst_price:
            self._worst_price = price

        heapq.heappush(self._heap, price)

    def pop(self) -> float:
        return self._direction * heapq.heappop(self._heap)

    def remove(self, price: float):
        try:
            self._heap.remove(price * self._direction)
        except ValueError as e:
            logger.warning(e)
            return

        heapq.heapify(self._heap)

    def top(self, n=25) -> List[float]:
        n = min(n, len(self._heap))

        return [p * self._direction for p in heapq.nsmallest(n, self._heap)]

    def __getitem__(self, item):
        return self._heap[item] * self._direction

    def __len__(self):
        return len(self._heap)

    def __bool__(self):
        if self._heap:
            return True

        return False


class OrderBook(object):
    def __init__(self, symbol: str, tick_price: float, max_depth=-1):
        self._symbol = symbol

        if not isinstance(tick_price, float):
            raise ValueError(
                "invalid tick_price: {}, must be a float.".format(tick_price))

        self._tick_price = tick_price

        self._max_depth = max_depth

        self._mbl = {
            Direction.Sell: MBL(direction=Direction.Sell, orderbook=self),
            Direction.Buy: MBL(direction=Direction.Buy, orderbook=self)
        }

        self._order_price_index_map = dict()

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

    def get_price_direction(self, price) -> Optional[Direction]:
        if price >= self.sell_mbl.best_price:
            return Direction.Sell

        if price <= self.buy_mbl.best_price:
            return Direction.Buy

        return None

    def in_gap(self, price: float) -> bool:
        return self.buy_mbl.best_price < price < self.sell_mbl.best_price

    def overlap_levels(self, price) -> (Optional[Direction], list):
        direction = self.get_price_direction(price)
        overlapped = list()

        while not self.in_gap(price):
            level = self._mbl[direction].pop_level()

            if level:
                overlapped.append(level)

        return direction, overlapped

    def __getitem__(self, item):
        """
        Get mbl or price level by value
        if item is Direction or Direction string, return mbl
        if item is float price, return price level or None(if level not exist)
        :param item: direction instance/str or price
        :return: mbl or price level
        :raise ValueError
        """
        if isinstance(item, Direction):
            return self._mbl[item]

        if isinstance(item, str):
            direction = create_enum_by_name(Direction, item)

            return self._mbl[direction]

        if isinstance(item, float):
            direction = self.get_price_direction(item)

            if direction:
                return self._mbl[direction][item]

        raise ValueError("invalid index key for mbl side: {}".format(item))


class MBL(object):
    def __init__(self, direction: Direction, orderbook: OrderBook):
        self._direction = direction
        self._orderbook = orderbook

        self._price_heap = PriceHeap(direction=direction)

        self._level_cache = defaultdict(
            lambda: PriceLevel(price=0.0, mbl=self))

    def __check_depth(self):
        assert len(self._level_cache) == len(self._price_heap), \
            ("mbl depth miss match in price heap and level cache: \n"
             "price heap: {}\nlevel cache: {}").format(
                self._price_heap, self._level_cache)

        return len(self._price_heap) > 0

    def __judge_worst_price(self, price):
        if not self._price_heap:
            return True

        switch = {
            Direction.Sell: lambda: price > self._price_heap.worst_price,
            Direction.Buy: lambda: price < self._price_heap.worst_price
        }

        return switch[self._direction]()

    def __get_counterparty(self):
        return self._orderbook[self._direction.flap()]

    @property
    def best_price(self) -> float:
        """
        Get best level's price
        :return: best level price
        :raise RuntimeError
        """

        return self._price_heap.best_price

    @property
    def best_level(self):
        """
        Get best price level in mbl
        Highest price level in Buy side
        Lowest price level in Sell side
        :return: price level
        :rtype Union[PriceLevel, None]
        :raise RuntimeError
        """

        return self._level_cache.get(self.best_price, None)

    @property
    def depth(self) -> int:
        """
        Level depth of mbl
        :return: level count
        :raise RuntimeError
        """
        if self.__check_depth():
            return len(self._price_heap)

        return 0

    def append_level(self, level):
        """
        Append exist price level to current mbl
        :param level: price level
        :return:
        """
        if not level.level_price:
            raise ValueError(
                "invalid level price[{}]".format(level.level_price))

        if level.level_price in self._level_cache:
            raise ValueError("level with price[{}] already exists.".format(
                level.level_price))

        self._level_cache[level.level_price] = level
        self._price_heap.push(level.level_price)

    def delete_level(self, price):
        """
        Delete a price level by price
        :param price: level price
        :return:
        :rtype Optional(PriceLevel)
        """

        if price not in self._level_cache:
            return

        level = self._level_cache.pop(price)

        self._price_heap.remove(price)

        return level

    def add_order(self, order: Order) -> int:
        if order["side"] != self._direction:
            raise ValueError(
                "order[{}]'s direction[{}] mis-match with current mbl[]"
                .format(order["orderID"], order["side"], self._direction))

        normalized_price = normalize_price(order["price"],
                                           self._orderbook.tick_price)
        order.price = normalized_price

        if normalized_price not in self._level_cache:
            self._price_heap.push(normalized_price)

        return self._level_cache[normalized_price].push_order(order)

    def trade_volume(self, volume: int) -> (int, Dict[float,
                                                      List[ReferenceType]]):
        remained_volume = volume
        traded_levels = dict()

        while remained_volume > 0 and self.__check_depth():
            best_level = self.best_level

            remained_volume, traded_levels[
                best_level.level_price] = best_level.trade_volume(
                remained_volume)

        return remained_volume, traded_levels

    def pop_level(self):
        """
        Pop best price level
        :return: PriceLevel
        :rtype Optional(PriceLevel)
        """
        if self.__check_depth():
            return None

        return self._level_cache.pop(self._price_heap.pop())

    def __contains__(self, price):
        """
        Check if price exists in mbl
        :param price:
        :return: exists
        """
        price = normalize_price(price, self._orderbook.tick_price)

        return price in self._level_cache

    def __getitem__(self, price):
        """
        Get price level by price
        :param price: level price
        :return: price level
        :rtype PriceLevel
        """

        price = normalize_price(price, self._orderbook.tick_price)

        if price in self._level_cache:
            return self._level_cache[price]

        return None


class PriceLevel(object):
    """
    This is a FIFO order queue presents a price level in MBL
    """
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

    @property
    def size(self) -> int:
        if not self.__check_depth():
            return 0

        if self.count == 1:
            return self._order_list[0].orderQty

        result = reduce(lambda x, y: x.orderQty + y.orderQty, self._order_list)

        return result

    def __add_to_mbl(self):
        if self._price and self._price not in self._mbl:
            self._mbl.append_level(self)

    def __check_depth(self) -> bool:
        assert len(self._order_list) == len(self._order_index_map), \
            ("order list size mis-match with order index map "
             "in current level[{}].\n"
             "order list: {}\norder index map:{}").format(
                self._price, self._order_list, self._order_index_map)

        if len(self._order_list) > 0:
            return True

        self._mbl.delete_level(self.level_price)

        return False

    def __verify_order_price(self, order):
        if not self._price:
            self._price = order["price"]

            self.__add_to_mbl()

            return

        if order["price"] != self._price:
            raise ValueError(
                "order[{}]'s price[{}] mis-match with current level[{}]"
                .format(order["orderID"], order["price"], self._price))

    def __renew_order_index_map(self, start_index, reduce_idx):
        for order in self._order_list[start_index:]:
            self._order_index_map[order.orderID] -= reduce_idx

    def __slice_index(self, idx) -> ReferenceType:
        order = self._order_list.pop(idx)

        self._order_index_map.pop(order.orderID)

        self.__renew_order_index_map(idx, 1)

        self.__check_depth()

        return ref(order)

    def __slice_left(self, index, keep_index=False) -> List[ReferenceType]:
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

        if order["orderID"] in self._order_index_map:
            raise ValueError(
                "order[{}] exists in current level[{}]\n"
                "origin order: {}\nnew order: {}".format(
                    order["orderID"], self.level_price,
                    self._order_list[self._order_index_map[order["orderID"]]],
                    order))

        self._order_list.append(order)
        self._order_index_map[order["orderID"]] = len(self._order_list) - 1

        return self._order_index_map[order["orderID"]]

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

        try:
            idx = self._order_index_map[order["orderID"]]
        except KeyError:
            raise ValueError(
                "order[{}] not exists under current level[{}]".format(
                    order["orderID"], self._price))

        self._order_list[idx] = order

        return idx

    def remove_order(self, order: Order) -> int:
        """
        Remove a order from current level
        if order's price mis-match with current level,
        an exception will raise
        :param order: Order
        :return: order index in level
        :raise ValueError
        """

        self.__verify_order_price(order)

        idx, _ = self.remove_order_by_id(order["orderID"])

        return idx

    def remove_order_by_id(self, order_id: str) -> (int, ReferenceType):
        """
        Remove a order by its orderID
        if order id not exist, (-1, None) will return
        :param order_id: order id
        :return: order index, removed order's weak ref
        """

        if not self.__check_depth():
            return -1, None

        if order_id not in self._order_index_map:
            return -1, None

        idx = self._order_index_map.get(order_id, -1)

        if idx >= 0:
            order_ref = self.__slice_index(idx)
        else:
            order_ref = None

        return idx, order_ref

    def trade_volume(self, volume: int) -> (int, List[ReferenceType]):
        """
        Trade specified volume size
        :param volume: volume size to be traded
        :return: remained volume size, traded order's weakref list
        """
        remained_volume = volume

        idx = 0

        for order in self._order_list:
            remained_volume -= order["leavesQty"]

            if remained_volume <= 0:
                break

            idx += 1

        traded_orders = self.__slice_left(index=idx,
                                          keep_index=remained_volume < 0)
        if remained_volume < 0:
            self._order_list[0]["leavesQty"] = abs(remained_volume)

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
