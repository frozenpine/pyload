# coding: utf-8
__all__ = ("Order", "Trade")

from datetime import datetime

from orderbook.utils import make_datetime
from orderbook.const import (create_enum_by_name, Direction, OrderStatus,
                             OrderType, TimeCondition)


class DataModel(object):
    __slots__ = dict()

    _column_mapper_ = dict()
    _required_columns_ = set()

    def __init__(self, **kwargs):
        missing_columns = self._required_columns_ - kwargs.keys()
        if missing_columns:
            raise AttributeError("columns[{}] is required.".format(
                ",".join(missing_columns)))

        for col, default in self.__slots__.items():
            if col not in kwargs:
                setattr(self, col, default)
                continue

            value = kwargs[col]
            if col in self._column_mapper_:
                value = self._column_mapper_[col](value)

            setattr(self, col, value)

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
    __slots__ = {"orderID": "", "clOrdID": "", "clOrdLinkID": "",
                 "account": "", "symbol": "", "side": Direction.Buy,
                 "simpleOrderQty": 0.0, "orderQty": 0, "price": 0.0,
                 "displayQty": 0, "stopPx": 0.0, "pegOffsetValue": 0.0,
                 "pegPriceType": "", "currency": "", "settlCurrency": "",
                 "ordType": OrderType.Limit,
                 "timeInForce": TimeCondition.GoodTillCancel, "execInst": "",
                 "contingencyType": "", "exDestination": "",
                 "ordStatus": OrderStatus.New, "triggered": "",
                 "workingIndicator": False, "ordRejReason": "",
                 "simpleLeavesQty": 0.0, "leavesQty": 0, "simpleCumQty": 0.0,
                 "cumQty": 0, "avgPx": 0.0, "multiLegReportingType": "",
                 "text": "", "transactTime": None, "timestamp": datetime.now()}

    _required_columns_ = {"orderID"}

    _column_mapper_ = {
        "side": lambda v: create_enum_by_name(Direction, v),
        "ordStatus": lambda v: create_enum_by_name(OrderStatus, v),
        "ordType": lambda v: create_enum_by_name(OrderType, v),
        "timeInForce": lambda v: create_enum_by_name(TimeCondition, v),
        "timestamp": make_datetime,
        "transactTime": make_datetime
    }

    def __init__(self, **kwargs):
        super(Order, self).__init__(**kwargs)

        if "leavesQty" not in kwargs:
            setattr(self, "leavesQty", self["orderQty"])

    def __eq__(self, other):
        return self["orderID"] == other["orderID"]


class Trade(DataModel):
    __slots__ = {"timestamp": datetime.now(), "symbol": "",
                 "side": Direction.Buy, "size": 0, "price": 0.0,
                 "tickDirection": Direction.Sell, "trdMatchID": "",
                 "grossValue": 0, "homeNotional": 0.0, "foreignNotional": 0.0}

    _column_mapper_ = {
        "timestamp": make_datetime,
        "tickDirection": lambda v: create_enum_by_name(Direction, v),
        "side": lambda v: create_enum_by_name(Direction, v)
    }

    _required_columns_ = {"symbol", "timestamp"}

    def __eq__(self, other):
        return (self["trdMatchID"] == other["trdMatchID"] and
                self["side"] == other["side"])
