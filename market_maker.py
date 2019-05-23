# coding: utf-8
import logging
import os
import threading
import csv
import sys
import signal
import pprint

from time import sleep
from queue import Queue

from bitmex import bitmex
from bitmex_websocket import BitMEXWebsocket
from bravado.exception import HTTPBadRequest, HTTPUnauthorized
from bravado_core.exception import SwaggerError

from clients.nge import nge, NGEAPIKeyAuthenticator
from common.utils import path


HOST = ("3.112.97.161", 80)

SYMBOL = "XBTUSD"

ORDERBOOK_DEPTH = 50

SIZE_SCALE = 0.01

LOOP_DELAY = 5

AUTH_TOTAL = 50

USE_PROXY = False
PROXY = "http://127.0.0.1:1080"

logging.basicConfig(level=logging.INFO)


def host_url(scheme="http", host=("trade", 80)):
    if isinstance(host, str):
        return "{}://{}".format(scheme, host)

    if isinstance(host, (list, tuple)):
        if len(host) < 2 or host[1] == 80:
            return "{}://{}".format(scheme, host[0])

        return "{0}://{1[0]}:{1[1]}".format(scheme, host)

    raise ValueError("invalid host: {}".format(host))


AUTH_LIST = list()
AUTH_QUEUE = Queue()


def handle_exception(ex):
    if not isinstance(ex, HTTPBadRequest):
        logging.exception(ex)
        return

    logging.error(ex.swagger_result)


def print_mbl_depth(mbl):
    pprint.pprint(mbl.keys(), indent=2)


def init_auth(user_file, count=None):
    user_count = 0

    with open(user_file) as f:
        reader = csv.DictReader(f)
        for user_data in reader:
            if not user_data["api_key"] or not user_data["api_secret"]:
                continue

            user_count += 1

            if count and 0 < count < user_count:
                break

            auth = NGEAPIKeyAuthenticator(host=host_url(host=HOST),
                                          api_key=user_data["api_key"],
                                          api_secret=user_data["api_secret"])

            AUTH_LIST.append(auth)

    logging.info(
        "Total {} user loaded.".format(len(AUTH_LIST)))


def scale_size(size):
    return max(int(abs(size) * SIZE_SCALE), 1)


def switch_side(side):
    side_switch = {
        "Buy": "Sell",
        "Sell": "Buy"
    }

    return side_switch[side]


def get_bitmex_mbl(symbol="XBTUSD", depth=25, is_test=False):
    if USE_PROXY:
        os.environ["https_proxy"] = PROXY

    client = bitmex(test=is_test)

    mbl_online, _ = client.OrderBook.OrderBook_getL2(symbol=symbol,
                                                     depth=depth).result()

    os.environ.pop("https_proxy", None)

    buy = [order for order in mbl_online if order["side"] == "Buy"]
    sell = [order for order in mbl_online if order["side"] == "Sell"]

    return sell, buy


def sync_orders(client, symbol, mbl, side):
    for auth in AUTH_LIST:
        client.swagger_spec.http_client.authenticator = auth

        try:
            logging.info(
                "getting history orders for: {}".format(auth.api_key))
            history_orders, _ = client.Order.Order_getOrders(
                symbol=symbol, count=100).result()
        except HTTPUnauthorized as e:
            logging.error(e.swagger_result)

            continue

        AUTH_QUEUE.put_nowait(auth)

        finished_orders = mbl[side].copy()

        for order in [o for o in history_orders if
                      o["ordStatus"] not in ("Filled", "Canceled") and
                      o["side"] == side]:
            mbl[side][order["price"]] = (order, auth)

            finished_orders.pop(order["price"], None)

        for finished_price in finished_orders.keys():
            mbl[side].pop(finished_price)


def make_mbl(client, symbol, mbl, side, orders):
    sync_orders(client, symbol, mbl, side)

    for order in orders:
        exist_order, origin_auth = mbl[side].pop(
            order["price"], (None, None))

        if exist_order:
            if order["size"] != exist_order["orderQty"]:

                client.swagger_spec.http_client.authenticator = origin_auth

                try:
                    client.Order.Order_amend(
                        orderID=exist_order["orderID"],
                        orderQty=scale_size(order["size"])).result()
                except (SwaggerError, HTTPBadRequest) as e:
                    handle_exception(e)
                    mbl[side].pop(order["price"], None)
                else:
                    exist_order["orderQty"] = order["size"]

            mbl[side][order["price"]] = (exist_order, origin_auth)

            continue

        new_auth = AUTH_QUEUE.get()

        client.swagger_spec.http_client.authenticator = new_auth

        AUTH_QUEUE.put_nowait(new_auth)

        try:
            # client.swagger_spec.http_client.authenticator
            # client.swagger_spec.origin_url
            if order["size"] == 0:
                continue

            result, _ = client.Order.Order_new(
                symbol=symbol,
                orderQty=scale_size(order["size"]),
                side=order["side"],
                price=order["price"]).result()

            mbl[side][order["price"]] = (result, new_auth)
        except (SwaggerError, HTTPBadRequest) as e:
            handle_exception(e)


def trim_orders(client, mbl, side, prices):
    for price in prices:
        order, origin_auth = mbl[side][price]

        client.swagger_spec.http_client.authenticator = origin_auth

        client.Order.Order_cancel(orderID=order["orderID"])


def modify_or_make_new(client, symbol, mbl, side, market_data):
    for market in market_data:
        origin_order, origin_auth = mbl[side].pop(
            market["price"], (None, None))

        if not origin_order:
            new_auth = AUTH_QUEUE.get()

            client.swagger_spec.http_client.authenticator = new_auth

            AUTH_QUEUE.put_nowait(new_auth)

            if market["size"] != 0:
                new_order, _ = client.Order.Order_new(
                    symbol=symbol, side=side,
                    orderQty=scale_size(market["size"]),
                    price=market["price"]).result()

                mbl[side][market["price"]] = (new_order, new_auth)

            continue

        client.swagger_spec.http_client.authenticator = origin_auth

        if market["size"] == 0:
            try:
                client.Order.Order_cancel(
                    orderID=origin_order["orderID"]).result()
            except (SwaggerError, HTTPBadRequest) as e:
                handle_exception(e)
            finally:
                mbl[side].pop(origin_order["price"], None)

            continue

        if market["size"] != origin_order["orderQty"]:
            try:
                client.Order.Order_amend(
                    orderID=origin_order["orderID"],
                    orderQty=scale_size(market["size"])).result()
            except (SwaggerError, HTTPBadRequest) as e:
                handle_exception(e)
                mbl[side].pop(origin_order["price"], None)

                continue

            origin_order["ordQty"] = market["size"]

            mbl[side][market["price"]] = (origin_order, origin_auth)


def wait_for_data(running, ws):
    running.wait()

    while running.is_set() and ("trade" not in ws.data or
                                "orderBookL2" not in ws.data):
        sleep(1)


def orderbook_follower(client, symbol, mbl, ws):
    is_trim_price = {
        "Sell": lambda p, p_list: p > p_list[-1]["price"] or
        p < p_list[0]["price"],
        "Buy": lambda p, p_list: p > p_list[0]["price"] or
        p < p_list[-1]["price"]
    }

    market_depth = ws.market_depth()

    sell = sorted([m for m in market_depth if m["side"] == "Sell"],
                  key=lambda x: x["price"])[:ORDERBOOK_DEPTH]

    buy = sorted([m for m in market_depth if m["side"] == "Buy"],
                 key=lambda x: x["price"], reverse=True)[:ORDERBOOK_DEPTH]

    # 取消对手方重叠价格
    trim_orders(
        client, mbl, "Buy",
        [price for price in mbl["Buy"].keys() if
         price >= sell[0]["price"]])
    # 取消多余挂单
    trim_orders(
        client, mbl, "Sell",
        [price for price in mbl["Sell"].keys() if
         is_trim_price["Sell"](price, sell)])

    trim_orders(
        client, mbl, "Sell",
        [price for price in mbl["Sell"].keys() if
         price <= buy[0]["price"]])

    trim_orders(
        client, mbl, "Buy",
        [price for price in mbl["Buy"].keys() if
         is_trim_price["Buy"](price, buy)])

    modify_or_make_new(client, symbol, mbl, "Sell", sell)
    modify_or_make_new(client, symbol, mbl, "Buy", buy)


def trade_follower(client, symbol, mbl, ws, last_trade):
    side_switch = {
        "Buy": "Sell",
        "Sell": "Buy"
    }

    latest_trade = ws.data["trade"][-1]

    if last_trade and last_trade["timestamp"] == latest_trade["timestamp"]:
        return

    last_trade.update(**latest_trade)

    new_auth = AUTH_QUEUE.get()

    client.swagger_spec.http_client.authenticator = new_auth

    AUTH_QUEUE.put_nowait(new_auth)

    price = last_trade["price"]
    side = last_trade["side"]
    order_qty = scale_size(last_trade["size"])

    if price in mbl[side_switch[side]]:
        client.swagger_spec.http_client.authenticator = new_auth

        client.Order.Order_new(
            symbol=symbol, side=side,
            price=price, orderQty=order_qty).result()

        return

    client.Order.Order_new(
        symbol=symbol, side=side_switch[side],
        orderQty=order_qty, price=price).result()

    client.Order.Order_new(
        symbol=symbol, side=side,
        price=price, orderQty=order_qty,
        timeInForce="FillOrKill").result()


def market_maker(running, symbol, client, mbl):
    running.wait()

    if USE_PROXY:
        os.environ["https_proxy"] = PROXY

    ws = BitMEXWebsocket(endpoint="https://www.bitmex.com/api/v1",
                         symbol=symbol)

    wait_for_data(running, ws)

    last_trade = dict()

    count = 0

    try:
        while ws.ws.sock.connected and running.is_set():
            if count % 10 == 9:
                sync_orders(client=client, symbol=symbol,
                            mbl=mbl, side="Buy")
                sync_orders(client=client, symbol=symbol,
                            mbl=mbl, side="Sell")

            count += 1

            orderbook_follower(client=client, symbol=symbol, mbl=mbl, ws=ws)

            trade_follower(client=client, symbol=symbol, mbl=mbl, ws=ws,
                           last_trade=last_trade)

            sleep(LOOP_DELAY)
    finally:
        os.environ.pop("https_proxy", None)


def main(running, client, symbol, mbl):
    running.wait()

    sell, buy = get_bitmex_mbl(symbol=symbol, depth=ORDERBOOK_DEPTH)

    make_mbl(client=client, symbol=symbol, mbl=mbl, side="Sell", orders=sell)
    make_mbl(client=client, symbol=symbol, mbl=mbl, side="Buy", orders=buy)

    while running.is_set():
        market_maker(running=running, symbol=symbol, client=client, mbl=mbl)

        logging.warning(
            "websocket disconnected, wait {} seconds to reconnect.".format(
                LOOP_DELAY))

        sleep(LOOP_DELAY)


def exit_func(sig, frame):
    print("terminate signal[{}] received: {}".format(sig, frame))

    running_flag.clear()


def cancel_all(client):
    for auth in AUTH_LIST:
        client.swagger_spec.http_client.authenticator = auth

        try:
            client.Order.Order_cancelAll().result()
        except (SwaggerError, HTTPBadRequest) as e:
            handle_exception(e)


if __name__ == "__main__":
    running_flag = threading.Event()

    init_auth(path("@/CSV/users.csv"), AUTH_TOTAL)

    client_instance = nge(host=host_url(host=HOST))

    if len(sys.argv) > 1 and sys.argv[1] == "cancel":
        cancel_all(client_instance)

        exit()

    signal.signal(signal.SIGABRT, exit_func)
    signal.signal(signal.SIGTERM, exit_func)
    signal.signal(signal.SIGINT, exit_func)

    mbl_local = {
        "Buy": dict(),
        "Sell": dict()
    }

    running_flag.set()

    main(running=running_flag, client=client_instance,
         symbol=SYMBOL, mbl=mbl_local)

    running_flag.clear()
