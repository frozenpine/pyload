# coding: utf-8
import logging
import os
import threading
import csv

from time import sleep
from queue import Queue

from bitmex import bitmex
from bitmex_websocket import BitMEXWebsocket
from bravado.exception import HTTPBadRequest, HTTPUnauthorized
from bravado_core.exception import SwaggerError

from clients.nge import nge, NGEAPIKeyAuthenticator
from common.utils import path


HOST = ("192.168.1.23", 80)

logging.basicConfig(level=logging.INFO)

RUNNING_FLAG = threading.Event()


def host_url(scheme="http", host=("trade", 80)):
    if isinstance(host, str):
        return "{}://{}".format(scheme, host)

    if isinstance(host, (list, tuple)):
        if len(host) < 2 or host[1] == 80:
            return "{}://{}".format(scheme, host[0])

        return "{0}://{1[0]}:{1[1]}".format(scheme, host)

    raise ValueError("invalid host: {}".format(host))


MBL_LOCAL = {
    "Buy": dict(),
    "Sell": dict()
}

AUTH_LIST = list()
AUTH_QUEUE = Queue()

os.environ["https_proxy"] = "http://127.0.0.1:1080"


def handle_exception(ex):
    if not isinstance(ex, HTTPBadRequest):
        logging.exception(ex)
        return

    logging.error(ex.swagger_result)


def init_auth(user_file):
    with open(user_file) as f:
        reader = csv.DictReader(f)
        for user_data in reader:
            if not user_data["api_key"] or not user_data["api_secret"]:
                continue

            auth = NGEAPIKeyAuthenticator(host=host_url(host=HOST),
                                          api_key=user_data["api_key"],
                                          api_secret=user_data["api_secret"])

            AUTH_LIST.append(auth)

    logging.info(
        "Total {} user loaded.".format(len(AUTH_LIST)))


def get_mbl(symbol="XBTUSD", depth=25):
    client = bitmex(test=False)

    mbl_online, _ = client.OrderBook.OrderBook_getL2(
        symbol=symbol, depth=depth).result()

    buy = [order for order in mbl_online if order["side"] == "Buy"]
    sell = [order for order in mbl_online if order["side"] == "Sell"]

    return sell, buy


def make_mbl(client, side, orders):
    for auth in AUTH_LIST:
        client.swagger_spec.http_client.authenticator = auth

        try:
            logging.info(
                "getting history orders for: {}".format(auth.api_key))
            history_orders, _ = client.Order.Order_getOrders(
                symbol="XBTUSD", count=100).result()
        except HTTPUnauthorized as e:
            logging.error(e.swagger_result)

            continue

        AUTH_QUEUE.put_nowait(auth)

        for order in [o for o in history_orders if
                      o["ordStatus"] != "Filled" and
                      o["side"] == side]:
            MBL_LOCAL[side][order["price"]] = (order, auth)

    for order in orders:
        exist_order, origin_auth = MBL_LOCAL[side].pop(
            order["price"], (None, None))

        if exist_order:
            if order["size"] != exist_order["orderQty"]:
                client.swagger_spec.http_client.authenticator = origin_auth

                try:
                    client.Order.Order_amend(
                        orderID=exist_order["orderID"],
                        orderQty=abs(order["size"])).result()
                except (SwaggerError, HTTPBadRequest) as e:
                    handle_exception(e)
                else:
                    exist_order["orderQty"] = order["size"]

            MBL_LOCAL[side][order["price"]] = (exist_order, origin_auth)

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
                symbol=order["symbol"],
                orderQty=abs(order["size"]),
                side=order["side"],
                price=order["price"]).result()

            MBL_LOCAL[side][order["price"]] = (result, new_auth)
        except (SwaggerError, HTTPBadRequest) as e:
            handle_exception(e)


def cancel_tail_orders(client, side, prices):
    for price in prices:
        order, origin_auth = MBL_LOCAL[side][price]

        client.swagger_spec.http_client.authenticator = origin_auth

        client.Order.Order_cancel(orderID=order["orderID"])


def modify_origin(client, side, market_data):
    for market in market_data:
        origin_order, origin_auth = MBL_LOCAL[side].pop(
            market["price"], (None, None))

        if not origin_order:
            new_auth = AUTH_QUEUE.get()

            client.swagger_spec.http_client.authenticator = new_auth

            AUTH_QUEUE.put_nowait(new_auth)

            if market["size"] != 0:
                new_order, _ = client.Order.Order_new(
                    symbol=market["symbol"],
                    orderQty=abs(market["size"]),
                    price=market["price"],
                    side=side).result()

                MBL_LOCAL[side][market["price"]] = (new_order, new_auth)

            continue

        client.swagger_spec.http_client.authenticator = origin_auth

        if market["size"] == 0:
            try:
                client.Order.Order_cancel(
                    orderID=origin_order["orderID"]).result()
            except (SwaggerError, HTTPBadRequest) as e:
                handle_exception(e)

            continue

        if market["size"] != origin_order["orderQty"]:
            try:
                client.Order.Order_amend(
                    orderID=origin_order["orderID"],
                    orderQty=abs(market["size"])).result()
            except (SwaggerError, HTTPBadRequest) as e:
                handle_exception(e)

                continue

            origin_order["ordQty"] = market["size"]

            MBL_LOCAL[side][market["price"]] = (origin_order, origin_auth)


def orderbook_follower(event, client, ws):
    event.wait()

    while event.is_set():
        market_depth = ws.market_depth()

        sell = sorted([m for m in market_depth if m["side"] == "Sell"],
                      key=lambda x: x["price"])[:50]

        buy = sorted([m for m in market_depth if m["side"] == "Buy"],
                     key=lambda x: x["price"], reverse=True)[:50]

        cancel_tail_orders(client, "Sell",
                           [p for p in MBL_LOCAL["Sell"] if
                            p > sell[-1]["price"]])
        cancel_tail_orders(client, "Buy",
                           [p for p in MBL_LOCAL["Buy"] if
                            p < buy[-1]["price"]])

        modify_origin(client, "Sell", sell)
        modify_origin(client, "Buy", buy)

        sleep(1)


def trade_follower(event, client, ws):
    event.wait()

    while event.is_set():
        sleep(1)

        tick = ws.get_ticker()

        last_price = tick["last"]

        new_auth = AUTH_QUEUE.get()

        client.swagger_spec.http_client.authenticator = new_auth

        AUTH_QUEUE.put_nowait(new_auth)

        if last_price in MBL_LOCAL["Buy"]:
            client.swagger_spec.http_client.authenticator = new_auth

            client.Order.Order_new(
                symbol="XBTUSD",
                orderQty=min(int(MBL_LOCAL["Buy"][last_price][0][
                                     "orderQty"] * 0.3),
                             1),
                price=last_price,
                side="Sell").result()

            continue
            # return

        if last_price in MBL_LOCAL["Sell"]:
            client.Order.Order_new(
                symbol="XBTUSD",
                orderQty=min(int(MBL_LOCAL["Sell"][last_price][0][
                                     "orderQty"] * 0.1),
                             1),
                price=last_price,
                side="Buy").result()

            continue
            # return

        client.Order.Order_new(
            symbol="XBTUSD",
            orderQty=10,
            price=last_price,
            side="Sell").result()

        client.Order.Order_new(
            symbol="XBTUSD",
            orderQty=10,
            price=last_price,
            side="Buy",
            timeInForce="FillOrKill").result()


def market_maker(client):
    ws = BitMEXWebsocket(endpoint="https://www.bitmex.com/api/v1",
                         symbol="XBTUSD")

    ob_tr = threading.Thread(target=orderbook_follower,
                             args=(RUNNING_FLAG, client, ws))
    ob_tr.daemon = True
    ob_tr.start()

    td_tr = threading.Thread(target=trade_follower,
                             args=(RUNNING_FLAG, client, ws))
    td_tr.daemon = True
    td_tr.start()

    # sleep(5)

    while(ws.ws.sock.connected):
        sleep(10)

        # orderbook_follower(RUNNING_FLAG, client, ws)
        #
        # trade_follower(RUNNING_FLAG, client, ws)

        RUNNING_FLAG.set()

    RUNNING_FLAG.clear()


def main():
    client = nge(host=host_url(host=HOST))

    init_auth(path("@/CSV/users.csv"))

    sell, buy = get_mbl(depth=50)

    make_mbl(client, "Sell", sell)
    make_mbl(client, "Buy", buy)

    market_maker(client)


if __name__ == "__main__":
    main()

    RUNNING_FLAG.clear()
