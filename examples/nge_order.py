# coding: utf-8
import os
import sys
import csv

from queue import Queue
from random import choice

try:
    from common.utils import path
    from clients.nge import nge, NGEAPIKeyAuthenticator
    # from clients.sso import User
except ImportError:
    CURRENT_DIR = os.path.dirname(sys.argv[0])
    sys.path.append(os.path.join(CURRENT_DIR, "../"))

    from common.utils import path
    from clients.nge import nge, NGEAPIKeyAuthenticator
    # from clients.sso import User


if __name__ == "__main__":
    host = "http://localhost"

    client = nge(host=host)

    auth_queue = Queue()

    base_price = float(os.environ.get("BASE_PRICE", 10000))
    levels = int(os.environ.get("LEVELS", 50))
    order_total = int(os.environ.get("ORDER_TOTAL", 10000))

    user_file = path("@/CSV/users.csv")
    order_file = path("@/CSV/orders.csv")

    price_list = list(map(
        lambda x: base_price + 0.5 * x,
        range(1, levels + 1, 1)
    ))
    volume_list = [1, 3, 5, 10, 15, 30, 50, 100]
    directions = ("Buy", "Sell")

    if not os.path.isfile(order_file):
        with open(order_file, mode='w+', encoding='utf-8', newline='') as f:
            order_writer = csv.DictWriter(
                f, fieldnames=("symbol", "side", "price", "orderQty"))
            order_writer.writeheader()
            order_writer.writerows([{
                'symbol': 'XBTUSD',
                'side': choice(directions),
                'price': choice(price_list),
                'orderQty': choice(volume_list)
            } for _ in range(order_total)])

    with open(user_file, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for user_data in reader:
            if not user_data["api_key"] or not user_data["api_secret"]:
                continue

            auth = NGEAPIKeyAuthenticator(host=host,
                                          api_key=user_data["api_key"],
                                          api_secret=user_data["api_secret"])

            auth_queue.put_nowait(auth)

    with open(order_file, encoding="utf8") as f:
        rd = csv.DictReader(f)
        for order in rd:
            auth = auth_queue.get()
            auth_queue.put_nowait(auth)

            client.swagger_spec.http_client.authenticator = auth

            result, rsp = client.Order.Order_new(**order).result()

            print(rsp, result)
