# coding: utf-8

import time

try:
    from clients.nge import nge
    from clients.sso import User
except ImportError:
    import sys
    import os

    CURRENT_DIR = os.path.dirname(sys.argv[0])

    sys.path.append(os.path.join(CURRENT_DIR, "../"))

    from clients.nge import nge
    from clients.sso import User

from random import random, randint, choice


if __name__ == "__main__":
    user = User()

    user.login(identity="yuanyang@quantdo.com.cn", password="yuanyang")

    if not user.logged:
        user = User.register(identity="yuanyang@quantdo.com.cn",
                             password="yuanyang")

    user.get_api_key()

    if not user.api_key or not user.api_secret:
        raise ValueError("fail to get user's apiKey & apiSecret.")

    client = nge(test=True, api_key=user.api_key, api_secret=user.api_secret)

    start = time.time()
    for _ in range(10000):
        rand_price = round(random(), 2)
        rand_qty = randint(1, 10)
        rand_side = choice((1, -1))

        result, response = client.Order.Order_new(
            symbol="XBTUSD", orderQty=rand_qty * rand_side,
            price=3536 + rand_price).result()

        if 200 != response.status_code:
            print(response.reason)

    end = time.time()

    last = end - start

    print("insert rate: {}/s".format(round(10000/last), 2))
    print("total time: {} s".format(last))
