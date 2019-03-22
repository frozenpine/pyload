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

from bravado_core.exception import SwaggerMappingError


if __name__ == "__main__":
    user = User()

    # if not user.login(
    #         identity="yuanyang@quantdo.com.cn", password="yuanyang"):
    #     user = User.register(identity="yuanyang@quantdo.com.cn",
    #                          password="yuanyang")
    # if not user.login(
    #         identity="journeyblue@163.com", password="123456"):
    #     user = User.register(identity="journeyblue@163.com",
    #                          password="123456")
    # if not user.login(
    #         identity="sonny.frozenpine@gmail.com", password="yuanyang"):
    #     user = User.register(identity="sonny.frozenpine@gmail.com",
    #                          password="yuanyang")
    if not user.login(
            identity="yuanyang@frozenpine.dev", password="yuanyang"):
        user = User.register(identity="yuanyang@frozenpine.dev",
                             password="yuanyang")

    user.get_api_key()

    if not user.api_key or not user.api_secret:
        raise ValueError("fail to get user's apiKey & apiSecret.")

    client = nge(api_key=user.api_key, api_secret=user.api_secret)

    # api_key = "L68h3Fn3QjRaKqhaU82Y"
    # api_secret = ("2AWH0b47nVxWxRZ14q7x8KH4pAgRp20n96oLHC6PQlI6WT4oU"
    #               "fJFZiQh0429t7p7I633j3vv55Q9DiuNESdOvnmKux6n01Ogj3y")
    #
    # client = nge(api_key=api_key, api_secret=api_secret)

    for _ in range(10):
        try:
            result, response = client.Order.Order_new(
                symbol="XBTUSD", orderQty=1, price=3536.7).result()
        except SwaggerMappingError as e:
            raise

        print(result, response)

        result, response = client.Order.Order_new(
            symbol="XBTUSD", orderQty=-1, price=3536.7).result()

        print(result, response)

    exit()

    start = time.time()
    for _ in range(5000):
        rand_price = round(random(), 2)
        rand_qty = randint(1, 10)
        # rand_side = choice((1, -1))

        for side in (1, -1):
            result, response = client.Order.Order_new(
                symbol="XBTUSD", orderQty=rand_qty * side,
                price=3536 + rand_price).result()

            if response.status_code != 200:
                print(response.reason)

    end = time.time()

    last = end - start

    print("insert rate: {}/s".format(round(10000/last, 2)))
    print("total time: {} s".format(last))
