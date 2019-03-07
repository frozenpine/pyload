# coding: utf-8

import time

from random import sample
from csv import DictWriter

from common.utils import path
from clients.sso import User


user_list = list()


if __name__ == "__main__":
    seed = ("1234567890abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()_+=-")

    start = time.time()
    for idx in range(10000):
        user_data = {
            "identity": "test{:05d}@quantdo.com.cn".format(idx + 1),
            "password": "".join(sample(seed, 8))
        }

        try:
            user = User.register(**user_data)
        except Exception as e:
            print(e)
            continue

        if not user or not user.logged:
            print("register failed: {}".format(user_data))
            continue

        if not user.get_api_key():
            print("get key failed: {}".format(user_data))
            continue

        user_data.update({
            'api_key': user.api_key,
            'api_secret': user.api_secret
        })

        user_list.append(user_data)

    end = time.time()

    with open(path("@/CSV/Users.csv"), mode="w", encoding="utf-8") as f:
        writer = DictWriter(f=f, fieldnames=["identity", "password",
                                             "api_key", "api_secret"])
        writer.writeheader()
        writer.writerows(user_list)

    print("user registered total: {}".format(len(user_list)))
    print("rps: {}".format(round(10000/(end - start))))
