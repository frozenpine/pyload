# coding: utf-8
import os
import sys
import time

from random import sample
from csv import DictWriter

try:
    from common.utils import path
    from clients.sso import User
except ImportError:
    CURRENT_DIR = os.path.dirname(sys.argv[0])
    sys.path.append(os.path.join(CURRENT_DIR, "../"))

    from common.utils import path
    from clients.sso import User


user_list = list()


if __name__ == "__main__":
    seed = ("1234567890abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()_+=-")

    User.change_host("192.168.14.242", 8201)

    user_total = int(os.environ.get("USER_TOTAL", 10))

    start = time.time()
    for idx in range(user_total):
        user_data = {
            "identity": "test{:05d}@115bit.com".format(idx + 1),
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

    with open(path("@/CSV/users.csv"), mode="w", encoding="utf-8") as f:
        writer = DictWriter(f=f, fieldnames=["identity", "password",
                                             "api_key", "api_secret"])
        writer.writeheader()
        writer.writerows(user_list)

    print("user registered total: {}".format(len(user_list)))
    print("rps: {}".format(round(user_total / (end - start))))
