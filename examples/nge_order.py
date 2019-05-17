# coding: utf-8

import csv

from queue import Queue

try:
    from common import path
    from clients.nge import nge, NGEAPIKeyAuthenticator
    from clients.sso import User
except ImportError:
    import sys
    import os

    CURRENT_DIR = os.path.dirname(sys.argv[0])

    sys.path.append(os.path.join(CURRENT_DIR, "../"))

    from common import path
    from clients.nge import nge, NGEAPIKeyAuthenticator
    from clients.sso import User


if __name__ == "__main__":
    host = "http://192.168.1.23"

    client = nge(host=host)

    auth_queue = Queue()

    user_file = path("@/CSV/users.csv")

    with open(user_file) as f:
        reader = csv.DictReader(f)
        for user_data in reader:
            if not user_data["api_key"] or not user_data["api_secret"]:
                continue

            auth = NGEAPIKeyAuthenticator(host=host,
                                          api_key=user_data["api_key"],
                                          api_secret=user_data["api_secret"])

            auth_queue.put_nowait(auth)

    # for
