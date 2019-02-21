# coding: utf-8

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

    result, response = client.Order.Order_new(
        symbol="XBTUSD", orderQty=1, price=3536.7).result()

    if 200 == response.status_code:
        print(result)
    else:
        print(response.reason)

    result, response = client.Order.Order_new(
        symbol="XBTUSD", orderQty=-1, price=3537.7).result()

    if 200 == response.status_code:
        print(result)
    else:
        print(response.reason)
