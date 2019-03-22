# encoding: utf-8

import bitmex


if __name__ == "__main__":
    # order-cancel
    # BITMEX_API_KEY = "45HxJCd3iBD4fszoi1q6JQCo"
    # BITMEX_API_SECRET = "3ZTfOkmLxTVWnmf3oc9WGgcT5EDL-JVr8EOHn79wYLEVQFDF"

    # oder
    BITMEX_API_KEY = "lsxYFbHTkusbuut-TWNoRvGG"
    BITMEX_API_SECRET = "eD2G3zE-zx3-OVy4pLZjc5O8d3ojXSONzwXU5C-OfUCwdq0A"

    CLIENT = bitmex.bitmex(
        test=True, api_key=BITMEX_API_KEY, api_secret=BITMEX_API_SECRET)

    RESULT_BUY, RESPONSE_BUY = CLIENT.Order.Order_new(
        symbol="XBTUSD", orderQty=10, price=3637.0).result()

    RESULT_SELL, RESPONSE_SELL = CLIENT.Order.Order_new(
        symbol="XBTUSD", orderQty=-10, price=3637.0).result()

    print(RESULT_BUY, RESULT_SELL)
