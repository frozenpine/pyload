# encoding: utf-8
import json
import bitmex


if __name__ == "__main__":
    # order-cancel
    # BITMEX_API_KEY = "45HxJCd3iBD4fszoi1q6JQCo"
    # BITMEX_API_SECRET = "3ZTfOkmLxTVWnmf3oc9WGgcT5EDL-JVr8EOHn79wYLEVQFDF"

    # oder
    BITMEX_API_KEY = "lsxYFbHTkusbuut-TWNoRvGG"
    BITMEX_API_SECRET = "eD2G3zE-zx3-OVy4pLZjc5O8d3ojXSONzwXU5C-OfUCwdq0A"

    client = bitmex.bitmex(
        test=True, api_key=BITMEX_API_KEY, api_secret=BITMEX_API_SECRET)

    result_buy = client.Order.Order_new(
        symbol="XBTUSD", orderQty=10, price=3637.0).result()

    result_sell = client.Order.Order_new(
        symbol="XBTUSD", orderQty=-10, price=3637.0).result()

    print(result_buy, result_sell)
