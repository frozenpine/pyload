# coding: utf-8
import os

from time import sleep

from bitmex_websocket import BitMEXWebsocket


if __name__ == "__main__":
    os.environ["https_proxy"] = "http://127.0.0.1:1080"

    ws = BitMEXWebsocket(endpoint="https://www.bitmex.com/api/v1",
                         symbol="XBTUSD")

    while ws.ws.sock.connected:
        last_trade = ws.data["trade"][-1]

        print(last_trade)

        sleep(1)
