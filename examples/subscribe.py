# coding: utf-8
from datetime import datetime
from time import sleep

from bitmex_websocket import BitMEXWebsocket
from clients.nge_websocket import NGEWebsocket


if __name__ == "__main__":
    # os.environ["https_proxy"] = "http://127.0.0.1:1080"
    #
    # ws = BitMEXWebsocket(endpoint="https://www.bitmex.com/api/v1",
    #                      symbol="XBTUSD")

    ws = NGEWebsocket(endpoint="http://3.112.97.161/api/v1",
                      symbol="XBTUSD")
    last_trade = None

    while ws.ws.sock.connected:
        latest_trade = ws.data["trade"][-1]

        if latest_trade != last_trade:
            print(datetime.now(), latest_trade)

            last_trade = latest_trade

        sleep(1)
