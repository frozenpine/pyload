# coding: utf-8


class NGEWebsocket(object):
    def __init__(self, host="http://trade", symbol="XBTUSD",
                 api_key=None, api_secret=None):
        self._host = host
        self._symbol_list = [symbol]

        self._api_key = api_key
        self._api_secret = api_secret

        self._order_book = None
