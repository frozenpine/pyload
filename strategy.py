# coding: utf-8
import logging

from threading import Thread, Event

from bitmex import bitmex

from websocket import WebSocketApp


class MarketSubscriber(object):
    def __init__(self, url="wss://testnet.bitmex.com", proxy=()):
        super(MarketSubscriber, self).__init__()

        self._url = url

        self._connected = Event()

        self._ws = None
        self._ws_tr = None

        self._proxy = proxy

    @property
    def is_running(self):
        if not self._ws:
            return False

        if not (self._ws_tr and self._ws_tr.is_alive()):
            return False

        if not self._connected.is_set():
            return False

        return True

    def connect(self, url=None):
        if url:
            self._url = url

        self._ws = WebSocketApp(url=self._url,
                                on_open=self.__on_close,
                                on_close=self.__on_close,
                                on_error=self.__on_error,
                                on_message=self.__on_message)

        ws_run_kwargs = {
            "ping_interval": 30,
            "ping_timeout": 60,
        }
        if self._proxy:
            ws_run_kwargs.update({
                "http_proxy_host": self._proxy[0],
                "http_proxy_port": self._proxy[1]
            })

        self._ws_tr = Thread(target=self._ws.run_forever,
                             kwargs=ws_run_kwargs)
        self._ws_tr.daemon = True
        self._ws_tr.start()

    def __on_open(self, ws):
        self._connected.set()

        logging.debug(ws)

    def __on_close(self, ws):
        self._connected.clear()

        del ws

    def __on_message(self, ws):
        pass

    def __on_error(self, ws):
        pass
