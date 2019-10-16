# coding: utf-8
import requests

from threading import Event

from common.utils import http_request

def check_code(result):
    if "code" in result:
        return 0 == int(result["code"])

    if "result" in result:
        result = result["result"]

    if result is None:
        return False

    if isinstance(result, int):
        return 0 == result

    return True if result else False


class Management:
    _scheme = "http"
    _host = ("admin", 8080)

    def __init__(self, user="admin", password="123456", schema="", host=()):
        self._user = user
        self._pass = password

        self._login = Event()

        self._session = requests.Session()

        if schema:
            self._scheme = schema

        if host:
            self.change_host(*host)

    @classmethod
    def host(cls):
        if cls._host[1] != 80:
            template = "{scheme}://{host[0]}:{host[1]}"
        else:
            template = "{scheme}://{host[0]}"

        return template.format(scheme=cls._scheme, host=cls._host)

    @classmethod
    def change_host(cls, addr, port=80):
        cls._host = (addr, port)

    @property
    def logged(self):
        return self._login.is_set()

    def _request(self, endpoint, **kwargs):
        response = http_request(
            uri="{}/{}".format(self.host(), endpoint.lstrip("/")),
            session=self._session,
            **kwargs
        )

        if self._login or "login" == endpoint:
            if "X-Auth-Token" in response.headers:
                self._session.headers.update({
                    "x-auth-token": response.headers["X-Auth-Token"]
                })

            self._session.cookies.update(response.cookies)

        return response.json()

    def login(self, user="", password=""):
        if user:
            self._user = user
        if password:
            self._pass = password

        login_data = {
            "userName": self._user,
            "password": self._pass
        }

        result = self._request(endpoint="/sso/login", json=login_data)

        if check_code(result):
            self._login.set()

        return self.logged

    def list_instruments(self, ins_status=None):
        if not self.logged:
            raise RuntimeWarning("please login first.")

        if not ins_status:
            result = self._request(
                endpoint="/digital/instrument/findInstrument")
        else:
            result = self._request(
                endpoint="/digital/instrument/findByCondition",
                json={"instrumentStatus": ins_status})

        if check_code(result):
            return result["result"]

        return []

    def sync_instrument(self, force=False):
        if not self.logged:
            raise RuntimeWarning("please login first.")

        result = self._request(
            endpoint="/digital/instrument/beListedInstrument",
            json=({"instrumentStatus": "0"} if not force else {})
        )

        if check_code(result):
            return self.list_instruments(ins_status="1")

        return []


if __name__ == "__main__":
    admin = Management(host=("localhost", 8080))

    if not admin.login():
        print("login failed")
        exit(1)

    print(admin.list_instruments(ins_status="0"))

    print(admin.sync_instrument())

