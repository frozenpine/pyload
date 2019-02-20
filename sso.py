# coding: utf-8

import requests
import rsa
import re
import binascii

from Crypto.PublicKey import RSA


class User(object):
    def __init__(self, schema="http", host=(), base_uri=None):
        self._login = False

        self._schema = schema

        if not host:
            host = ("trade", 80)
        self._host = host

        if not base_uri:
            base_uri = "/api/v1"

        self._base_uri = base_uri

        self._session = requests.Session()

        self._host_public_key = self._get_public_key()

        priv_key = RSA.generate(1024)

        self._private_key = rsa.PrivateKey.load_pkcs1(
            priv_key.export_key())
        self._public_key = priv_key.publickey()

        self._api_key = ""
        self._api_secret = ""
        self._user_info = None

    @property
    def base_url(self):
        return "{schema}://{host[0]}:{host[1]}/{base_uri}/{endpoint}".format(
            schema=self._schema, host=self._host,
            base_uri=self._base_uri.lstrip("/"),
            endpoint=self.__class__.__name__.lower())

    @property
    def rsa_public_key(self):
        key_string = self._public_key.export_key(pkcs=8).decode()

        return "".join(key_string.split("\n")[1:-1])

    @property
    def api_key(self):
        return self._api_key

    @property
    def api_secret(self):
        return self._api_secret

    def _request(self, endpoint, **kwargs):
        response = self._session.post(
            "{}/{}".format(self.base_url, endpoint.lstrip("/")),
            **kwargs)

        if not response.ok:
            raise requests.RequestException(response=response)

        if self._login or "login" == endpoint:
            if "X-Auth-Token" in response.headers:
                self._session.headers.update({
                    "x-auth-token": response.headers["X-Auth-Token"]
                })

            self._session.cookies.update(response.cookies)

        return response.json()

    def _get_public_key(self):
        result = self._request("getPublicKey")

        pub_key_string = result["result"]

        miss_padding = len(pub_key_string) % 4

        if miss_padding:
            pub_key_string = pub_key_string + "=" * (4 - miss_padding)

        pub_key_content = ("-----BEGIN PUBLIC KEY-----\n"
                           "{}\n"
                           "-----END PUBLIC KEY-----").format(pub_key_string)

        pub_key = rsa.PublicKey.load_pkcs1_openssl_pem(
            pub_key_content.encode())

        return pub_key

    def _rsa_encrypt(self, message: [str, bytes]):
        if isinstance(message, str):
            message = message.encode()

        return binascii.b2a_base64(
            rsa.encrypt(message, self._host_public_key)).decode().strip()

    def _rsa_decrypt(self, secret):
        secret = binascii.a2b_base64(secret)

        return rsa.decrypt(secret, self._private_key).decode().strip()

    @staticmethod
    def _check_code(result):
        if "result" in result:
            result = result["result"]

        return "0" == result["code"]

    def get_api_key(self):
        if not self._login:
            raise Exception("please login first.")

        result = self._request(
            endpoint="getUserSysApiKey",
            data=self.rsa_public_key)

        if self._check_code(result) and "secret" in result["result"]:
            self._api_key = result["result"]["apiKey"]
            self._api_secret = self._rsa_decrypt(result["result"]["secret"])

    def login(self, identity, password, capcha=None):
        patterns = {
            "email": re.compile(r'[a-zA-Z.-]+@[\w.-]+'),
            "mobile": re.compile(r'[+\d-]+')
        }

        login_data = {
            "password": self._rsa_encrypt(password),
            "type": "account"
        }

        for name, pattern in patterns.items():
            if pattern.match(identity):
                login_data.update({
                    name: identity
                })

        if len(login_data) <= 2:
            raise ValueError(
                "Invalid identity[{}], valid identity patterns: {}"
                .format(identity, ", ".join(patterns.keys())))

        result = self._request(endpoint="login", json=login_data)

        if self._check_code(result):
            self._login = True
            self._user_info = result["result"]

        return result


if __name__ == "__main__":
    user = User()

    user.login("journeyblue@163.com", "yuanyang")
    user.get_api_key()

    print()
