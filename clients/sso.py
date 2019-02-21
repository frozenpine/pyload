# coding: utf-8
import requests
import rsa
import re
import binascii

# noinspection PyPackageRequirements
from Crypto.PublicKey import RSA
from collections import OrderedDict

from common.utils import http_request


def check_code(result):
    if "result" in result:
        result = result["result"]

    if isinstance(result, int):
        return 0 == result

    return "0" == result["code"]


class User(object):
    """
    User model for NGE sso
    """
    _schema = "http"
    _host = ("trade", 80)
    _base_uri = "/api/v1"

    _host_public_key = None

    _identity_patterns = {
        "email": re.compile(r'[a-zA-Z.-]+@[\w.-]+'),
        "telephone": re.compile(r'[+\d-]+')
    }

    def __init__(self, schema: str = "", host: tuple = (),
                 base_uri: str = "", key_bit: int = 1024):
        self._login = False

        if schema:
            self._schema = schema

        if host:
            self._host = host

        if base_uri:
            self._base_uri = base_uri

        self._session = requests.Session()

        priv_key = RSA.generate(key_bit)

        self._private_key = rsa.PrivateKey.load_pkcs1(
            priv_key.export_key())
        self._public_key = priv_key.publickey()

        self._api_key = ""
        self._api_secret = ""
        self.user_info = None

    @property
    def rsa_public_key(self, full_format=False):
        """
        Get user's RSA public key content in PKCS#8 format.
        :param full_format:
            True: "-----BEGIN PUBLIC KEY-----\n"
                  "{key data}\n"
                  "-----END PUBLIC KEY-----"
            False: {key data}
        :return:
        """
        key_string = self._public_key.export_key(pkcs=8).decode()

        if full_format:
            return key_string

        return "".join(key_string.split("\n")[1:-1])

    @property
    def logged(self):
        return self._login

    @property
    def api_key(self):
        return self._api_key

    @property
    def api_secret(self):
        return self._api_secret

    @classmethod
    def base_url(cls):
        return "{schema}://{host[0]}:{host[1]}/{base_uri}/{endpoint}".format(
            schema=cls._schema, host=cls._host,
            base_uri=cls._base_uri.lstrip("/"),
            endpoint=cls.__name__.lower())

    @classmethod
    def _get_public_key(cls):
        result = http_request(
            cls.base_url() + "/getPublicKey").json()

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

    @classmethod
    def _rsa_encrypt(cls, message):
        if not cls._host_public_key:
            cls._host_public_key = cls._get_public_key()

        if isinstance(message, str):
            message = message.encode()

        return binascii.b2a_base64(
            rsa.encrypt(message, cls._host_public_key)).decode().strip()

    @classmethod
    def _get_identity_dict(cls, identity):
        for name, pattern in cls._identity_patterns.items():
            if pattern.match(identity):
                return {name: identity}

        raise ValueError(
            "Invalid identity[{}], valid identity patterns: {}".format(
                identity, ", ".join(cls._identity_patterns.keys())))

    @classmethod
    def register(cls, identity: str, password: str,
                 captcha: str = "", invite: str = ""):
        """
        Register user
        :param identity: User identity: email or mobile
        :param password: User password
        :param captcha: Captcha code
        :param invite: Invite code
        :return: Logged in <User> instance if succeed else None
        """
        register_data = {
            "password": cls._rsa_encrypt(password),
            "confirm": "",
            "verifyCode": captcha,
            "inviteCode": invite
        }

        failed_message = {
            1: "Duplicate user identity: {}".format(identity)
        }

        register_data.update(cls._get_identity_dict(identity))

        result = http_request(
            cls.base_url() + "/register",
            json=register_data).json()

        if check_code(result):
            _user = cls()
            _user.login(identity=identity, password=password)

            return _user

        try:
            print(failed_message[result["result"]])
        except KeyError:
            print("Unknown error while register user: {}".format(identity))

        if 1 == result["result"]:
            return cls

    def _request(self, endpoint, **kwargs):
        response = http_request(
            uri="{}/{}".format(self.base_url(), endpoint.lstrip("/")),
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

    def _rsa_decrypt(self, secret):
        secret = binascii.a2b_base64(secret)

        return rsa.decrypt(secret, self._private_key).decode().strip()

    def login(self, identity: str, password: str, captcha: str = ""):
        """
        Login user
        :param identity:
        :param password:
        :param captcha:
        :return:
        """
        login_data = {
            "password": self._rsa_encrypt(password),
            "type": "account",
            "verifyCode": captcha
        }

        login_data.update(self._get_identity_dict(identity))

        result = self._request(endpoint="login", json=login_data)

        if check_code(result):
            self._login = True
            self.user_info = result["result"]

        return result

    def logout(self):
        if not self._login:
            Exception("please login first.")

        result = self._request("logout")

        if check_code(result):
            self._login = False
            self._session = requests.Session()
            self.user_info = None
            self._api_key = ""
            self._api_secret = ""

    def get_api_key(self):
        if not self._login:
            raise Exception("please login first.")

        result = self._request(
            endpoint="getUserSysApiKey",
            data=self.rsa_public_key)

        if check_code(result) and "secret" in result["result"]:
            self._api_key = result["result"]["apiKey"]
            self._api_secret = self._rsa_decrypt(result["result"]["secret"])

    def __repr__(self):
        info = OrderedDict()

        if self.user_info:
            info.update({
                "UserName": self.user_info["userName"],
                "UserID": self.user_info["userId"],
                "Telephone": self.user_info["telephone"],
                "Email": self.user_info["email"]
            })

        return "User<class 'sso.User'>: {}".format(info)

    def __del__(self):
        if not self._session:
            self._session.close()
