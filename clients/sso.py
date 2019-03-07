# coding: utf-8
import requests
import rsa
import re
import binascii
import time

# noinspection PyPackageRequirements
from Crypto.PublicKey import RSA
from collections import OrderedDict
from threading import Event

# noinspection PyPackageRequirements
from locust import Locust, events
# noinspection PyPackageRequirements
from locust.exception import StopLocust

from common.utils import http_request


class UserException(Exception):
    pass


class RegisterExcept(UserException):
    pass


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
    _scheme = "http"
    _host = ("trade", 80)
    _base_uri = "/api/v1"

    _host_public_key = dict()

    _identity_patterns = {
        "email": re.compile(r'[\w.-]+@[\w.-]+'),
        "telephone": re.compile(r'[+\d-]+')
    }

    def __init__(self, schema="", host=(), base_uri="", key_bit=1024):
        self._login = Event()

        if schema:
            self._scheme = schema

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
                  "{key user}\n"
                  "-----END PUBLIC KEY-----"
            False: {key user}
        :return:
        """
        key_string = self._public_key.export_key(pkcs=8).decode()

        if full_format:
            return key_string

        return "".join(key_string.split("\n")[1:-1])

    @property
    def logged(self):
        return self._login.is_set()

    @property
    def api_key(self):
        return self._api_key

    @property
    def api_secret(self):
        neither = ""
        return self._api_secret

    @classmethod
    def host(cls):
        if cls._host[1] != 80:
            template = "{scheme}://{host[0]}:{host[1]}"
        else:
            template = "{scheme}://{host[0]}"

        return template.format(scheme=cls._scheme, host=cls._host)

    @classmethod
    def base_uri(cls):
        return "/{base_uri}/{endpoint}".format(
            base_uri=cls._base_uri.lstrip("/"),
            endpoint=cls.__name__.lower())

    @classmethod
    def base_url(cls):
        return "{host}/{base_uri}".format(
            host=cls.host().rstrip("/"),
            base_uri=cls.base_uri().lstrip("/"))

    @classmethod
    def _get_public_key(cls):
        host_url = cls.base_url()

        if host_url in cls._host_public_key:
            return cls._host_public_key[host_url]

        result = http_request(
            host_url + "/getPublicKey").json()

        pub_key_string = result["result"]

        # deal with invalid base64 padding alignment
        miss_padding = len(pub_key_string) % 4
        if miss_padding:
            pub_key_string = pub_key_string + "=" * (4 - miss_padding)

        # java rsa public key in PKCS#8 format without header & footer
        pub_key_content = ("-----BEGIN PUBLIC KEY-----\n"
                           "{}\n"
                           "-----END PUBLIC KEY-----").format(pub_key_string)

        pub_key = rsa.PublicKey.load_pkcs1_openssl_pem(
            pub_key_content.encode())

        cls._host_public_key[host_url] = pub_key

        return pub_key

    @classmethod
    def _rsa_encrypt(cls, message):
        pub_key = cls._get_public_key()

        if isinstance(message, str):
            message = message.encode()

        return binascii.b2a_base64(
            rsa.encrypt(message, pub_key)).decode().strip()

    @classmethod
    def _get_identity_dict(cls, identity):
        for name, pattern in cls._identity_patterns.items():
            if pattern.match(identity):
                return {name: identity}

        raise ValueError(
            "Invalid identity[{}], valid identity patterns: {}".format(
                identity, ", ".join(cls._identity_patterns.keys())))

    @classmethod
    def register(cls, identity, password, captcha="", invite=""):
        """
        Register user
        :param identity: User identity: email or mobile
        :param password: User password
        :param captcha: Captcha code
        :param invite: Invite code
        :return: Logged in <User> instance if succeed
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
            message = failed_message[result["result"]]
        except KeyError:
            message = "unknown error returned in registration: {}".format(
                result["result"])

        raise RegisterExcept(message)

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

    def login(self, identity, password, captcha=""):
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
            self._login.set()
            self.user_info = result["result"]

        return self.logged

    def logout(self):
        if not self.logged:
            RuntimeWarning("please login first.")

        result = self._request("logout")

        if check_code(result):
            self._login.clear()
            self._session = requests.Session()
            self.user_info = None
            self._api_key = ""
            self._api_secret = ""

        return not self.logged

    def get_api_key(self):
        if not self.logged:
            raise RuntimeWarning("please login first.")

        result = self._request(
            endpoint="getUserSysApiKey",
            data=self.rsa_public_key)

        if check_code(result) and "secret" in result["result"]:
            self._api_key = result["result"]["apiKey"]
            self._api_secret = self._rsa_decrypt(result["result"]["secret"])

        return self.api_key and self.api_secret

    def __repr__(self):
        info = OrderedDict()

        if self.user_info:
            info.update({
                "UserName": self.user_info["userName"],
                "UserID": self.user_info["userId"],
                "Telephone": self.user_info["telephone"],
                "Email": self.user_info["email"]
            })

        return "User<{}>: {}".format(self.base_url(), info)

    def __del__(self):
        self._session.close()


class LocustWrapper(object):
    def __init__(self, client):
        self._client = client

    def __getattr__(self, item):
        origin_attr = getattr(self._client, item)

        def wrapper(*args, **kwargs):
            start_time = time.time()

            try:
                result = origin_attr(*args, **kwargs)
            except Exception as e:
                end_time = time.time()
                total_ms = int((end_time - start_time) * 1000)

                events.request_failure.fire(
                    request_type=self._client.__class__.__name__,
                    name=item, response_time=total_ms,
                    exception=e)
                raise

            end_time = time.time()
            total_ms = int((end_time - start_time) * 1000)

            if not result:
                events.request_failure.fire(
                    request_type=self._client.__class__.__name__,
                    name=item, response_time=total_ms,
                    exception=Exception("{} failed.".format(item)))
            else:
                events.request_success.fire(
                    request_type=self._client.__class__.__name__,
                    name=item, response_time=total_ms,
                    response_length=0)

            return result

        return wrapper


class SSOLocust(Locust):
    def __init__(self):
        super(SSOLocust, self).__init__()

        host_pattern = re.compile(
            r"(?P<scheme>https?)://"
            r"(?P<host>\w[\w.-]*)(?::(?P<port>\d+))?/?")

        if not self.host:
            user = User()
        else:
            match = host_pattern.match(self.host)

            if not match:
                raise StopLocust("Invalid host.")

            result = match.groupdict()

            user = User(schema=result["scheme"],
                        host=(result["host"],
                              int(result["port"] if result["port"] else 80)))

        self.client = LocustWrapper(user)
