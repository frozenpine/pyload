# coding utf-8

import logging
import csv


class AuthCache(object):
    def __init__(self, auth_file):
        self._auth_file = auth_file

        self._auth_list = list()

        self._auth_cache = dict()

        self._counter = 0

    @property
    def length(self):
        return len(self._auth_list)

    def _import_auth_from_file(self):
        if self._auth_file == "":
            logging.error(
                "auth file path is invalid: \"{}\"".format(self._auth_file))

            return

        with open(self._auth_file) as f:
            reader = csv.DictReader(f)

            # for rec in reader:

