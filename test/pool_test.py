# coding: utf-8

import unittest

from bravado.client import Spec

from clients.nge import NGEClientPool


class PoolTests(unittest.TestCase):
    def test_pool(self):
        pool_size = 5
        pool = NGEClientPool(host="http://localhost", size=pool_size)

        self.assertTrue(isinstance(pool.Order, NGEClientPool.BravadoWrapper))
        self.assertTrue(callable(pool.Order.Order_new))
        self.assertTrue(isinstance(pool.swagger_spec, Spec))
