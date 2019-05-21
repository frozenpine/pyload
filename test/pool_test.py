# coding: utf-8

import unittest

from clients.nge import NGEClientPool


class PoolTests(unittest.TestCase):
    def test_pool(self):
        pool_size = 5
        pool = NGEClientPool(host="http://localhost", size=pool_size)

        # for i in range(pool_size+5):
        #     pool.Order.Order_new(symbol="XBTUSD",
        #                          price=7816.5, orderQty=1,
        #                          side="Buy").result()

        self.assertTrue(isinstance(pool.Order, NGEClientPool.BravadoWrapper))
        self.assertTrue(callable(pool.Order.Order_new))
