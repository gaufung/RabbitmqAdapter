# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import unittest
import functools
import uuid
import random
from rabbitmq.tornado_adapter import TornadoAdapter
from tornado.gen import coroutine,Return, sleep
from tornado.testing import AsyncTestCase, gen_test
from tornado.queues import Queue
from rabbitmq.rabbitmq_util import make_properties
from concurrent.futures import ThreadPoolExecutor

EXECUTOR = ThreadPoolExecutor(max_workers=10)


class TestTornadoAdapterPublish(AsyncTestCase):
    def setUp(self):
        super(TestTornadoAdapterPublish, self).setUp()
        self._url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        self._adapter = TornadoAdapter(self._url, io_loop=self.io_loop)
        self._result_queue = Queue(maxsize=2)
        self.exchange = "adapter_exchange"
        self.routing_key = "adapter.*"
        self.queue = "adapter_queue"

    @coroutine
    def _process(self, body, back_value):
        self._result_queue.put(body)
        raise Return(back_value)

    @gen_test(timeout=5)
    def test_publish(self):
        success = yield self._adapter.establish_connections()
        self.assertTrue(success)
        expect_body = 'Hello World!'
        yield self._adapter.receive(self.exchange, self.routing_key, self.queue,
                                    functools.partial(self._process, back_value=True))
        yield self._adapter.publish(self.exchange, "adapter.one", expect_body)
        actual = yield self._result_queue.get()
        self.assertEqual(actual, expect_body)

    @gen_test(timeout=5)
    def test_publish_with_reply(self):
        success = yield self._adapter.establish_connections()
        self.assertTrue(success)
        corr_id = str(uuid.uuid4())
        reply_to = "queue_reply_to"
        yield self._adapter.receive(self.exchange, self.routing_key, self.queue,
                                    functools.partial(self._process, back_value="Nice to meet you too!"))
        yield self._adapter.receive(self.exchange, reply_to, reply_to,
                                    functools.partial(self._process, back_value=True))
        yield self._adapter.publish(self.exchange, "adapter.two", "Nice to meet you!",
                                    properties=make_properties(correlation_id=corr_id, reply_to=reply_to))
        actual = yield self._result_queue.get()
        self.assertEqual(actual, "Nice to meet you!")
        actual = yield self._result_queue.get()
        self.assertEqual(actual, "Nice to meet you too!")


class TestTornadoAdapterRpc(AsyncTestCase):
    def setUp(self):
        super(TestTornadoAdapterRpc, self).setUp()
        self._url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        self._adapter = TornadoAdapter(self._url, self.io_loop)
        self._exchange = "adapter_rpc"
        self._routing_key = "fib.*"
        self._queue = "adapter_rpc_queue"

    @coroutine
    def fib(self, body):
        n = int(body)
        result = yield EXECUTOR.submit(self._fib, *(n,))
        raise Return(str(result))

    def _fib(self, n):
        if n < 2:
            return n
        else:
            return self._fib(n - 1) + self._fib(n - 2)

    @gen_test(timeout=10)
    def test_rpc_call(self):
        success = yield self._adapter.establish_connections()
        self.assertTrue(success)
        yield self._adapter.receive(self._exchange, self._routing_key, self._queue, self.fib)
        value = yield self._adapter.rpc(self._exchange,"fib.call", "10")
        self.assertEqual(str(self._fib(10)), value)

    @gen_test(timeout=20)
    def test_rpc_calls(self):
        success = yield self._adapter.establish_connections()
        self.assertTrue(success)
        yield self._adapter.receive(self._exchange, self._routing_key, self._queue, self.fib)
        size = 2000
        values =[random.randint(10, 20) for _ in range(size)]
        expects = [str(self._fib(value)) for value in values]
        actuals = yield [
            self._adapter.rpc(self._exchange, "fib.%d" % value, str(value)) for value in values
        ]
        for expect, actual in zip(expects, actuals):
            self.assertEqual(expect, actual)

    @gen_test(timeout=10)
    def test_rpc_timeout(self):
        success = yield self._adapter.establish_connections()
        self.assertTrue(success)
        yield self._adapter.receive(self._exchange, self._routing_key, self._queue, self.fib)
        with self.assertRaises(Exception):
            yield self._adapter.rpc(self._exchange, "fib.50", "50", 3)


if __name__ == "__main__":
    unittest.main()
