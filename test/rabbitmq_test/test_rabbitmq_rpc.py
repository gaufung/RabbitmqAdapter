# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import unittest
import uuid
import time
import random
from rabbitmq.rabbitmq_rpc import AsyncRabbitMQ
from rabbitmq.rabbitmq_util import make_properties
from tornado.gen import coroutine,Return, sleep
from tornado.testing import AsyncTestCase, gen_test
from tornado.queues import Queue
from concurrent.futures import ThreadPoolExecutor

EXECUTOR = ThreadPoolExecutor(max_workers=4)


class TestAsyncRabbitMQPublish(AsyncTestCase):
    def setUp(self):
        super(TestAsyncRabbitMQPublish, self).setUp()
        self._url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        self._client = AsyncRabbitMQ(self._url, io_loop=self.io_loop)
        self._exchange = "test_asyncrabbitmq_exchange"
        self._queue_name = "test_asyncrabbitmq_queue"
        self._result_queue = Queue(maxsize=10)
        self._fib_queue = Queue(maxsize=1)

    def _processMessage(self, channel, method, props, body):
        self._result_queue.put(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        raise Return(True)

    @gen_test(timeout=10)
    def test_publish(self):
        yield self._client.wait_connected()
        yield self._client.consume(self._exchange, self._queue_name,"dog.*", self._processMessage)
        yield self._client.publish(self._exchange, "dog.yellow", "A yellow dog")
        actual = yield self._result_queue.get()
        self.assertEqual(actual,  "A yellow dog")

    @gen_test(timeout=10)
    def test_concurrent_publish(self):
        yield self._client.wait_connected()
        yield self._client.consume(self._exchange, self._queue_name, "dog.*", self._processMessage)
        yield [
            self._client.publish(self._exchange, "dog.yellow", "a yellow dog"),
            self._client.publish(self._exchange, "dog.red", "a red dog"),
            self._client.publish(self._exchange, "dog.blue", "a blue dog"),
            self._client.publish(self._exchange, "dog.green", "a green dog"),
            self._client.publish(self._exchange, "cat.yellow", "a yellow cat"), # this message will be discarded
            self._client.publish(self._exchange, "dog.colorful", "a colorful dog"),
        ]
        result_set = set(["a yellow dog", "a red dog", "a blue dog",
                             "a green dog", "a colorful dog"])
        for _ in range(5):
            actual = yield self._result_queue.get()
            self.assertTrue(actual in result_set)

    @coroutine
    def _process(self, channel, method, props, body):
        n = int(body)
        result = self._fib(n)
        if props is not None:
            channel.basic_publish(exchange=self._exchange,
                                  routing_key=props.reply_to,
                                  properties=make_properties(correlation_id=props.correlation_id),
                                  body=str(result))
        channel.basic_ack(delivery_tag=method.delivery_tag)
        raise Return(True)

    def _fib(self, n):
        if n < 2:
            return n
        else:
            return self._fib(n - 1) + self._fib(n - 2)

    @coroutine
    def _fib_back(self, channel, method, props, body):
        self._fib_queue.put(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        raise Return(True)

    @gen_test(timeout=10)
    def test_publish_with_reply(self):
        fib_back_queue = "fibnacci_call_back"
        corr_id = str(uuid.uuid4())
        yield self._client.wait_connected()
        yield self._client.consume(self._exchange, self._queue_name, "fib.*", self._process)
        yield self._client.consume(self._exchange, fib_back_queue, fib_back_queue, self._fib_back)
        yield self._client.publish(self._exchange, "fib.call", "10", properties=make_properties(
                                    correlation_id=corr_id, reply_to=fib_back_queue))
        actual = yield self._fib_queue.get()
        expect = str(self._fib(10))
        self.assertEqual(actual, expect)


class TestAsyncRabbitMQCall(AsyncTestCase):
    def setUp(self):
        super(TestAsyncRabbitMQCall, self).setUp()
        self._url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        self._client = AsyncRabbitMQ(self._url, io_loop=self.io_loop)
        self._client = AsyncRabbitMQ(self._url, io_loop=self.io_loop)
        self._exchange = "test_asyncrabbitmq_exchange"
        self._queue_name = "test_asyncrabbitmq_queue"
        self._client =  AsyncRabbitMQ(self._url, io_loop=self.io_loop)

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
    def test_call(self):
        yield self._client.wait_connected()
        yield self._client.service(self._exchange, self._queue_name, "fib.*", self.fib)
        values = [5, 10, 8, 9, 10, 23, 12]
        got_values = yield [self._client.call(self._exchange, "fib.call", str(value), "fib_call_back_queue")
                            for value in values]
        for expect, actual in zip(values, got_values):
            self.assertEqual(str(self._fib(expect)), actual)

    @gen_test(timeout=60)
    def test_call_benchmark(self):
        yield self._client.wait_connected()
        yield self._client.service(self._exchange, self._queue_name, "fib.*", self.fib)
        yield self._client.service(self._exchange, self._queue_name, "fib.*", self.fib)
        yield self._client.service(self._exchange, self._queue_name, "fib.*", self.fib)
        # size = 4000
        size = 5000
        #size = 6000
        values = [random.randint(5, 10) for _ in range(size)]
        got_values = yield [self._client.call(self._exchange, "fib.call", str(value), "fib_call_back_queue")
                            for value in values]
        for expect, actual in zip(values, got_values):
            self.assertEqual(str(self._fib(expect)), actual)

    @coroutine
    def fib_timeout(self, body):
        result = yield EXECUTOR.submit(self._fib_timeout)
        raise Return(str(result))

    @staticmethod
    def _fib_timeout():
        time.sleep(2)
        return "Task done"

    @gen_test(timeout=10)
    def test_call_timeout(self):
        yield self._client.wait_connected()
        yield self._client.service(self._exchange, self._queue_name, "fibtimtout.*", self.fib_timeout)
        value = yield self._client.call(self._exchange, "fibtimtout.call", "message", "fib_call_back_queue_timeout", timeout=1)
        self.assertIsNone(value)
        EXECUTOR.shutdown(True)


if __name__ == "__main__":
    unittest.main()
