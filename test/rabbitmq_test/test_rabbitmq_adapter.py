# -*- encoding:utf-8 -*-
import functools
import random
import unittest
import uuid

import pika
from pika import BasicProperties
from tornado.gen import coroutine, Return
from tornado.queues import Queue
from tornado.testing import AsyncTestCase, gen_test

from common.rabbitmq import TornadoAdapter, RabbitMQError, SyncRabbitMQProducer
from common.util import AsyncThreadPool

EXECUTOR = AsyncThreadPool(pool_size=10)


class TestTornadoAdapterPublish(AsyncTestCase):
    def setUp(self):
        super(TestTornadoAdapterPublish, self).setUp()
        self._url = 'amqp://dev:aispeech2018@10.12.6.35:5672/'
        self._adapter = TornadoAdapter(self._url, io_loop=self.io_loop)
        self._result_queue = Queue(maxsize=2)
        self.exchange = "adapter_exchange"
        self.routing_key = "adapter.*"
        self.queue = "adapter_queue"

    def tearDown(self):
        self._delete_queues(self.queue)
        self._adapter.close()
        super(TestTornadoAdapterPublish, self).tearDown()

    def _delete_queues(self, *queues):
        connection = pika.BlockingConnection(pika.URLParameters(self._url))
        channel = connection.channel()
        for queue in queues:
            channel.queue_delete(queue)
        connection.close()

    @coroutine
    def _process(self, body, back_value):
        self._result_queue.put(body)
        raise Return(back_value)

    @gen_test(timeout=5)
    def test_publish(self):
        expect_body = 'Hello World!'
        yield self._adapter.receive(self.exchange, self.routing_key, self.queue,
                                    functools.partial(self._process, back_value=True))
        yield self._adapter.publish(self.exchange, "adapter.one", expect_body)
        actual = yield self._result_queue.get()
        self.assertEqual(actual, expect_body)

    @gen_test(timeout=5)
    def test_publish_with_reply(self):
        corr_id = str(uuid.uuid4())
        reply_to = "queue_reply_to"
        yield self._adapter.receive(self.exchange, self.routing_key, self.queue,
                                    functools.partial(self._process, back_value="Nice to meet you too!"))
        yield self._adapter.receive(self.exchange, reply_to, reply_to,
                                    functools.partial(self._process, back_value=True))
        yield self._adapter.publish(self.exchange, "adapter.two", "Nice to meet you!",
                                             properties=BasicProperties(correlation_id=corr_id, reply_to=reply_to))
        actual = yield self._result_queue.get()
        self.assertEqual(actual, "Nice to meet you!")
        actual = yield self._result_queue.get()
        self.assertEqual(actual, "Nice to meet you too!")


class TestTornadoAdapterRpc(AsyncTestCase):
    def setUp(self):
        super(TestTornadoAdapterRpc, self).setUp()
        self._url = 'amqp://dev:aispeech2018@10.12.6.35:5672/'
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

    @gen_test(timeout=20)
    def test_rpc_call(self):
        yield self._adapter.receive(self._exchange, self._routing_key, self._queue, self.fib)
        value = yield self._adapter.rpc(self._exchange,"fib.call", "10")
        self.assertEqual(str(self._fib(10)), value)

    @gen_test(timeout=20)
    def test_rpc_calls(self):
        yield self._adapter.receive(self._exchange, self._routing_key, self._queue, self.fib)
        size = 20
        values =[random.randint(10, 20) for _ in range(size)]
        expect_values = [str(self._fib(value)) for value in values]
        actual_values = yield [
            self._adapter.rpc(self._exchange, "fib.%d" % value, str(value)) for value in values
        ]
        for expect, actual in zip(expect_values, actual_values):
            self.assertEqual(expect, actual)

    @gen_test(timeout=10)
    def test_rpc_timeout(self):
        success = yield self._adapter.connect()
        self.assertTrue(success)
        yield self._adapter.receive(self._exchange, self._routing_key, self._queue, self.fib)
        with self.assertRaises(RabbitMQError):
            yield self._adapter.rpc(self._exchange, "fib.50", "50", 3)

    def tearDown(self):
        self._delete_queues(self._queue)
        self._adapter.close()
        super(TestTornadoAdapterRpc, self).tearDown()

    def _delete_queues(self, *queues):
        connection = pika.BlockingConnection(pika.URLParameters(self._url))
        channel = connection.channel()
        for queue in queues:
            channel.queue_delete(queue)
        connection.close()


class TestSyncRabbitMQProducer(AsyncTestCase):
    def setUp(self):
        super(TestSyncRabbitMQProducer, self).setUp()
        self._url = 'amqp://dev:aispeech2018@10.12.6.35:5672/'
        self._adapter = TornadoAdapter(self._url, io_loop=self.io_loop)
        self._result_queue = Queue(maxsize=10)
        self.exchange = "sync_exchange"
        self.routing_key = "sync.*"
        self.queue = "sync_queue"
        
    def tearDown(self):
        self._delete_queues(self.queue)
        self._adapter.close()
        super(TestSyncRabbitMQProducer, self).tearDown()

    def _delete_queues(self, *queues):
        connection = pika.BlockingConnection(pika.URLParameters(self._url))
        channel = connection.channel()
        for queue in queues:
            channel.queue_delete(queue)
        connection.close()
        

    @coroutine
    def _process(self, body, back_value):
        self._result_queue.put(body)
        raise Return(back_value)

    @gen_test
    def test_publish_exception(self):
        p = SyncRabbitMQProducer(self._url)
        with self.assertRaises(RabbitMQError):
            p.publish(self.exchange, "sync.dog", "nice to meet you")
        with self.assertRaises(RabbitMQError):
            p.publish_messages(self.exchange, {'sync.cat':"A cat","sync.dog":"A dog"})
        p.connect()
        with self.assertRaises(RabbitMQError):
            p.publish_messages(self.exchange, ["sync.cat", "A cat"])

    @gen_test(timeout=10)
    def test_publish_single(self):
        success = yield self._adapter.connect()
        self.assertTrue(success)
        yield self._adapter.receive(self.exchange, self.routing_key, self.queue,
                                    functools.partial(self._process, back_value=True))
        with SyncRabbitMQProducer(self._url) as p:
            p.publish(self.exchange, "sync.dog", "A big dog")
            value = yield self._result_queue.get()
            self.assertEqual(value, "A big dog")
            p.publish(self.exchange, "sync.cat", "A big cat")

    @gen_test(timeout=10)
    def test_publish_multi(self):
        success = yield self._adapter.connect()
        self.assertTrue(success)
        yield self._adapter.receive(self.exchange, self.routing_key, self.queue,
                                    functools.partial(self._process, back_value=True))
        with SyncRabbitMQProducer(self._url) as p:
            p.publish(self.exchange, "sync.dog", "A big dog", "A small dog", "A tiny dog", "A tough dog")
            result_set = set(["A big dog", "A small dog", "A tiny dog", "A tough dog"])
            for i in range(4):
                value = yield self._result_queue.get()
                self.assertTrue(value in result_set)
        p = SyncRabbitMQProducer(self._url)
        p.connect()
        p.publish_messages(self.exchange, {'sync.cat': "A cat","sync.dog": "A dog"})
        result_set = set(["A cat", "A dog"])
        for i in range(2):
            value = yield self._result_queue.get()
            self.assertTrue(value in result_set)
        p.disconnect()

    @gen_test(timeout=10)
    def test_publish_reply_with(self):
        success = yield self._adapter.connect()
        self.assertTrue(success)
        corrid = str(uuid.uuid4())
        reply_to = "async_reply_to"
        yield self._adapter.receive(self.exchange, self.routing_key, self.queue,
                                    functools.partial(self._process, back_value="Nice to meet you too!"))
        yield self._adapter.receive(self.exchange, reply_to, reply_to,
                                    functools.partial(self._process, back_value=True))
        with SyncRabbitMQProducer(self._url) as p:
            p.publish(self.exchange, "sync.you", "Nice to meet you!", properties=BasicProperties(
                correlation_id=corrid, reply_to=reply_to))

        value = yield self._result_queue.get()
        self.assertEqual(value, "Nice to meet you!")
        value = yield self._result_queue.get()
        self.assertEqual(value, "Nice to meet you too!")


if __name__ == "__main__":
    unittest.main()
