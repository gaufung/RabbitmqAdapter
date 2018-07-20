# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import unittest
import logging
import uuid
from tornado.gen import coroutine,Return
from tornado.testing import AsyncTestCase, gen_test
from tornado.queues import Queue
from rabbitmq.rabbitmq_client import AsyncAMQPConsumer, SyncAMQPProducer
from rabbitmq.rabbitmq_util import make_properties


class TestRabbitMQClient(AsyncTestCase):
    @coroutine
    def _process(self, channel, method, header, body):
        self.queue.put(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        raise Return(True)

    def setUp(self):
        super(TestRabbitMQClient, self).setUp()
        self._url = "amqp://dev:aispeech2018@10.12.7.22:5672/"
        self.queue = Queue(maxsize=10)
        self.r = AsyncAMQPConsumer(self._url, exchange_name="another_exchange", routing_key="dog.*", handler=self._process, io_loop=self.io_loop)
        self.p = SyncAMQPProducer(self._url, exchange_name="another_exchange")
        self.p.connect()

    @gen_test
    def test_single_publish(self):
        yield self.r.is_connected()
        yield self.r.consume()
        self.p.publish("dog.yellow", "A yellow dog")
        value = yield self.queue.get()
        self.assertEqual(value, 'A yellow dog')
        self.p.publish("dog.red", "A red dog")
        value = yield self.queue.get()
        self.assertEqual(value, "A red dog")

    @gen_test
    def test_multi_publish(self):
        yield self.r.is_connected()
        yield self.r.consume()
        self.p.publish("dog.big", "a big dog", "another big dog", "more big dog")
        result_set = set(["a big dog", "another big dog", "more big dog"])
        for i in range(3):
            value = yield self.queue.get()
            self.assertTrue(value in result_set)

    @gen_test
    def test_multi_routing_key_publish(self):
        yield self.r.is_connected()
        yield self.r.consume()
        messages = {
            "dog.yellow": "a yellow dog",
            "dog.red": "a red dog",
            "dog.green": "a green dog"}
        result_set = set(messages.values())
        self.p.publish_messages(messages)
        for i in range(3):
            value = yield self.queue.get()
            self.assertTrue(value in result_set)


class TestRabbitMQClientWithReply(AsyncTestCase):
    @coroutine
    def _process(self, channel, method, props, body):
        n = int(body)
        result = self._fib(n)
        if props is not None:
            channel.basic_publish(exchange="fib_exchange",
                                  routing_key=props.reply_to,
                                  properties=make_properties(correlation_id=props.correlation_id),
                                  body=str(result))
        channel.basic_ack(delivery_tag=method.delivery_tag)
        raise Return(True)

    def _fib(self, n):
        if n < 2:
            return n
        else:
            return self._fib(n-1) + self._fib(n-2)

    @coroutine
    def _fib_back(self, channel, method, props, body):
        self.queue.put(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        raise Return(True)

    def setUp(self):
        super(TestRabbitMQClientWithReply, self).setUp()
        self._url = "amqp://dev:aispeech2018@10.12.7.22:5672/"
        self._time_to_wait = 1
        self.queue = Queue(maxsize=1)
        self.fib_back_queue = "fibnacci_call_back"
        self.corr_id = str(uuid.uuid4())
        self.r = AsyncAMQPConsumer(self._url, exchange_name="fib_exchange", routing_key="fib.*",
                                   handler=self._process, io_loop=self.io_loop)
        self.callback_r = AsyncAMQPConsumer(self._url, exchange_name="fib_exchange", routing_key=self.fib_back_queue,
                                   queue_name=self.fib_back_queue, handler=self._fib_back, io_loop=self.io_loop)
        self.p = SyncAMQPProducer(self._url, exchange_name="fib_exchange")
        self.p.connect()

    @gen_test(timeout=10)
    def test_fib(self):
        # wait consumers build connection
        yield self.r.is_connected()
        yield self.callback_r.is_connected()
        yield self.r.consume()
        yield self.callback_r.consume()
        self.p.publish("fib.now", "2", properties=make_properties(correlation_id=self.corr_id, reply_to=self.fib_back_queue))
        value = yield self.queue.get()
        expect = str(self._fib(2))
        self.assertEqual(value, expect)
        self.p.publish("fib.now", "10",
                       properties=make_properties(correlation_id=self.corr_id, reply_to=self.fib_back_queue))
        value = yield self.queue.get()
        expect = str(self._fib(10))
        self.assertEqual(value, expect)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()