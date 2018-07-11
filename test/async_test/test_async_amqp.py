# -*- coding:utf-8 -*-
from __future__ import unicode_literals
import unittest
from tornado.queues import Queue
import uuid
import logging
from tornado.testing import AsyncTestCase, gen_test
from async.async_amqp import AsyncAMQPProducer, AsyncAMQPConsumer


class TestAsyncProducerConsumer(AsyncTestCase):

    def _put_queue(self, body):
        self._queue.put(body)
        return True

    def init(self):
        self._queue = Queue(maxsize=1)
        url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        _exchange_name = "direct_logs"
        _publish_routing_key1 = "topic1"
        _publish_routing_key2 = "topic2"
        _receive_routing_keys = ["topic1", "topic3"]
        _queue_name = "queue1"

        p2 = AsyncAMQPProducer(url, "message_topic2", _exchange_name, _publish_routing_key2, io_loop=self.io_loop)
        p2.publish()
        c = AsyncAMQPConsumer(url, self._put_queue, _exchange_name, _receive_routing_keys, _queue_name, io_loop=self.io_loop)
        c.consume()
        p1 = AsyncAMQPProducer(url, "message_topic1", _exchange_name, _publish_routing_key1, io_loop=self.io_loop)
        p1.publish()

    @gen_test
    @unittest.skip("asynchronous tests have not be solved")
    def test_consume(self):
        self.init()
        value = yield self._queue.get()
        self.assertEqual(value, "topic1")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()