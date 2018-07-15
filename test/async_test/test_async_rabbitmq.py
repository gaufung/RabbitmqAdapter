# -*- coding:utf-8 -*-
import unittest
from async.rabbitmq_client import SyncAMQPProducer, AsyncAMQPConsumer, AMQPError
from tornado.queues import Queue
from tornado.testing import AsyncTestCase, gen_test
from tornado.ioloop import IOLoop
import tornado
import uuid
import logging
import time


class TestOnlySyncAMQPProducer(unittest.TestCase):
    def setUp(self):
        self._url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        self._exchange_name = "exchange_test_" + str(uuid.uuid4())

    def test_publish(self):
        with self.assertRaises(AMQPError):
            p = SyncAMQPProducer(self._url, self._exchange_name)
            p.publish("invalid_routing_key", "nothing")


class TestSyncAMQPProducerAsyncAMQPConsumer(AsyncTestCase):
    def init(self):
        self._queue = Queue(maxsize=1)

        def _process(body):
            self._queue.put(body)
            return True

        self._url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        self._exchange_name = "exchange_test_" + str(uuid.uuid4())
        self._routing_key = "dog.*"
        c = AsyncAMQPConsumer(self._url, self._exchange_name, _process, self._routing_key, io_loop=self.io_loop)
        c.consume()

    @gen_test
    def test_produce1(self):
        self.init()
        time.sleep(5)
        p = SyncAMQPProducer(self._url, self._exchange_name)
        p.publish("dog.yellow", "A bit dog")
        value = yield self._queue.get()
        self.assertEqual(value, "A bit dog")

    def tearDown(self):
        self.io_loop.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()