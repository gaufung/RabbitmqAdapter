# -*- encoding:utf-8 -*-
import functools
import time
import unittest

import pika
from pika import TornadoConnection
from tornado.concurrent import Future
from tornado.gen import coroutine
from tornado.queues import Queue
from tornado.testing import AsyncTestCase, gen_test

from common.rabbitmq import TornadoAdapter, RabbitMQError

_queue = Queue(maxsize=3)


class MockTornadoAdapter(TornadoAdapter):
    def __init__(self, rabbitmq_url, io_loop=None):
        super(MockTornadoAdapter, self).__init__(rabbitmq_url, io_loop)

    def _create_connection(self, parameter):
            self.logger.info("creating connection")
            future = Future()

            def open_callback(unused_connection):
                self.logger.info("created connection")
                future.set_result(unused_connection)

            def open_error_callback(connection, exception):
                self.logger.error("open connection with error: %s" % exception)
                future.set_exception(exception)

            def close_callback(connection, reply_code, reply_text):
                if reply_code not in [self._NORMAL_CLOSE_CODE, ]:
                    self.logger.error("closing connection: reply code:%s, reply_text: %s. system will exist" % (
                    reply_code, reply_text,))
                    #raise RabbitMQError("close connection error")
                    _queue.put(RabbitMQError("close connection error"))

            TornadoConnection(parameter,
                              on_open_callback=open_callback,
                              on_open_error_callback=open_error_callback,
                              on_close_callback=close_callback,
                              custom_ioloop=self._io_loop)
            return future

    def _create_channel(self, connection):
        self.logger.info("creating channel")
        future = Future()

        def on_channel_closed(channel, reply_code, reply_txt):
            if reply_code not in [self._NORMAL_CLOSE_CODE, self._USER_CLOSE_CODE]:
                self.logger.error("channel closed. reply code: %s; reply text: %s. system will exist"
                                  % (reply_code, reply_txt,))
                _queue.put(RabbitMQError("close channel error"))

        def open_callback(channel):
            self.logger.info("created channel")
            channel.add_on_close_callback(on_channel_closed)
            future.set_result(channel)

        connection.channel(on_open_callback=open_callback)
        return future


class TestMockTornadoAdapter(AsyncTestCase):

    RABBITMQ_SERVER = 'amqp://dev:aispeech2018@10.12.6.35:5672/'

    def setUp(self):
        super(TestMockTornadoAdapter, self).setUp()
        self._url = 'amqp://dev:aispeech2018@10.12.6.35:5672/%2f?heartbeat=5'
        self._adapter = MockTornadoAdapter(self._url, self.io_loop)
        self._exchange = "timeout_exchange"
        self._routing_key = "timeout.*"
        self._queue = "test_timeout_queue"

    def tearDown(self):
        self._delete_queues(self._queue)
        super(TestMockTornadoAdapter, self).tearDown()

    def _delete_queues(self, *queues):
        connection = pika.BlockingConnection(pika.URLParameters(self.RABBITMQ_SERVER))
        channel = connection.channel()
        for queue in queues:
            channel.queue_delete(queue)
        connection.close()

    @coroutine
    def _process(self, body, wait_time):
        print(body)
        time.sleep(wait_time)
        yield _queue.put(body)

    @unittest.skip("need to be optimized")
    @gen_test(timeout=60)
    def test_connection_close_error(self):
        success = yield self._adapter.connect()
        self.assertTrue(success)
        body = "Hello World"
        yield self._adapter.receive(self._exchange, self._routing_key, self._queue,
                                    functools.partial(self._process, wait_time=10.0))
        yield self._adapter.publish(self._exchange, "timeout.hello", body)
        yield _queue.get(body)


if __name__ == "__main__":
    unittest.main()