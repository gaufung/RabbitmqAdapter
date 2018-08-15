# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import time
import unittest
import functools
from rabbitmq.rabbitmq_adapter import TornadoAdapter, RabbitMQError
from pika import TornadoConnection
from tornado.testing import AsyncTestCase, gen_test
from tornado.gen import coroutine, Return
from tornado.concurrent import Future
from tornado.queues import Queue

_Error_Queue = Queue(maxsize=3)


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
                    _Error_Queue.put(RabbitMQError("close connection error"))

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
                _Error_Queue.put(RabbitMQError("close channel error"))

        def open_callback(channel):
            self.logger.info("created channel")
            channel.add_on_close_callback(on_channel_closed)
            future.set_result(channel)

        connection.channel(on_open_callback=open_callback)
        return future


class TestMockTornadoAdapter(AsyncTestCase):
    def setUp(self):
        super(TestMockTornadoAdapter, self).setUp()
        self._url = 'amqp://dev:aispeech2018@10.12.7.22:5672/%2f?heartbeat=5'
        self._adapter = MockTornadoAdapter(self._url, self.io_loop)
        self._exchange = "timeout_exchange"
        self._routing_key = "timeout.*"
        self._queue = "timeout_queue"

    @coroutine
    def _process(self, body, wait_time):
        time.sleep(wait_time)
        raise Return(body)

    @gen_test(timeout=60)
    def test_connection_close_error(self):
        success = yield self._adapter.connect()
        self.assertTrue(success)
        body = "Hello World"
        yield self._adapter.receive(self._exchange, self._routing_key, self._queue,
                                    functools.partial(self._process, wait_time=10.0))
        yield self._adapter.publish(self._exchange, "timeout.hello", body)
        for i in range(3):
            value = yield _Error_Queue.get()
            self.assertIsInstance(value, RabbitMQError)


if __name__ == "__main__":
    unittest.main()