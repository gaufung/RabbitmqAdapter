# # -*- encoding:utf-8 -*-
# from __future__ import unicode_literals
# import unittest
# import uuid
# import logging
# from tornado.queues import Queue
# from tornado import gen
# from rabbitmq.rabbitmq_rpc import AsyncRabbitMQ
# from pika.spec import CHANNEL_ERROR
# from tornado.testing import gen_test, AsyncTestCase
#
# dog_queue = Queue(maxsize=1)
# cat_queue = Queue(maxsize=1)
#
#
# @gen.coroutine
# def _dog_process(channel, method, header, body):
#     dog_queue.put(body)
#     raise gen.Return(True)
#
#
# @gen.coroutine
# def _cat_process(channel, method, header, body):
#     cat_queue.put(body)
#     raise gen.Return(True)
#
#
# @gen.coroutine
# def fib(n):
#     value = _fib(int(n))
#     raise gen.Return("fib(%s) = %d" % (n, value, ))
#
# def _fib(n):
#     if n< 2:
#         return n
#     else:
#         return _fib(n-1) + _fib(n-2)
#
#
# class TestAsyncRabbitMQ(AsyncTestCase):
#
#     def setUp(self):
#         super(TestAsyncRabbitMQ, self).setUp()
#         self.io_loop.spawn_callback(self.init)
#
#     @gen.coroutine
#     def init(self):
#         self._url = "127.0.0.1"
#         self._exchange_name = "exchange_name_" + str(uuid.uuid4())
#         self._rpc=AsyncRabbitMQ(self._url, io_loop=self.io_loop)
#         yield self._rpc.consume(self._exchange_name, "queue_routing_dog", "dog.*", _dog_process)
#         yield self._rpc.consume(self._exchange_name, "queue_routing_cat", "cat.*", _cat_process)
#         yield self._rpc.service(self._exchange_name, "rpc_queue", "pig.*", fib)
#
#     @gen_test
#     def test_publish(self):
#         self.io_loop.spawn_callback(self.init)
#         self._rpc.publish(self._exchange_name, "dog.yellow", "A big yellow dog")
#         value = yield dog_queue.get()
#         self.assertEqual(value, "A big yellow dog")
#         self._rpc.publish(self._exchange_name, "cat.black", "A black cat")
#         value = yield cat_queue.get()
#         self.assertEqual(value, value, "A black cat")
#         self._rpc.publish(self._exchange_name, "cat.yellow", "A yellow cat")
#         value = yield cat_queue.get()
#         self.assertEqual(value, "A yellow cat")
#         with self.assertRaises(CHANNEL_ERROR):
#             self._rpc.publish("non-existing-exchange", "cat.nothing", "nothing")
#
#     @gen_test
#     def test_call(self):
#         self.init()
#         actual_val = yield self._rpc.call(self._exchange_name, "rpc_queue", "pig.fib", "10")
#         expect_val = yield fib("10")
#         self.assertNotEqual(actual_val, expect_val)
#
#
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     unittest.main()
