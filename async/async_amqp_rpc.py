# -*- coding:utf-8 -*-
import logging
import uuid
import tornado.ioloop
from tornado.gen import coroutine, Return
from tornado.queues import Queue
import pika


class RpcObject(object):
    """
    RPC object, contains _get_log method
    """
    @classmethod
    def _get_log(cls, *names):
        return logging.getLogger(".".join((cls.__module____, cls.__name__) + names))


class AsyncAMQPServer(RpcObject):
    """
    AsyncAMQPServer for rpc
    """
    def __init__(self, url, handler, exchange_name="rpc", exchange_type="direct", queue_name="rpc_queue",
                 routing_keys=None, io_loop=None):
        if io_loop is None:
            io_loop = tornado.ioloop.IOLoop.current()
        self._connection = None
        self._url = url
        self._handler = handler
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._queue_name = queue_name
        self._routing_keys = routing_keys
        self._io_loop = io_loop

    def service(self):
        self._connection = pika.TornadoConnection(pika.URLParameters(self._url),
                                                  on_open_callback=self._on_open_connection,
                                                  custom_ioloop=self._io_loop)

    def _on_open_connection(self, conn):
        conn.channel(on_open_callback=self._on_open_channel)

    @coroutine
    def _on_open_channel(self, channel):
        yield tornado.gen.Task(channel.exchange_declare, exchange=self._exchange_name,
                               exchange_type=self._exchange_type)
        yield tornado.gen.Task(channel.queue_declare, queue=self._queue_name)
        yield tornado.gen.Task(channel.basic_qos, prefetch_count=1)
        if self._routing_keys is not None:
            for binding_key in self._routing_keys:
                yield tornado.gen.Task(channel.queue_bind, exchange=self._exchange_name,
                                       routing_key=binding_key, queue=self._queue_name)
        channel.basic_consume(consumer_callback=self._on_request, queue=self._queue_name)

    def _on_request(self, ch, method, props, body):
        response = self._handler(body)
        ch.basic_publish(exchange=self._exchange_name,
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id=props.correlation_id),
                         body=str(response))
        ch.basic_ack(delivery_tag=method.delivery_tag)


class AsyncAMQPClient(RpcObject):
    """
    AsyncAMQPClient for rpc
    """
    def __init__(self, url, exchange_name="rpc", exchange_type="direct", queue_name="rpc_queue", routing_key=None,
                 timeout=60, io_loop=None):
        if io_loop is None:
            io_loop = tornado.ioloop.IOLoop.current()
        self._connection = None
        self._channel = None
        self._exchange_name = ""
        self._callback_queue = "rpc-answer-%s" % str(uuid.uuid4())
        self._reply_queues = {}
        self._url = url
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._queue_name = queue_name
        self._routing_key = routing_key
        self._timeout = timeout
        self._io_loop = io_loop

    def connect(self):
        self._connection = pika.TornadoConnection(pika.URLParameters(self._url),
                                                  on_open_callback=self._on_open_connection,
                                                  custom_ioloop=self._io_loop)

    def _on_open_connection(self, conn):
        conn.channel(on_open_callback=self._on_open_channel)

    @coroutine
    def _on_open_channel(self, channel):
        self._channel = channel
        yield tornado.gen.Task(self._channel.exchange_declare, exchange=self._exchange_name,
                               exchange_type=self._exchange_type)
        yield tornado.gen.Task(self._channel.queue_declare, queue=self._callback_queue)
        yield tornado.gen.Task(self._channel.queue_declare, queue=self._queue_name)
        self._channel.basic_consume(self._on_message, queue=self._callback_queue)

    def _on_message(self, ch, basic_deliver, properties, body):
        corr_id = properties.correlation_id
        if corr_id in self._reply_queues:
            self._reply_queues[corr_id].put(body)
            del self._reply_queues[corr_id]

    @coroutine
    def call(self, body):
        corr_id = str(uuid.uuid4())
        queue = Queue(maxsize=1)
        self._reply_queues[corr_id] = queue
        self._channel.basic_publish(
            exchange=self._exchange_name,
            routing_key=self._routing_key,
            properties=pika.BasicProperties(
                correlation_id=corr_id,
                reply_to=self._callback_queue),
            body=body)
        result = yield queue.get()
        raise Return(result)


if __name__ == "__main__":
    def fib(n):
        n = int(n)
        if n < 2:
            return n
        else:
            return fib(n-1) + fib(n-2)
    _url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
    _io_loop = tornado.ioloop.IOLoop.current()
    server = AsyncAMQPServer(_url, fib, routing_keys=["info"], io_loop=_io_loop)
    server.service()
    _client = AsyncAMQPClient(_url, routing_key="info", io_loop=_io_loop)
    _client.connect()
    a = _client.call(10)
    print(a.result())
    tornado.ioloop.IOLoop().current().start()





