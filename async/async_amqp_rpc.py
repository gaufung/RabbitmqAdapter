# -*- coding:utf-8 -*-
import logging
import uuid
import time
import functools
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
        return logging.getLogger(".".join((cls.__module__, cls.__name__) + names))


class AsyncAMQPServer(RpcObject):
    """
    AsyncAMQPServer for rpc
    """
    def __init__(self, url, handler, exchange_name="rpc", exchange_type="direct", queue_name="rpc_queue",
                 routing_keys=None, io_loop=None):
        """
        Asynchronous amqp rpc sever
        :param url: amqp url: 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        :param handler: service handler
        :param exchange_name: exchanger name, default "rpc"
        :param exchange_type: exchanger type: default "direct"
        :param queue_name: queue name, default "rpc_queue"
        :param routing_keys: routing keys for various topics
        :type routing_keys: list or tuple: ["topic1", "topic2", "topic3"]
        :param io_loop: io loop: default `tornado.ioloop.IOLoop.current()`
        """
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
        """
        rpc server starts service
        :return: None
        """
        log = self._get_log("service")
        log.info("initialize connection")
        self._connection = pika.TornadoConnection(pika.URLParameters(self._url),
                                                  on_open_callback=self._on_open_connection,
                                                  custom_ioloop=self._io_loop)

    def _on_open_connection(self, conn):
        log = self._get_log("_on_open_connection")
        log.info("open connection")
        conn.channel(on_open_callback=self._on_open_channel)

    @coroutine
    def _on_open_channel(self, channel):
        log = self._get_log("_on_open_channel")
        log.info("open channel")
        yield tornado.gen.Task(channel.exchange_declare, exchange=self._exchange_name,
                               exchange_type=self._exchange_type)
        yield tornado.gen.Task(channel.queue_declare, queue=self._queue_name)
        if self._routing_keys is not None:
            for binding_key in self._routing_keys:
                yield tornado.gen.Task(channel.queue_bind, exchange=self._exchange_name,
                                       routing_key=binding_key, queue=self._queue_name)
        channel.basic_consume(consumer_callback=self._on_request, queue=self._queue_name)

    def _on_request(self, ch, method, props, body):
        log = self._get_log("_on_request")
        log.info("received body: %s" % (body, ))
        response = self._handler(body)
        if response is not None:
            log.info("routing_key: %s" % (props.reply_to, ))
            log.info("correlation_id: %s", (props.correlation_id, ))
            log.info("to send body: %s" % (response,))
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
        """
        asynchronous APQP client for rpc
        :param url: url for amqp 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        :param exchange_name: exchanger name. default: "rpc"
        :param exchange_type: exchange type, default "direct"
        :param queue_name: queue name, default "rpc_queue"
        :param routing_key: routing key: such as "topic1"
        :type routing_key: str or unicode
        :param timeout: timeout [second] (if timeout, return None)
        :type timeout: int
        :param io_loop: io loop, default: `tornado.ioloop.IOLoop.current()`
        """
        if io_loop is None:
            io_loop = tornado.ioloop.IOLoop.current()
        self._connection = None
        self._channel = None
        self._exchange_name = exchange_name
        self._callback_queue = "rpc-answer-%s" % str(uuid.uuid4())
        self._reply_queues = {}
        self._body = None
        self._corr_id = None
        self._url = url
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self._queue_name = queue_name
        self._routing_key = routing_key
        self._timeout = timeout
        self._io_loop = io_loop

    def connect(self):
        # print("[client:]connection")
        log = self._get_log("connect")
        log.info("initialize connection")
        self._connection = pika.adapters.TornadoConnection(
                                                  pika.URLParameters(self._url),
                                                  on_open_callback=self._on_open_connection,
                                                  custom_ioloop=self._io_loop)

    def _on_open_connection(self, conn):
        log = self._get_log("_on_open_connection")
        log.info("open connection")
        conn.channel(on_open_callback=self._on_open_channel)

    @coroutine
    def _on_open_channel(self, channel):
        log = self._get_log("_on_open_channel")
        log.info("open channel")
        self._channel = channel
        yield tornado.gen.Task(self._channel.exchange_declare, exchange=self._exchange_name,
                               exchange_type=self._exchange_type)
        yield tornado.gen.Task(self._channel.queue_declare, queue=self._callback_queue)
        yield tornado.gen.Task(self._channel.queue_declare, queue=self._queue_name)
        yield tornado.gen.Task(self._channel.queue_bind, exchange=self._exchange_name,
                               routing_key=self._callback_queue, queue=self._callback_queue)
        self._channel.basic_consume(self._on_message, queue=self._callback_queue)
        log.info("routing_key: %s" % (self._routing_key,))
        log.info("correlation_id: %s" % (self._corr_id,))
        log.info("reply_to: %s" % (self._callback_queue,))
        log.info("send body: %s" % (self._body, ))
        self._channel.basic_publish(exchange=self._exchange_name, routing_key=self._routing_key,
                                    properties=pika.BasicProperties(correlation_id=self._corr_id,
                                                                    reply_to=self._callback_queue),
                                    body=self._body)

    def _on_message(self, ch, method, props, body):
        log = self._get_log("_on_message")
        log.info("receive body: %s" % (body, ))
        corr_id = props.correlation_id
        if corr_id in self._reply_queues:
            log.info("got response")
            self._reply_queues[corr_id].put(body)
            del self._reply_queues[corr_id]
        else:
            log.info("didn't got response")
            pass

    @coroutine
    def call(self, body):
        """
        client call
        :param body: call argument
        :return: response value
        """
        log = self._get_log("call")
        corr_id = str(uuid.uuid4())
        log.info("generating corr_id %s" % (corr_id,))
        log.info("to send body %s " % (body,))
        self._corr_id = corr_id
        queue = Queue(maxsize=1)
        self._body = body
        self._reply_queues[corr_id] = queue
        self.connect()
        self._io_loop.add_timeout(time.time() + self._timeout,
                                  functools.partial(self._on_timeout, correlation_id=corr_id))
        result = yield queue.get()
        raise Return(result)

    @coroutine
    def _on_timeout(self, correlation_id):
        if correlation_id in self._reply_queues:
            self._reply_queues[correlation_id].put(None)


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
    a = _client.call(str(10))
    tornado.ioloop.IOLoop().current().start()





