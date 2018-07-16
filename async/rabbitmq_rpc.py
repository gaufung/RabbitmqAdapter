# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import logging
import re
import pika
from tornado.ioloop import IOLoop
from tornado.queues import Queue
from tornado.gen import Return, coroutine

__author__ = ["feng.gao@aispeech.com"]


class AMQPRpcObject(object):
    EXCHANGE_TYPE = "topic"
    LOCALHOST = "127.0.0.1"

    @classmethod
    def _get_log(cls, *names):
        return logging.getLogger(".".join((cls.__module__, cls.__name__) + names))

    def __init__(self, amqp_url):
        """
        initialize an AMQPRpcObject instance
        :param amqp_url: amqp url, it can be either 'amqp://dev:aispeech2018@10.12.7.22:5672/' or "127.0.0.1"
        """
        self._parameter = pika.ConnectionParameters(amqp_url) if amqp_url == self.LOCALHOST else \
            pika.URLParameters(amqp_url)


class AsyncRabbitMQ(AMQPRpcObject):
    """
    It is an `Everything-in-One` RabbitMQ client, including features as follows:
        - producer
        - consumer
        - rpc client
        - rpc server
    All of above clients share the only one connection.
    """
    def __init__(self, amqp_url, io_loop=None):
        super(AsyncRabbitMQ, self).__init__(amqp_url)
        if io_loop is None:
            io_loop = IOLoop.current()
        self._io_loop = io_loop
        self._connection = None
        self._channel = None
        self._channel_queue = Queue(maxsize=1)
        self._exchange_declare_dict = dict()
        self._queue_handlers_dict = dict()
        self._queue_declare_dict = dict()
        self._queue_bind_dict = dict()

    def _connect(self):
        pika.TornadoConnection(parameters=self._parameter,
                               on_open_callback=self._on_open_connection,
                               on_open_error_callback=self._on_open_connection_error,
                               on_close_callback=self._on_close_connection,
                               custom_ioloop=self._io_loop)

    def _on_open_connection(self, conn):
        log = self._get_log("_on_open_connection")
        self._connection = conn
        log.info("initializing connection")
        self._connection.channel(self._on_open_channel)

    def _on_open_channel(self, channel):
        log = self._get_log("_on_open_channel")
        self._channel = channel
        log.info("initializing channel")
        self._channel_queue.put(True)

    def _on_close_connection(self, connection, reason_code, reason_tex):
        log = self._get_log("_on_close_connection")
        log.info("close connection. reason code %s, reason text %s" % (reason_code, reason_tex))

    def _on_open_connection_error(self, error):
        log = self._get_log("_on_open_connection_error")
        if isinstance(error, str):
            log.error("error: %s" % (error,))
        else:
            log.error("exception: %s" % (error,))

    def _on_exchange_declare(self, exchange_name, passive=True):
        log = self._get_log("_on_exchange_declare")
        try:
            self._channel.exchange_declare(callback=self._on_exchange_declare_ok,
                                           exchange=exchange_name,
                                           exchange_type=self.EXCHANGE_TYPE, passive=passive,
                                           auto_delete=True)
            self._exchange_declare_dict[exchange_name].put(True)
        except Exception as e:
            log.error("exchange has not been declared")
            raise e

    def _on_exchange_declare_ok(self, unframe):
        pass

    @coroutine
    def publish(self, exchange_name, routing_key, message):
        log = self._get_log("publish")
        if self._channel is None:
            log.info("connect")
            self._connect()
            yield self._channel_queue.get()
        if exchange_name not in self._exchange_declare_dict:
            log.info("declare exchange")
            self._exchange_declare_dict[exchange_name] = Queue(maxsize=1)
            self._on_exchange_declare(exchange_name, True)
            yield self._exchange_declare_dict[exchange_name].get()
        self._channel.basic_publish(exchange=exchange_name,
                                          routing_key=routing_key,
                                          body=message)

    @coroutine
    def consume(self, exchange_name, queue_name, routing_key, handler):
        log = self._get_log("consume")
        if self._channel is None:
            log.info("connect")
            self._connect()
            yield self._channel_queue.get()
        if exchange_name not in self._exchange_declare_dict:
            log.info("declare exchange")
            self._exchange_declare_dict[exchange_name] = Queue(maxsize=1)
            self._on_exchange_declare(exchange_name, False)
            yield self._exchange_declare_dict[exchange_name].get()
        if queue_name not in self._queue_declare_dict:
            log.info("declare queue")
            self._queue_declare_dict[queue_name] = Queue(maxsize=1)
            self._on_queue_declare(queue_name)
            yield self._queue_declare_dict[queue_name].get()
        if queue_name not in self._queue_bind_dict:
            log.info("bind queue")
            self._queue_bind_dict[queue_name] = Queue(maxsize=1)
            self._on_queue_bind(exchange_name, queue_name, routing_key)
            yield self._queue_bind_dict[queue_name].get()
        self._queue_handlers_dict[self._routing_key_pattern(routing_key)] = handler
        self._channel.basic_consume(self._handler_delivery, queue=queue_name)

    def _handler_delivery(self, channel, method, header, body):
        log = self._get_log("_handler_delivery")
        log.info("consume body %s" % (body,))
        self._io_loop.spawn_callback(self._process_message, body=body, channel=channel, method=method,
                                     delivery_tag=method.delivery_tag, header=header)

    @coroutine
    def _process_message(self, body, channel, method, delivery_tag,header):
        log = self._get_log("_process_message")
        log.info("start processing")
        handler = self._lookup_handler(method.routing_key)
        if handler is None:
            log.info("handler not found")
        result = yield handler(body)
        if result:
            log.info("message process success")
            channel.basic_ack(delivery_tag=delivery_tag)
        else:
            log.error("message process failed")
            pass

    def _on_queue_declare(self, queue_name):
        self._channel.queue_declare(callback=self._on_queue_declare_ok,
                                    queue=queue_name,
                                    durable=True,
                                    exclusive=False,
                                    auto_delete=True)
        self._queue_declare_dict[queue_name].put(True)

    def _on_queue_declare_ok(self, method_frame):
        pass

    def _on_queue_bind(self, exchange_name, queue_name, routing_key):
        self._channel.queue_bind(callback=self._on_queue_bind_ok,
                                 queue=queue_name,
                                 exchange=exchange_name,
                                 routing_key=routing_key)
        self._queue_bind_dict[queue_name].put(True)

    def _on_queue_bind_ok(self, method_frame):
        pass

    @staticmethod
    def _routing_key_pattern(routing_key):
        pattern = routing_key.replace("*", "\w+?").replace(".", "\.")
        return re.compile(pattern)

    def _lookup_handler(self, routing_key):
        for pattern, handler in self._queue_handlers_dict.items():
            if pattern.match(routing_key):
                return handler
        return None

    def call(self, exchange_name, routing_key, argument, timeout=None):
        pass

    def service(self, exchange_name, rouging_key, queue_name, handler):
        pass
