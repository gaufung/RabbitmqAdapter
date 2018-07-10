# -*- coding:utf-8 -*-
import logging
import tornado.ioloop
from tornado.gen import coroutine
import pika
"""
The simple AMQP asynchronous Producer and Consumer 

Usage:

"""

__author__ = ["feng.gao@aispeech.com"]


class AMQObject(object):
    """
    AMQP object, contains _get_log method
    """
    @classmethod
    def _get_log(cls, *name):
        return logging.getLogger('.'.join((cls.__name__, cls.__name__) + name))


class AsyncAMQPProducer(AMQObject):
    """
    asynchronous AMQP producer
    """
    def __init__(self, url, message, exchange_name, routing_key, exchange_type="direct", io_loop=None):
        """
        Construct asynchronous AMQP producer
        :param url: AMQP url, 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        :param message: message wanted to send.
        :type message: str or unicode
        :param exchange_name: exchanger name
        :type: exchange_name: str
        :param routing_key: message routing key.
        :param exchange_type: exchanger type, default is 'direct' for multi-subscribes
        :param io_loop: io loop. default is tornado.ioloop.IOLoop.current()
        """
        if io_loop is None:
            io_loop = tornado.ioloop.IOLoop.current()
        self._connection = None
        self._url = url
        self._message = message
        self._exchange_name = exchange_name
        self._routing_key = routing_key
        self._exchange_type = exchange_type
        self._io_loop = io_loop

    def publish(self):
        """
        publish message
        :return: None
        """
        self._connection = pika.TornadoConnection(pika.URLParameters(self._url),
                                                  on_open_callback=self._on_open_connection,
                                                  custom_ioloop=self._io_loop)

    def _on_open_connection(self, conn):
        log = self._get_log("_on_open_connection")
        log.info("open connection")
        conn.channel(on_open_callback=self._on_open_channel)

    @coroutine
    def _on_open_channel(self, channel):
        yield tornado.gen.Task(channel.exchange_declare, exchange=self._exchange_name, exchange_type=self._exchange_type)
        channel.basic_publish(exchange=self._exchange_name, routing_key=self._routing_key, body=self._message)
        channel.close()


class AsyncAMQPConsumer(AMQObject):
    """
    asynchronous AMQP producer
    """
    def __init__(self, url, handler, exchange_name, routing_keys, queue_name,
                 exchange_type="direct", io_loop=None):
        """
        Construct asynchronous AMQP consumer
        :param url: AMQP url, 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        :param handler: handle receive message
        :type: handler: function (get str or unicode parameter and return boolean type. True: success; False: failed.)
        :param exchange_name: exchanger name
        :param routing_keys: routing keys
        :type routing_keys: list or tuple. such as ["error", "info"], it will handle both "error" and "info" categories messages
        :param queue_name: queue name
        :param exchange_type: exchanger type: default is 'direct'
        :param io_loop:  io loop. default is tornado.ioloop.IOLoop.current()
        """
        if io_loop is None:
            io_loop = tornado.ioloop.IOLoop.current()
        self._connection = None
        self._url = url
        self._handler = handler
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        self._queue_name = queue_name
        self._exchange_type = exchange_type
        self._io_loop = io_loop

    def consume(self):
        """
        consume message
        :return:
        """
        self._connection = pika.TornadoConnection(pika.URLParameters(self._url),
                                                  on_open_callback=self._on_open_connection,
                                                  custom_ioloop=self._io_loop)

    def _on_open_connection(self, conn):
        conn.channel(on_open_callback=self._on_open_channel)

    def _process(self, ch, method, properties, body):
        result = self._handler(body)
        if result:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    @coroutine
    def _on_open_channel(self, channel):
        yield tornado.gen.Task(channel.exchange_declare, exchange=self._exchange_name,
                               exchange_type=self._exchange_type)
        yield tornado.gen.Task(channel.queue_declare, queue=self._queue_name)
        for binding_key in self._routing_keys:
            yield tornado.gen.Task(channel.queue_bind, exchange=self._exchange_name,
                                   routing_key=binding_key, queue=self._queue_name)
        channel.basic_consume(consumer_callback=self._process, queue=self._queue_name)


if __name__ == "__main__":
    def callback(message):
        print message
        return True

    _url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
    _io_loop = tornado.ioloop.IOLoop.current()
    _exchange_name = "direct_logs"
    _publish_routing_keys = "info"
    _message = "A critical kernel"
    _receive_routing_keys = ["error", "info"]
    _queue_name = "name"
    p = AsyncAMQPProducer(_url, _message, _exchange_name, _publish_routing_keys, io_loop=_io_loop)
    p.publish()
    p = AsyncAMQPProducer(_url, "error kernel", _exchange_name, _publish_routing_keys, io_loop=_io_loop)
    p.publish()
    c = AsyncAMQPConsumer(_url, callback, _exchange_name, _receive_routing_keys, "queue_name", io_loop=_io_loop)
    c.consume()
    tornado.ioloop.IOLoop().current().start()

