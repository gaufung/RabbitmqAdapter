# -*- coding:utf-8 -*-
from __future__ import unicode_literals
import logging
import tornado.ioloop
from tornado.gen import coroutine
import pika
import uuid
"""
The simple AMQP asynchronous Producer and Consumer 

Usage: 

TODO
"""

__author__ = ["feng.gao@aispeech.com"]


class AMQObject(object):
    """
    AMQP object, contains _get_log method
    """
    @classmethod
    def _get_log(cls, *name):
        return logging.getLogger('.'.join((cls.__module__, cls.__name__) + name))


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
        log = self._get_log("publish")
        log.info("initialize connection")
        self._connection = pika.TornadoConnection(pika.URLParameters(self._url),
                                                  on_open_callback=self._on_open_connection,
                                                  on_open_error_callback=self._on_open_error,
                                                  on_close_callback=self._on_close_connection,
                                                  custom_ioloop=self._io_loop)

    def _on_open_connection(self, conn):
        log = self._get_log("_on_open_connection")
        log.info("open connection")
        conn.channel(on_open_callback=self._on_open_channel)

    def _on_open_channel(self, channel):
        log = self._get_log("_on_open_channel")
        log.info("open channel")
        tornado.gen.Task(channel.exchange_declare, exchange=self._exchange_name,
                               exchange_type=self._exchange_type)
        channel.basic_publish(exchange=self._exchange_name, routing_key=self._routing_key, body=self._message)
        channel.close()
        self._connection.close()
        log.info("close channel")

    def _on_close_connection(self, connection, reason_code, reason_text):
        log = self._get_log("_on_close_connection")
        log.info("closing connection reason code :%s, reason text: %s" % (reason_code, reason_text, ))
        pass

    def _on_open_error(self, error):
        log = self._get_log("_on_open_error")
        if isinstance(error, str):
            log.error("error: %s" % (error,))
        else:
            log.error("exception: %s" % (error,))


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
        log = self._get_log("consume")
        log.info("initialize connection")
        self._connection = pika.TornadoConnection(pika.URLParameters(self._url),
                                                  on_open_callback=self._on_open_connection,
                                                  on_close_callback=self._on_close_connection,
                                                  on_open_error_callback=self._on_open_error,
                                                  custom_ioloop=self._io_loop)

    def _on_open_connection(self, conn):
        log = self._get_log("_on_open_connection")
        log.info("open connection")
        conn.channel(on_open_callback=self._on_open_channel)

    def _process(self, ch, method, properties, body):
        log = self._get_log("_process")
        log.info("consume body %s" % (body,))
        result = self._handler(body)
        if result:
            log.info("message process success")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            log.error("message process failed")
            pass

    def _on_open_channel(self, channel):
        self._channel = channel
        log = self._get_log("_on_open_channel")
        log.info("open channel")
        tornado.gen.Task(self._channel.exchange_declare, exchange=self._exchange_name,
                               exchange_type=self._exchange_type)
        tornado.gen.Task(self._channel.queue_declare, queue=self._queue_name)
        for binding_key in self._routing_keys:
            tornado.gen.Task(self._channel.queue_bind, exchange=self._exchange_name,
                                   routing_key=binding_key, queue=self._queue_name)
        self._channel.basic_consume(consumer_callback=self._process, queue=self._queue_name)

    def _on_close_connection(self, connection, reason_code, reason_tex):
        log = self._get_log("_on_close_connection")
        log.info("close connection. reason code %s, reason text %s" % (reason_code, reason_tex))

    def close(self):
        if self._channel is not None:
            self._channel.close()
        if self._connection is not None:
            self._connection.close()

    def _on_open_error(self, error):
        log = self._get_log("_on_open_error")
        if isinstance(error, str):
            log.error("error: %s" % (error,))
        else:
            log.error("exception: %s" % (error,))


if __name__ == "__main__":
    def callback(message):
        print(message)
        return True
    logging.basicConfig(level=logging.INFO)
    _url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
    _io_loop = tornado.ioloop.IOLoop.current()
    _exchange_name = "direct_logs"
    _publish_routing_keys = "info"
    _message = "A critical kernel"
    _receive_routing_keys = ["error", "info"]
    _queue_name = "name" + str(uuid.uuid4())
    p = AsyncAMQPProducer(_url, "info kernel", _exchange_name, "info", io_loop=_io_loop)
    p.publish()
    p = AsyncAMQPProducer(_url, "debug kernel", _exchange_name, "debug", io_loop=_io_loop)
    p.publish()
    c = AsyncAMQPConsumer(_url, callback, _exchange_name, _receive_routing_keys, "queue_name121", io_loop=_io_loop)
    c.consume()
    tornado.ioloop.IOLoop().current().start()

