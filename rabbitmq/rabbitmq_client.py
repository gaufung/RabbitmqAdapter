# -*- encoding:utf-8 -*-
import uuid
import logging
import warnings
import pika
from pika.exceptions import ChannelClosed
from tornado.ioloop import IOLoop
from tornado.gen import coroutine

__author__ = ["feng.gao@aispeech.com"]

""" 
rabbitmq clients, including rabbitmq synchronous publish and consumer.
"""


class AMQPError(Exception):
    def __init__(self, msg):
        self._message = msg

    def __str__(self):
        return "AMQPError: %s" % self._message


class AMQPObject(object):
    EXCHANGE_TYPE = 'topic'
    LOCALHOST = "127.0.0.1"
    LOGGER_HANDLER = None

    @classmethod
    def _get_log(cls, *name):
        logger = logging.getLogger('.'.join((cls.__module__, cls.__name__) + name))
        if AMQPObject.LOGGER_HANDLER is not None:
            logger.addHandler(AMQPObject.LOGGER_HANDLER)
        return logger

    def __init__(self, amqp_url, logger_handler=None):
        self._parameter = pika.ConnectionParameters(amqp_url) if amqp_url == self.LOCALHOST else \
            pika.URLParameters(amqp_url)
        AMQPObject.LOGGER_HANDLER = logger_handler


class SyncAMQPProducer(AMQPObject):
    """
    synchronize amqp producer
    usage:
        with SyncAMQPProducer("127.0.0.1", "exchange_name") as p:
            p.publish("dog.black", "message1", "message2")
    """
    def __init__(self, amqp_url, exchange_name, logger_handler=None):
        """
        synchronous AMQP producer
        :param amqp_url:
        :param exchange_name:
        :param logger_handler:
        """
        super(SyncAMQPProducer, self).__init__(amqp_url, logger_handler)
        self._connection = None
        self._channel = None
        self._exchange_name = exchange_name

    def _connect(self):
        log = self._get_log("_connect")
        log.info("initialize connection and channel")
        self._connection = pika.BlockingConnection(self._parameter)
        self._channel = self._connection.channel()
        try:
            self._channel.exchange_declare(exchange=self._exchange_name,
                                           exchange_type=self.EXCHANGE_TYPE, passive=True)
        except ChannelClosed as e:
            log.error("exchange %s doesn't exist. the messages may be lost. %s"
                            % (self._exchange_name, e))
            raise AMQPError("exchange %s doesn't exist. the messages may be lost. %s"
                            % (self._exchange_name, e))

    def _disconnect(self):
        log = self._get_log("_disconnect")
        log.info("tear down connection and channel")
        if self._channel is not None:
            self._channel.close()
        if self._connection is not None:
            self._connection.close()

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._disconnect()
        return not isinstance(exc_val, Exception)

    def publish(self, routing_key, *messages, **kwargs):
        """"
        synchronous rabbitmq publish
        :param routing_key: routing key for messages to published.
        :param messages: messages to be published variadic parameters.
        :param kwargs: parameter for publishing
        """
        log = self._get_log("publish")
        if self._channel is None:
            log.error("channel is not initialized")
            raise AMQPError("channel is not initialized")
        properties = kwargs.pop("properties") if kwargs.has_key("properties") else None
        for message in messages:
            self._channel.basic_publish(exchange=self._exchange_name,
                                        routing_key=routing_key,
                                        body=message,
                                        properties=properties)

    def publish_messages(self, messages, **kwargs):
        log = self._get_log("publish_messages")
        if self._channel is None:
            log.error("channel is not initialized")
            raise AMQPError("channel is not initialized")
        if not isinstance(messages, dict):
            log.error("messages is not dict")
            raise AMQPError("messages is not dict")
        properties = kwargs.pop("properties")
        for routing_key, message in messages.items():
            self._channel.basic_publish(exchange=self._exchange_name,
                                        routing_key=routing_key,
                                        body=message,
                                        properties=properties)

    def connect(self):
        """
        This is method doesn't recommend. using `with` context instead
        :return: None
        """
        warnings.warn("Call connect() method", category=DeprecationWarning, stacklevel=2)
        self._connect()

    def disconnect(self):
        """
        This is method doesn't recommend. using `with` context instead
        :return: None
        """
        warnings.warn("Call disconnect() method", category=DeprecationWarning, stacklevel=2)
        self._disconnect()


class AsyncAMQPConsumer(AMQPObject):
    """
    asynchronous amqp consumer
    """
    def __init__(self, amqp_url, exchange_name, routing_key, handler, queue_name=None, io_loop=None,
                 logger_handler=None):
        """
        synchronous amqp consumer
        :param amqp_url:  amqp url, it can be either 'localhost' or 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        :param exchange_name: exchange name
        :param routing_key: routing key
        :param handler: handler to process message
        :type handler:  signature. f(channel, method, header, body). return value is true or false
        :param queue_name:queue name for consuming message.
        :param io_loop: io loop. default value is IOLoop.current()
        :param logger_handler: handler for logging
        """
        super(AsyncAMQPConsumer, self).__init__(amqp_url, logger_handler)
        if queue_name is None:
            queue_name = "consume_queue_" + str(uuid.uuid4())
        if io_loop is None:
            io_loop = IOLoop.current()
        self._connection = None
        self._channel = None
        self._amq_url = amqp_url
        self._exchange_name = exchange_name
        self._handler = handler
        self._routing_key = routing_key
        self._queue_name = queue_name
        self._io_loop = io_loop

    def consume(self):
        self._connection=pika.TornadoConnection(self._parameter,
                                                on_open_callback=self._on_open_connection,
                                                on_open_error_callback=self._on_open_connection_error,
                                                on_close_callback=self._on_close_connection,
                                                custom_ioloop=self._io_loop)

    def _on_open_connection(self, conn):
        log = self._get_log("_on_open_connection")
        log.info("starting open channel")
        self._connection.channel(self._on_channel_open)

    def _on_channel_open(self, channel):
        log = self._get_log("_on_channel_open")
        self._channel = channel
        log.info("declaring exchange %s " % self._exchange_name)
        self._channel.exchange_declare(callback=self._on_exchange_declare,
                                       exchange=self._exchange_name,
                                       auto_delete=True,
                                       exchange_type=self.EXCHANGE_TYPE)

    def _on_exchange_declare(self, method_frame):
        log = self._get_log("_on_exchange_declare")
        log.info("declaring queue %s" % self._queue_name)
        self._channel.queue_declare(callback=self._on_queue_declared,
                                    queue=self._queue_name,
                                    durable=True,
                                    exclusive=False,
                                    auto_delete=True)

    def _on_queue_declared(self, method_frame):
        log = self._get_log("_on_queue_declared")
        log.info("binding queue %s" % self._queue_name)
        self._channel.queue_bind(callback=self._on_queue_bind,
                                 queue=self._queue_name,
                                 exchange=self._exchange_name,
                                 routing_key=self._routing_key)

    def _on_queue_bind(self, method_frame):
        log = self._get_log("_on_queue_bind")
        log.info("starting consume")
        self._channel.basic_consume(self._handler_delivery, queue=self._queue_name)

    def _handler_delivery(self, channel, method, header, body):
        log = self._get_log("_handler_delivery")
        log.info("consume body %s" % (body,))
        self._io_loop.spawn_callback(self._process_message,
                                     channel=channel,
                                     method=method,
                                     header=header,
                                     body=body)

    @coroutine
    def _process_message(self, channel, method, header, body):
        log = self._get_log("_process_message")
        log.info("start processing")
        result = yield self._handler(channel, method, header, body)
        if result:
            log.info("message process success")
        else:
            log.error("message process failed")

    def _on_close_connection(self, connection, reason_code, reason_tex):
        log = self._get_log("_on_close_connection")
        log.info("close connection. reason code %s, reason text %s" % (reason_code, reason_tex))

    def _on_open_connection_error(self, error):
        log = self._get_log("_on_open_connection_error")
        if isinstance(error, str):
            log.error("error: %s" % (error,))
        else:
            log.error("exception: %s" % (error,))
