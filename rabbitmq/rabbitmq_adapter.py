# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import functools
import logging
import uuid
import warnings
import sys
from pika import URLParameters, ConnectionParameters, BlockingConnection
from pika import TornadoConnection
from pika import BasicProperties
from tornado.ioloop import IOLoop
from tornado.concurrent import Future
from tornado import gen
from tornado.queues import Queue


class RabbitMQError(Exception):
    def __init__(self, msg):
        self._message = msg

    def __str__(self):
        return "RabbitMQError: %s" % self._message


class SyncRabbitMQProducer(object):
    """
    synchronize amqp producer
    usage:
        with SyncAMQPProducer("127.0.0.1") as p:
            p.publish("exchange_name", "dog.black", "message1", "message2")
    """
    def __init__(self, rabbitmq_url):
        """

        :param rabbitmq_url:
        """
        self._parameter = ConnectionParameters("127.0.0.1") if rabbitmq_url in ["localhost", "127.0.0.1"] else \
            URLParameters(rabbitmq_url)
        self._logger = logging.getLogger(__name__)
        self._connection = None
        self._channel = None

    @property
    def logger(self):
        """
        logger
        :return:
        """
        return self._logger

    def _connect(self):
        self.logger.info("initialize connection and channel")
        self._connection = BlockingConnection(self._parameter)
        self._channel = self._connection.channel()

    def _disconnect(self):
        self.logger.info("tear down channel and connection")
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

    def publish(self, exchange, routing_key, *messages, **kwargs):
        self.logger.info("[publishing] exchange name: %s; routing key: %s" % (exchange, routing_key,))
        if self._channel is None:
            raise RabbitMQError("channel has not been initialized")
        properties = kwargs.pop("properties") if kwargs.has_key("properties") else None
        for message in messages:
            self.logger.info("publish message: %s" % message)
            self._channel.basic_publish(exchange=exchange,
                                        routing_key=routing_key,
                                        body=str(message),
                                        properties=properties)

    def publish_messages(self, exchange, messages, **kwargs):
        self.logger.info("[publish_message] exchange name: %s" % exchange)
        if self._channel is None:
            raise RabbitMQError("channel has not been initialized")
        if not isinstance(messages, dict):
            raise RabbitMQError("messages is not dict")
        properties = kwargs.pop("properties") if kwargs.has_key("properties") else None
        for routing_key, message in messages.items():
            self.logger.info("routing key:%s, message: %s" % (routing_key, message, ))
            self._channel.basic_publish(exchange=exchange,
                                        routing_key=routing_key,
                                        body=str(message),
                                        properties=properties)

    def connect(self):
        """
        This is method doesn't recommend. using `with` context instead
        :return: None
        """
        warnings.warn("Call connect() method, using `with` context instead.", category=DeprecationWarning, stacklevel=2)
        self._connect()

    def disconnect(self):
        """
        This is method doesn't recommend. using `with` context instead
        :return: None
        """
        warnings.warn("Call disconnect() method, using `with` context instead.", category=DeprecationWarning, stacklevel=2)
        self._disconnect()


class TornadoAdapter(object):
    _EXIST_CODE = 1  # system exist code
    _NORMAL_CLOSE_CODE = 200  # connection or channel closes normally
    """
    If channel closing is initiated by user (either directly of indirectly by closing a 
    connection containing the channel) and closing concludes gracefully without Channel.
    Close from the broker and without loss of connection, the callback will receive 0 as 
    reply_code and empty string as reply_text.
    """
    _USER_CLOSE_CODE = 0

    def __init__(self, rabbitmq_url, io_loop=None):
        """
        A Rabbitmq client, using tornado to complete asynchronous invoking.
        It is an `everything-in-one` RabbitMQ client, including following interfaces:
            - establish connection
            - publish
            - receive
            - rpc
        details:
            1. establish connection
            Every instance of `TornadoAdapter` has two rabbitmq connections. one for publish and the other for consumer.
            2. publish
            Publishing connection creates a channel when invoke publish method. After publishing message, it closes channel.
            3. receive
            It receives message from queue and process message. and publish message if necessary.
            4. rpc
            It invoke rpc call and wait result asynchronously. Of course, it supports timeout.
        :param rabbitmq_url: url for rabbitmq. it can be either '127.0.0.1' ("localhost") or 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        :param io_loop: io loop. if it is none, using IOLoop.current() instead.
        """
        self._parameter = ConnectionParameters("127.0.0.1") if rabbitmq_url in ["localhost", "127.0.0.1"] else \
            URLParameters(rabbitmq_url)
        self._logger = logging.getLogger(__name__)
        self._publish_connection = None
        self._receive_connection = None
        self._rpc_exchange_dict = dict()
        self._rpc_corr_id_dict = dict()
        if io_loop is None:
            io_loop = IOLoop.current()
        self._io_loop = io_loop

    @gen.coroutine
    def connect(self):
        """
        establishing two connections for publishing and receiving respectively.
        :return: True if establish successfully.
        """
        self._publish_connection = yield self._create_connection(self._parameter)
        self._receive_connection = yield self._create_connection(self._parameter)
        raise gen.Return(True)

    @property
    def logger(self):
        """
        logger
        :return:
        """
        return self._logger

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
            if reply_code not in [self._NORMAL_CLOSE_CODE,]:
                self.logger.error("closing connection: reply code:%s, reply_text: %s. system will exist" % (reply_code, reply_text,))
                sys.exit(self._EXIST_CODE)

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
                sys.exit(self._EXIST_CODE)

        def open_callback(channel):
            self.logger.info("created channel")
            channel.add_on_close_callback(on_channel_closed)
            future.set_result(channel)

        connection.channel(on_open_callback=open_callback)
        return future

    def _exchange_declare(self, channel, exchange=None, exchange_type='topic', **kwargs):
        self.logger.info("declaring exchange: %s " % exchange)
        future = Future()

        def callback(unframe):
            self.logger.info("declared exchange: %s" % exchange)
            future.set_result(unframe)

        channel.exchange_declare(callback=callback,
                                 exchange=exchange, exchange_type=exchange_type, **kwargs)
        return future

    def _queue_declare(self, channel, queue='', **kwargs):
        self.logger.info("declaring queue: %s" % queue)
        future = Future()

        def callback(method_frame):
            self.logger.info("declared queue: %s" % method_frame.method.queue)
            future.set_result(method_frame.method.queue)

        channel.queue_declare(callback=callback, queue=queue, **kwargs)
        return future

    def _queue_bind(self, channel, queue, exchange, routing_key=None, **kwargs):
        self.logger.info("binding queue: %s to exchange: %s" % (queue, exchange,))
        future = Future()

        def callback(unframe):
            self.logger.info("bound queue: %s to exchange: %s" % (queue, exchange,))
            future.set_result(unframe)
        channel.queue_bind(callback, queue=queue, exchange=exchange, routing_key=routing_key, **kwargs)
        return future

    @gen.coroutine
    def publish(self, exchange, routing_key, body, properties=None):
        """
        publish message. creating a brand new channel once invoke this method. After publishing, it closes the
        channel.
        :param exchange: exchange name
        :type exchange; str or unicode
        :param routing_key: routing key (e.g. dog.yellow, cat.big)
        :param body: message
        :param properties: properties
        :return: None
        """
        # self.logger.info("[publishing] exchange: %s; routing key: %s; body: %s." % (exchange, routing_key, body,))
        channel = yield self._create_channel(self._publish_connection)
        channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=properties)
        channel.close()

    @gen.coroutine
    def receive(self, exchange, routing_key, queue_name, handler, no_ack=False, prefetch_count=0):
        """
        receive message. creating a brand new channel to consume message. Before consuming, it have to declaring
        exchange and queue. And bind queue to particular exchange with routing key. if received properties is not
        none, it publishes result back to `reply_to` queue.
        :param exchange: exchange name
        :param routing_key: routing key (e.g. dog.*, *.big)
        :param queue_name: queue name
        :param handler: message handler,
        :type handler def fn(body)
        :param no_ack: ack
        :param prefetch_count: prefetch count
        :return: None
        """
        self.logger.info("[receive] exchange: %s; routing key: %s; queue name: %s" % (exchange, routing_key, queue_name,))
        channel = yield self._create_channel(self._publish_connection)
        yield self._queue_declare(channel, queue=queue_name, auto_delete=False)
        if routing_key != "":
            yield self._exchange_declare(channel, exchange=exchange)
            yield self._queue_bind(channel, exchange=exchange, queue=queue_name, routing_key=routing_key)
        self.logger.info("[start consuming] exchange: %s; routing key: %s; queue name: %s" % (exchange,
                                                                                              routing_key, queue_name,))
        channel.basic_qos(prefetch_count=prefetch_count)
        channel.basic_consume(functools.partial(self._on_message, exchange=exchange, handler=handler)
                              , queue=queue_name, no_ack=no_ack)

    def _on_message(self, unused_channel, basic_deliver, properties, body, exchange, handler=None):
        self.logger.info("consuming message: %s" % body)
        self._io_loop.spawn_callback(self._process_message, unused_channel, basic_deliver, properties, body,
                                     exchange, handler)

    @gen.coroutine
    def _process_message(self, unused_channel, basic_deliver, properties, body, exchange, handler=None):
        try:
            result = yield handler(body)
            self.logger.info("%s has been processed successfully and result is  %s" % (body, result,))
            if properties is not None \
                    and properties.reply_to is not None \
                    and properties.correlation_id is not None:
                self.logger.info("sending result back to %s" % properties.reply_to)
                self.publish(exchange=exchange,
                             routing_key=properties.reply_to,
                             properties=BasicProperties(correlation_id=properties.correlation_id),
                             body=str(result))
            unused_channel.basic_ack(basic_deliver.delivery_tag)
        except Exception:
            unused_channel.basic_ack(basic_deliver.delivery_tag)
            import traceback
            self.logger.error(traceback.format_exc())

    @gen.coroutine
    def rpc(self, exchange, routing_key, body, timeout=None):
        """
        rpc call. It create a queue randomly when encounters first call with the same exchange name. Then, it starts
        consuming the created queue(waiting result). It publishes message to rabbitmq with properties that has correlation_id
        and reply_to. if timeout is set, it starts a coroutine to wait timeout and raises an `Exception("timeout")`.
        If server has been sent result, it return it asynchronously.
        :param exchange: exchange name
        :param routing_key: routing key(e.g. dog.Yellow, cat.big)
        :param body: message
        :param timeout: timeout
        :return: result or Exception("timeout")
        """
        # self.logger.info("rpc call. exchange: %s; routing_key: %s; body: %s" % (exchange, routing_key, body,))
        if exchange not in self._rpc_exchange_dict:
            self._rpc_exchange_dict[exchange] = Queue(maxsize=1)
            callback_queue = yield self._initialize_rpc_callback(exchange)
            yield self._rpc_exchange_dict[exchange].put(callback_queue)
        callback_queue = yield self._rpc_exchange_dict[exchange].get()
        yield self._rpc_exchange_dict[exchange].put(callback_queue)
        self.logger.info("starting calling. %s" % body)
        result = yield self._call(exchange, callback_queue, routing_key, body, timeout)
        raise gen.Return(result)

    @gen.coroutine
    def _initialize_rpc_callback(self, exchange):
        self.logger.info("initialize rpc callback queue")
        rpc_channel = yield self._create_channel(self._receive_connection)
        callback_queue = yield self._queue_declare(rpc_channel, auto_delete=True)
        self.logger.info("callback queue: %s" % callback_queue)
        if exchange != "":
            yield self._exchange_declare(rpc_channel, exchange)
            yield self._queue_bind(rpc_channel, exchange=exchange, queue=callback_queue, routing_key=callback_queue)
        rpc_channel.basic_consume(self._rpc_callback_process, queue=callback_queue)
        raise gen.Return(callback_queue)

    def _rpc_callback_process(self, unused_channel, basic_deliver, properties, body):
        if properties.correlation_id in self._rpc_corr_id_dict:
            self._rpc_corr_id_dict[properties.correlation_id].set_result(body)

    def _call(self, exchange, callback_queue, routing_key, body, timeout=None):
        future = Future()
        corr_id = str(uuid.uuid1())
        self._rpc_corr_id_dict[corr_id] = future
        self.publish(exchange, routing_key, body,
                     properties=BasicProperties(correlation_id=corr_id,
                                                reply_to=callback_queue))

        def on_timeout():
            self.logger.error("timeout")
            del self._rpc_corr_id_dict[corr_id]
            future.set_exception(RabbitMQError('rpc timeout'))

        if timeout is not None:
            self._io_loop.add_timeout(float(timeout), on_timeout)
        return future

    def close(self):
        if self._publish_connection is not None and self._publish_connection.is_open:
            self._publish_connection.close()
        if self._receive_connection is not None and self._receive_connection.is_open:
            self._receive_connection.close()