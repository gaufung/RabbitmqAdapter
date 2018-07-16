# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import logging
import uuid
import re
import datetime
import functools
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
        """
        Initialize a AsyncRabbitMQ instance
        :param amqp_url: amqp_url: amqp url, it can be either 'amqp://dev:aispeech2018@10.12.7.22:5672/' or "127.0.0.1"
        :param io_loop: io_loop, the default is tornado.ioloop.IOLoop.current()
        """
        super(AsyncRabbitMQ, self).__init__(amqp_url)
        if io_loop is None:
            io_loop = IOLoop.current()
        self._io_loop = io_loop
        self._connection = None
        self._channel = None
        self._channel_queue = Queue(maxsize=1)
        self._exchange_declare_dict = dict()
        self._queue_declare_dict = dict()
        self._queue_bind_dict = dict()
        self._consumer_routing_key_handlers_dict = dict()
        self._service_routing_key_handlers_dict = dict()
        self._reply_queue_dict = dict()

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
                                           exchange_type=self.EXCHANGE_TYPE,
                                           passive=passive,
                                           auto_delete=True)
            self._exchange_declare_dict[exchange_name].put(True)
            log.info("exchange %s has been declared" % exchange_name)
        except Exception as e:
            log.error("error with exchange declaring %s" % e)
            raise e

    def _on_exchange_declare_ok(self, unframe):
        log = self._get_log("_on_exchange_declare_ok")
        log.info("exchange declare ok")
        pass

    @coroutine
    def publish(self, exchange_name, routing_key, message, properties=None):
        """
        publisher client for rabbitmq.
        :param exchange_name: exchange name
        :param routing_key: routing key
        :param message: message
        :param properties: properties for publish
        :return:
        """
        log = self._get_log("publish")
        if self._channel is None:
            log.info("publish start connect")
            self._connect()
            yield self._channel_queue.get()
        if exchange_name not in self._exchange_declare_dict:
            log.info("declaring exchange: %s" % exchange_name)
            self._exchange_declare_dict[exchange_name] = Queue(maxsize=1)
            self._on_exchange_declare(exchange_name, True)
            yield self._exchange_declare_dict[exchange_name].get()
        self._channel.basic_publish(exchange=exchange_name,
                                    routing_key=routing_key,
                                    body=message,
                                    properties=properties)

    @coroutine
    def consume(self, exchange_name, queue_name, routing_key, handler):
        """
        consumer client rabbitmq
        :param exchange_name: exchange name
        :param queue_name: binding queue
        :param routing_key: routing key
        :param handler: handler for message
        :return: None
        """
        log = self._get_log("consume")
        if self._channel is None:
            log.info("consume connects")
            self._connect()
            yield self._channel_queue.get()
        if exchange_name not in self._exchange_declare_dict:
            log.info("consume declares exchange %s" % exchange_name)
            self._exchange_declare_dict[exchange_name] = Queue(maxsize=1)
            self._on_exchange_declare(exchange_name, False)
            yield self._exchange_declare_dict[exchange_name].get()
        if queue_name not in self._queue_declare_dict:
            log.info("consume declares queue %s" % queue_name)
            self._queue_declare_dict[queue_name] = Queue(maxsize=1)
            self._on_queue_declare(queue_name)
            yield self._queue_declare_dict[queue_name].get()
        if queue_name not in self._queue_bind_dict:
            log.info("consume binds queue %s" % queue_name)
            self._queue_bind_dict[queue_name] = Queue(maxsize=1)
            self._on_queue_bind(exchange_name, queue_name, routing_key)
            yield self._queue_bind_dict[queue_name].get()
        self._consumer_routing_key_handlers_dict[self._routing_key_pattern(routing_key)] = handler
        self._channel.basic_consume(self._consume_handler_delivery, queue=queue_name)

    def _consume_handler_delivery(self, channel, method, header, body):
        log = self._get_log("_consume_handler_delivery")
        log.info("consume body %s" % (body,))
        self._io_loop.spawn_callback(self._consume_process_message, body=body, channel=channel, method=method,
                                     header=header)

    @coroutine
    def _consume_process_message(self, body, channel, method, header):
        log = self._get_log("_consume_process_message")
        log.info("start processing")
        handler = self._lookup_handler(method.routing_key)
        if handler is None:
            log.info("routing_key %s handler not found" % method.routing_key)
            return
        result = yield handler(body)
        if result:
            log.info("message process success")
            channel.basic_ack(delivery_tag=method.delivery_tag)
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
        log = self._get_log("_on_queue_declare_ok")
        log.info("queue declare ok")

    def _on_queue_bind(self, exchange_name, queue_name, routing_key):
        log = self._get_log("_on_queue_bind")
        log.info("exchange: %s; queue  %s; routing_key %s", exchange_name, queue_name, routing_key)
        self._channel.queue_bind(callback=self._on_queue_bind_ok,
                                 queue=queue_name,
                                 exchange=exchange_name,
                                 routing_key=routing_key)
        self._queue_bind_dict[queue_name].put(True)

    def _on_queue_bind_ok(self, method_frame):
        log = self._get_log("_on_queue_bind_ok")
        log.info("queue binds ok")

    @staticmethod
    def _routing_key_pattern(routing_key):
        """
        as topic exchange routing_key pattern. only supports `topic.*` form.
        rules:
            - 'aispeech.*" => "aispeech\.\w+?"
            - "aispeech.aihome.*" => "aipseech\.aihome\.\w+?"
        :param routing_key: consumer routing key
        :return: stored handler
        """
        pattern = routing_key.replace("*", "\w+?").replace(".", "\.")
        return re.compile(pattern)

    def _lookup_handler(self, routing_key, is_consume=True):
        """
        according to routing key, lookup the best match handler.
        e.g.
            routing_key: dog.black
            handlers: dog\.\w+? matches
        :param routing_key: routing key
        :param is_consume: True: lookup handler in consume; False: lookup handler in service
        :return: if matching, return handler otherwise return None
        """
        if is_consume:
            for pattern, handler in self._consumer_routing_key_handlers_dict.items():
                if pattern.match(routing_key):
                    return handler
            return None
        else:
            for pattern, handler in self._service_routing_key_handlers_dict.items():
                if pattern.match(routing_key):
                    return handler
            return None

    @coroutine
    def service(self, exchange_name, queue_name, routing_key, handler):
        """
        start service for rpc
        :param exchange_name: exchange name
        :param queue_name: queue name
        :param routing_key: routing key. e.g. dog.*
        :param handler: handler for this routing key
        :return: None
        """
        log = self._get_log("service")
        if self._channel is None:
            log.info("service connects")
            self._connect()
            yield self._channel_queue.get()
        if exchange_name not in self._exchange_declare_dict:
            log.info("service declares exchange %s " % exchange_name)
            self._exchange_declare_dict[exchange_name] = Queue(maxsize=1)
            self._on_exchange_declare(exchange_name, False)
            yield self._exchange_declare_dict[exchange_name].get()
        if queue_name not in self._queue_declare_dict:
            log.info("service declare queue %s" % queue_name)
            self._queue_declare_dict[queue_name] = Queue(maxsize=1)
            self._on_queue_declare(queue_name)
            yield self._queue_declare_dict[queue_name].get()
        if queue_name not in self._queue_bind_dict:
            log.info("service bind queue")
            self._queue_bind_dict[queue_name] = Queue(maxsize=1)
            self._on_queue_bind(exchange_name, queue_name, routing_key)
            yield self._queue_bind_dict[queue_name].get()
        self._service_routing_key_handlers_dict[self._routing_key_pattern(routing_key)] = handler
        self._channel.basic_consume(self._service_handler_delivery, queue=queue_name)

    def _service_handler_delivery(self, channel, method, props, body):
        log = self._get_log("_service_handler_delivery")
        log.info("service body %s " % body)
        self._io_loop.spawn_callback(self._service_process_message,
                                     channel=channel,
                                     method=method,
                                     props=props,
                                     body=body)

    @coroutine
    def _service_process_message(self, channel, method, props, body):
        log = self._get_log("_service_process_message")
        log.info("start process")
        handler = self._lookup_handler(method.routing_key, is_consume=False)
        if handler is None:
            log.info("handler not found")
            return
        response = yield handler(body)
        if response is not None:
            log.info('service response routing key: %s' % props.reply_to)
            log.info('service correlation id: %s' % props.correlation_id)
            log.info("service sends body %s" % response)
            self._channel.basic_publish(exchange=method.exchange,
                                        routing_key=props.reply_to,
                                        properties=pika.BasicProperties(correlation_id=props.correlation_id),
                                        body=str(response))
            self._channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            log.info("response is None")

    @coroutine
    def call(self, exchange_name, queue_name, routing_key, body, timeout=None):
        """
        call client for rpc.
        :param exchange_name: exchange name
        :param queue_name: queue name
        :param routing_key: routing key
        :param body: send body
        :param timeout: timeout after rpc call
        :return: result
        """
        log = self._get_log("call")
        callback_queue = "rpc_answer_%s" % str(uuid.uuid4())
        corr_id = str(uuid.uuid4())
        log.info("generating correlation id %s" % corr_id)
        log.info("to send body %s" % body)
        queue = Queue(maxsize=1)
        self._reply_queue_dict[corr_id] = queue
        # open connection and send message
        if self._channel is None:
            log.info("client connect")
            self._connect()
            yield self._channel_queue.get()
        if exchange_name not in self._exchange_declare_dict:
            log.info("client declares exchange %s. " % exchange_name)
            self._exchange_declare_dict[exchange_name] = Queue(maxsize=1)
            self._on_exchange_declare(exchange_name, True)
            yield self._exchange_declare_dict[exchange_name].get()
        if queue_name not in self._queue_declare_dict:
            log.info("client declares queue: %s." % queue_name)
            self._queue_declare_dict[queue_name] = Queue(maxsize=1)
            self._on_queue_declare(queue_name)
            yield self._queue_declare_dict[queue_name].get()
        if callback_queue not in self._queue_declare_dict:
            log.info("client declares queue: %s." % callback_queue)
            self._queue_declare_dict[callback_queue] = Queue(maxsize=1)
            self._on_queue_declare(callback_queue)
            yield self._queue_declare_dict[callback_queue].get()
        if queue_name not in self._queue_bind_dict:
            log.info("client binds queue %s" % queue_name)
            self._queue_bind_dict[queue_name] = Queue(maxsize=1)
            self._on_queue_bind(exchange_name, queue_name, routing_key)
            yield self._queue_bind_dict[queue_name].get()
        if callback_queue not in self._queue_bind_dict:
            log.info("client binds queue %s" % callback_queue)
            self._queue_bind_dict[callback_queue] = Queue(maxsize=1)
            self._on_queue_bind(exchange_name, queue_name=callback_queue, routing_key=callback_queue)
        self._channel.basic_consume(self._client_on_message, queue=callback_queue)
        log.info("routing_key: %s" % routing_key)
        log.info("correlation_id: %s " % corr_id)
        log.info("reply to: %s " % callback_queue)
        log.info("send body: %s" % body)
        self._channel.basic_publish(exchange=exchange_name,
                                    routing_key=routing_key,
                                    properties=pika.BasicProperties(correlation_id=corr_id,
                                                                    reply_to=callback_queue),
                                    body=body)
        # end up with push and wait request
        if timeout is not None:
            log.info("add timeout %s" % timeout)
            self._io_loop.add_timeout(datetime.timedelta(days=0, seconds=timeout),
                                      functools.partial(self._on_timeout,correlation_id=corr_id))
        result = yield queue.get()
        raise Return(result)

    def _client_on_message(self,ch, method, props, body):
        log = self._get_log("_client_on_message")
        log.info("receive body: %s" % body)
        corr_id = props.correlation_id
        if corr_id in self._reply_queue_dict:
            log.info("get response")
            self._reply_queue_dict[corr_id].put(body)
            log.info("delete corr_id %s in _reply_queue." % corr_id)
            del self._reply_queue_dict[corr_id]
        else:
            log.info("valid response")
            pass

    @coroutine
    def _on_timeout(self, correlation_id):
        log = self._get_log("_on_timeout")
        log.info("timeout")
        if correlation_id in self._reply_queue_dict:
            self._reply_queue_dict[correlation_id].put(None)
            log.info("delete correlation_id %s in _reply_queue_dict" % correlation_id)
            del self._reply_queue_dict[correlation_id]
        else:
            log.info("correlation_id %s doest not exist. " % correlation_id)
