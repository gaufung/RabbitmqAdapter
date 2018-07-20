# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import logging
import uuid
import re
import datetime
import functools
import pika
from tornado.queues import Queue
from tornado.gen import Return, coroutine
from tornado.ioloop import IOLoop


__author__ = ["feng.gao@aispeech.com"]


class AMQPRpcObject(object):
    EXCHANGE_TYPE = "topic"
    LOCALHOST = "127.0.0.1"
    LOGGER_HANDLER = None

    @classmethod
    def _get_log(cls, *names):
        logger =  logging.getLogger(".".join((cls.__module__, cls.__name__) + names))
        if AMQPRpcObject.LOGGER_HANDLER is not None:
            logger.addHandler(AMQPRpcObject.LOGGER_HANDLER)
        return logger

    def __init__(self, amqp_url, logger_handler):
        """
        initialize an AMQPRpcObject instance
        :param amqp_url: amqp url, it can be either 'amqp://dev:aispeech2018@10.12.7.22:5672/' or "127.0.0.1"
        """
        self._parameter = pika.ConnectionParameters(amqp_url) if amqp_url == self.LOCALHOST else \
            pika.URLParameters(amqp_url)
        AMQPRpcObject.LOGGER_HANDLER = logger_handler


class AsyncRabbitMQ(AMQPRpcObject):
    def __init__(self, amqp_url, io_loop=None, is_auto_delete=True, pool_size=4, logger_handler=None):
        """
        It is an "everything-in-one" RabbitMQ client, including interfaces as follows:
            - producer => `publish` method
            - consumer => `consume` method
            - rpc client => `call` method
            - rpc server => `service` method

        update:
            Taking performance into consideration, all above interfaces are designed by following strategies:
            1. It provides rabbitmq connection pool and its size is given by pool_size.
            2. Every time invoking `publish` method, it fetches one connection from pool and creates new
               a channel to push body. After publishing message, it closes channel and return connection to
               pool.
            3. It also creates two connections for service and consume respectively.
        :param amqp_url: amqp url, it can be either 'amqp://dev:aispeech2018@10.12.7.22:5672/' or "127.0.0.1"
        :param io_loop: the default is tornado.ioloop.IOLoop.current()
        :param is_auto_delete: whether delete queue when losing connection
        :param pool_size: pool size
        :param logger_handler: handler for logging
        """
        super(AsyncRabbitMQ, self).__init__(amqp_url, logger_handler)
        if io_loop is None:
            io_loop = IOLoop.current()
        self._io_loop = io_loop
        self._connection_pool = Queue(maxsize=pool_size)
        self._consumer_connection = Queue(maxsize=1)
        self._service_connection = Queue(maxsize=1)
        self._is_auto_delete = is_auto_delete
        self._consumer_routing_key_handlers_dict = dict()
        self._service_routing_key_handlers_dict = dict()
        self._reply_queue_dict=dict()
        self._initialize_connection_pool(pool_size)

    def _initialize_connection_pool(self, size):
        log = self._get_log("_initialize_connection_pool")
        log.info("starting establishing connection")
        for i in range(size):
            self._try_connection(functools.partial(self._init_open_connection,
                                                   pool=self._connection_pool))
        log.info("starting establishing consumer connection")
        self._try_connection(functools.partial(self._init_open_connection,
                                               pool=self._consumer_connection))
        log.info("starting establishing service connection")
        self._try_connection(functools.partial(self._init_open_connection,
                                               pool=self._service_connection))

    @coroutine
    def wait_connected(self):
        """
        waiting conenction pool, consumer connection, service connection establish.
        :return:
        """
        conn = yield self._connection_pool.get()
        self._connection_pool.put(conn)
        conn = yield self._consumer_connection.get()
        self._consumer_connection.put(conn)
        conn = yield self._service_connection.get()
        self._service_connection.put(conn)

    def _try_connection(self, open_callback):
        pika.TornadoConnection(parameters=self._parameter,
                               on_open_callback=open_callback,
                               custom_ioloop=self._io_loop)

    def _init_open_connection(self, conn, pool):
        log = self._get_log("_init_open_connection")
        log.info("having been established connection")
        pool.put(conn)

    def _open_channel(self, channel, channel_queue):
        log = self._get_log("_open_channel")
        log.info("open a channel")
        channel_queue.put(channel)

    def _on_exchange_declare(self, channel, exchange_name, done_queue):
        log = self._get_log("_on_exchange_declare")
        log.info("start declaring exchange")
        channel.exchange_declare(callback=functools.partial(self._on_exchange_declare_ok,
                                                            done_queue=done_queue),
                                 exchange=exchange_name,
                                 exchange_type=self.EXCHANGE_TYPE,
                                 durable=False,
                                 auto_delete=self._is_auto_delete)

    def _on_exchange_declare_ok(self, unframe, done_queue):
        log = self._get_log("_on_exchange_declare_ok")
        log.info("declared exchange")
        done_queue.put(True)

    def _on_queue_declare(self, channel, queue_name, done_queue):
        log = self._get_log("_on_queue_declare")
        log.info("start declaring queue")
        channel.queue_declare(callback=functools.partial(self._on_queue_declare_ok,
                                                         done_queue=done_queue),
                              queue=queue_name,
                              durable=False,
                              exclusive=False,
                              auto_delete=self._is_auto_delete)

    def _on_queue_declare_ok(self, unframe, done_queue):
        log = self._get_log("_on_queue_declare_ok")
        log.info("declared queue")
        done_queue.put(True)

    def _on_queue_bind(self, channel, exchange_name, queue_name, routing_key, done_queue):
        log = self._get_log("_on_queue_bind")
        log.info("start binding queue")
        channel.queue_bind(callback=functools.partial(self._on_queue_bind_ok,
                                                      done_queue=done_queue),
                           queue=queue_name,
                           exchange=exchange_name,
                           routing_key=routing_key)

    def _on_queue_bind_ok(self, unframe, done_queue):
        log = self._get_log("_on_queue_bind_ok")
        log.info("bound queue")
        done_queue.put(True)

    @coroutine
    def _initialize(self, channel, exchange_name, queue_name, routing_key):
        done_queue = Queue(maxsize=1)
        self._on_exchange_declare(channel, exchange_name, done_queue)
        yield done_queue.get()
        self._on_queue_declare(channel, queue_name, done_queue)
        yield done_queue.get()
        self._on_queue_bind(channel, exchange_name, queue_name, routing_key, done_queue)
        yield done_queue.get()

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
    def publish(self, exchange_name, routing_key, body, properties=None):
        """
        publish interface.
        1. wait connection for connection pool (asynchronous)
        2. put it back into connection pool
        3. creating a new channel by using connection
        4. publish message
        5. close channel
        :param exchange_name: exchange name. it needs to be declared ahead.(consumer's task)
        :param routing_key: routing key
        :param body: message to be sent
        :param properties: properties (for replying to)
        :return: None
        """
        log = self._get_log("publish")
        conn = yield self._connection_pool.get()
        log.info("got connection for connection pool")
        self._connection_pool.put(conn)
        channel_queue = Queue(maxsize=1)
        conn.channel(functools.partial(self._open_channel, channel_queue=channel_queue))
        channel = yield channel_queue.get()
        log.info("got channel")
        log.info("starting publish")
        log.info("exchange name: %s" % (exchange_name,))
        log.info("routing key: %s" % routing_key)
        log.info("to send body: %s" % body)
        channel.basic_publish(exchange=exchange_name,
                              routing_key=routing_key,
                              body=body,
                              properties=properties)
        log.info("sent message. closing channel")
        channel.close()

    @coroutine
    def consume(self, exchange_name, queue_name, routing_key, handler):
        """
        consume interface
        1. wait connection for _consumer_connection
        2. put it back
        3. creating a new channel by using connection
        4. initialize tasks:
            a. declare exchange
            b. declare queue
            c. bind queue to exchange
        5. store handler, taking routing key as dictionary key
            (using regular expression, see `_routing_key_pattern` for more detail)
        6. consume message
        :param exchange_name: exchange name
        :param queue_name: queue name
        :param routing_key: routing key.
        :param handler:
        :return:
        """
        log = self._get_log("consume")
        conn = yield self._consumer_connection.get()
        log.info("got a connection")
        self._consumer_connection.put(conn)
        channel_queue = Queue(maxsize=1)
        conn.channel(functools.partial(self._open_channel, channel_queue=channel_queue))
        channel = yield channel_queue.get()
        log.info("crated a new channel")
        yield self._initialize(channel, exchange_name,queue_name, routing_key)
        log.info("initialized exchange ane queue")
        log.info("start consuming")
        self._consumer_routing_key_handlers_dict[self._routing_key_pattern(routing_key)] = handler
        log.info("start consuming")
        channel.basic_consume(self._consume_handler_delivery, queue=queue_name)

    def _consume_handler_delivery(self, channel, method, header, body):
        log = self._get_log("_consume_handler_delivery")
        log.info("consume body %s" % (body,))
        self._io_loop.spawn_callback(self._consume_process_message,
                                     channel=channel,
                                     method=method,
                                     header=header,
                                     body=body)

    @coroutine
    def _consume_process_message(self, channel, method, header, body):
        log = self._get_log("_consume_process_message")
        log.info("start processing")
        handler = self._lookup_handler(method.routing_key)
        if handler is None:
            log.error("routing_key %s handler not found" % method.routing_key)
            return
        result = yield handler(channel, method, header, body)
        if result:
            log.info("message process success")
        else:
            log.error("message process failed")
            pass

    @coroutine
    def service(self, exchange_name, queue_name, routing_key, handler):
        """
        service interface
        1. wait connection from _service_connection
        2. put it back
        3. create a new channel
        4. initialize tasks:
            a. declare exchange
            b. declare queue
            c. bind queue to exchange
        5. store handler, taking routing key as dictionary key
            (using regular expression, see `_routing_key_pattern` for more detail)
        6. service
        :param exchange_name: exchange name
        :param queue_name: queue for receiving message.
        :param routing_key: routing key
        :param handler: message's handler
        :return: None
        """
        log = self._get_log("service")
        conn = yield self._service_connection.get()
        log.info('got connection')
        self._service_connection.put(conn)
        channel_queue = Queue(maxsize=1)
        conn.channel(functools.partial(self._open_channel, channel_queue=channel_queue))
        channel = yield channel_queue.get()
        log.info("created a channel")
        yield self._initialize(channel, exchange_name, queue_name, routing_key)
        log.info("starting servicing")
        self._service_routing_key_handlers_dict[self._routing_key_pattern(routing_key)] = handler
        channel.basic_consume(self._service_handler_delivery, queue=queue_name)

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
            log.error("handler not found")
            return
        response = yield handler(body)
        if response is not None:
            log.info('service response routing key: %s' % props.reply_to)
            log.info('service correlation id: %s' % props.correlation_id)
            log.info("service sends body %s" % response)
            channel.basic_publish(exchange=method.exchange,
                                  routing_key=props.reply_to,
                                  properties=pika.BasicProperties(correlation_id=props.correlation_id),
                                  body=str(response))
            channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            log.error("response is None")

    @coroutine
    def call(self, exchange_name, routing_key, body, callback_queue=None, timeout=None):
        """
        call interface
        1. wait a connection from connection_pool
        2. put it back
        3. create callback queue if it doesn't give
        4. create a unique `corr_id` for current call and a Queue(maxsize=1) instance. Combine them as key-value pair
        5. create channel to wait result
        6. initialize tasks:
            a. declare exchange
            b. declare queue (callback_queue)
            c. bind queue to exchange
        7. start consuming (waiting result from service)
        8. create a new channel to send request
        9. send request to server
        10. close sending channel
        11. if timeout is set, start a coroutine when timeout
        12. wait result from previous `Queue(maxsize=1)` (asynchronous)
        :param exchange_name:
        :param routing_key:
        :param body:
        :param callback_queue:
        :param timeout:
        :return:
        """
        log = self._get_log("call")
        conn = yield self._connection_pool.get()
        self._connection_pool.put(conn)
        if callback_queue is None:
            callback_queue = "rpc_answer_%s" % str(uuid.uuid4())
            log.info("generating callback queue  %s randomly")
        corr_id = str(uuid.uuid4())
        log.info("generating correlation id %s" % corr_id)
        log.info("to send body %s" % body)
        queue = Queue(maxsize=1)
        self._reply_queue_dict[corr_id] = queue
        channel_queue = Queue(maxsize=1)
        conn.channel(functools.partial(self._open_channel, channel_queue=channel_queue))
        channel = yield channel_queue.get()
        log.info("got consume channel")
        yield self._initialize(channel, exchange_name, callback_queue, callback_queue)
        log.info("initialized callback queue")
        log.info("start calling")
        channel.basic_consume(self._client_on_message, queue=callback_queue)
        log.info("routing_key: %s" % routing_key)
        log.info("correlation_id: %s " % corr_id)
        log.info("reply to: %s " % callback_queue)
        log.info("send body: %s" % body)
        conn.channel(functools.partial(self._open_channel, channel_queue=channel_queue))
        publish_channel = yield channel_queue.get()
        log.info("got publishing channel")
        publish_channel.basic_publish(exchange=exchange_name,
                                      routing_key=routing_key,
                                      properties=pika.BasicProperties(correlation_id=corr_id,
                                                                      reply_to=callback_queue),
                                      body=body)
        log.info("close publishing channel")
        publish_channel.close()
        if timeout is not None:
            log.info("add timeout %s" % timeout)
            self._io_loop.add_timeout(datetime.timedelta(days=0, seconds=timeout),
                                      functools.partial(self._on_timeout, corr_id=corr_id))
        result = yield queue.get()
        raise Return(result)

    def _client_on_message(self, ch, method, props, body):
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
    def _on_timeout(self, corr_id):
        log = self._get_log("_on_timeout")
        log.info("timeout")
        if corr_id in self._reply_queue_dict:
            self._reply_queue_dict[corr_id].put(None)
            log.info("delete correlation_id %s in _reply_queue_dict" % corr_id)
            del self._reply_queue_dict[corr_id]
        else:
            log.info("correlation_id %s doest not exist. " % corr_id)
