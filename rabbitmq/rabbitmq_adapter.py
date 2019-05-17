# -*- encoding:utf-8 -*-
import datetime
import functools
import logging
import uuid
import warnings

from pika import BasicProperties
from pika import TornadoConnection
from pika import URLParameters, ConnectionParameters, BlockingConnection
from tornado import gen
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado.queues import Queue


class RabbitMQError(Exception):
    pass


class RabbitMQConnectError(RabbitMQError):
    pass


class RabbitMQPublishError(RabbitMQError):
    pass


class RabbitMQReceiveError(RabbitMQError):
    pass


class RabbitMQTimeoutError(RabbitMQError):
    pass


class RabbitMQRpcError(RabbitMQError):
    pass


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
        try:
            self._connection = BlockingConnection(self._parameter)
            self._channel = self._connection.channel()
        except Exception:
            raise RabbitMQConnectError("failed to create a connection and a channel.")

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
        self.logger.info("[publishing] exchange name: %s; routing key: %s", exchange, routing_key)
        if self._channel is None:
            raise RabbitMQConnectError("channel has not been initialized")
        properties = kwargs.pop("properties") if kwargs.has_key("properties") else None
        try:
            for message in messages:
                self._channel.basic_publish(exchange=exchange,
                                            routing_key=routing_key,
                                            body=str(message),
                                            properties=properties)
        except Exception as e:
            self.logger.error("failed to publish message. %s", e.message)
            raise RabbitMQPublishError("failed to publish message.")

    def publish_messages(self, exchange, messages, **kwargs):
        self.logger.info("[publish_message] exchange name: %s", exchange)
        if self._channel is None:
            raise RabbitMQConnectError("channel has not been initialized")
        if not isinstance(messages, dict):
            raise RabbitMQError("messages is not dict")
        properties = kwargs.pop("properties") if "properties" in kwargs else None
        try:
            for routing_key, message in messages.items():
                self.logger.info("routing key:%s, message: %s", routing_key, message)
                self._channel.basic_publish(exchange=exchange,
                                            routing_key=routing_key,
                                            body=str(message),
                                            properties=properties)
        except Exception as e:
            self.logger.error("fail to publish message: %s", e.message)
            raise RabbitMQPublishError("fail to publish message.")

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
        warnings.warn("Call disconnect() method, using `with` context instead.", category=DeprecationWarning,
                      stacklevel=2)
        self._disconnect()


class _AsyncConnection(object):
    INIT_STATUS = "init"
    CONNECTING_STATUS = "connecting"
    OPEN_STATUS = "open"
    CLOSE_STATUS = "close"
    TIMEOUT_STATUS = 'timeout'

    def __init__(self, rabbitmq_url, io_loop, timeout=10):
        self._parameter = ConnectionParameters("127.0.0.1") if rabbitmq_url in ["localhost", "127.0.0.1"] else \
            URLParameters(rabbitmq_url)
        self._io_loop = io_loop
        self._timeout = timeout
        self._logger = logging.getLogger(__name__)
        self._queue = Queue(maxsize=1)
        self._current_status = self.INIT_STATUS

    @property
    def logger(self):
        return self._logger

    @property
    def status_ok(self):
        return self._current_status != self.CLOSE_STATUS \
               and self._current_status != self.TIMEOUT_STATUS

    @gen.coroutine
    def get_connection(self):
        if self._current_status == self.INIT_STATUS:
            self._current_status = self.CONNECTING_STATUS
            yield self._connect()
        conn = yield self._top()
        raise gen.Return(conn)

    @gen.coroutine
    def _connect(self):
        try:
            connection = yield self._try_connect()
            if connection is not None:
                self._queue.put(connection)
        except Exception as e:
            self.logger.error("failed to connect rabbitmq. %s", e)

    @gen.coroutine
    def _top(self):
        conn = yield self._queue.get()
        self._queue.put(conn)
        raise gen.Return(conn)

    def _on_timeout(self, future):
        if self._current_status == self.CONNECTING_STATUS:
            self.logger.error("creating connection time out")
            self._current_status = self.TIMEOUT_STATUS
            future.set_exception(Exception("connection Timeout"))

    def _try_connect(self):
        self.logger.info("creating connection")
        future = Future()
        self._io_loop.add_timeout(datetime.timedelta(seconds=self._timeout),
                                  functools.partial(self._on_timeout, future=future))

        def open_callback(unused_connection):
            self.logger.info("created connection")
            self._current_status = self.OPEN_STATUS
            future.set_result(unused_connection)

        def open_error_callback(connection, exception):
            self.logger.error("open connection with error: %s", exception)
            self._current_status = self.CLOSE_STATUS
            future.set_exception(exception)

        def close_callback(connection, reply_code, reply_text):
            self.logger.error("closing connection: reply code:%s, reply_text: %s. system will exist",
                              reply_code, reply_text)
            self._current_status = self.CLOSE_STATUS

        TornadoConnection(self._parameter,
                          on_open_callback=open_callback,
                          on_open_error_callback=open_error_callback,
                          on_close_callback=close_callback,
                          custom_ioloop=self._io_loop)
        return future


class TornadoAdapter(object):
    _NORMAL_CLOSE_CODE = 200  # connection or channel closes normally
    """
    If channel closing is initiated by user (either directly of indirectly by closing a 
    connection containing the channel) and closing concludes gracefully without Channel.
    Close from the broker and without loss of connection, the callback will receive 0 as 
    reply_code and empty string as reply_text.
    """
    _USER_CLOSE_CODE = 0

    _NO_ROUTE_CODE = 312

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
        if io_loop is None:
            io_loop = IOLoop.current()
        self._io_loop = io_loop
        self._logger = logging.getLogger(__name__)
        self._publish_conn = _AsyncConnection(rabbitmq_url, io_loop)
        self._receive_conn = _AsyncConnection(rabbitmq_url, io_loop)
        self._rpc_exchange_dict = dict()
        self._rpc_corr_id_dict = dict()

    @gen.coroutine
    def connect(self):
        """
        establishing two connections for publishing and receiving respectively.
        :return: True if establish successfully.
        """
        warnings.warn("Calling connect() method is unnecessary. "
                      "Call `publish`, `receive` and `rpc` methods straightly. ",
                      category=DeprecationWarning, stacklevel=2)
        raise gen.Return(True)

    @property
    def logger(self):
        """
        logger
        :return:
        """
        return self._logger

    def _create_channel(self, connection, return_callback=None,
                        cancel_callback=None, close_callback=None):
        self.logger.info("creating channel")
        future = Future()

        def on_channel_flow(*args, **kwargs):
            pass

        def on_channel_cancel(frame):
            self.logger.error("channel was canceled")
            if cancel_callback is not None:
                if hasattr(cancel_callback, "__tornado_coroutine__"):
                    self._io_loop.spawn_callback(cancel_callback)
                else:
                    cancel_callback()

        def on_channel_closed(channel, reply_code, reply_txt):
            if reply_code not in [self._NORMAL_CLOSE_CODE, self._USER_CLOSE_CODE]:
                self.logger.error("channel closed. reply code: %d; reply text: %s. system will exist"
                                  , reply_code, reply_txt)
                if close_callback is not None:
                    if hasattr(close_callback, "__tornado_coroutine__"):
                        self._io_loop.spawn_callback(close_callback)
                    else:
                        close_callback()
                else:
                    raise Exception("channel was close unexpected. reply code: %d, reply text: %s"
                                    % (reply_code, reply_txt,))
            else:
                self.logger.info("reply code %s, reply txt: %s", reply_code, reply_txt)

        # if publish message failed, it will invoke this method.
        def on_channel_return(channel, method, property, body):
                self.logger.error("reject from server. reply code: %d, reply text: %s.",
                                  method.reply_code, method.reply_text)
                if return_callback is not None:
                    if hasattr(return_callback, "__tornado_coroutine__"):
                        self._io_loop.spawn_callback(return_callback)
                    else:
                        return_callback()
                else:
                    raise Exception("failed to publish message.")

        def open_callback(channel):
            self.logger.info("created channel")
            channel.add_on_close_callback(on_channel_closed)
            channel.add_on_return_callback(on_channel_return)
            channel.add_on_flow_callback(on_channel_flow)
            channel.add_on_cancel_callback(on_channel_cancel)
            future.set_result(channel)

        connection.channel(on_open_callback=open_callback)
        return future

    def _exchange_declare(self, channel, exchange=None, exchange_type='topic', **kwargs):
        self.logger.info("declaring exchange: %s " % exchange)
        future = Future()

        def callback(unframe):
            self.logger.info("declared exchange: %s", exchange)
            future.set_result(unframe)

        channel.exchange_declare(callback=callback,
                                 exchange=exchange, exchange_type=exchange_type, **kwargs)
        return future

    def _queue_declare(self, channel, queue='', **kwargs):
        self.logger.info("declaring queue: %s" % queue)
        future = Future()

        def callback(method_frame):
            self.logger.info("declared queue: %s", method_frame.method.queue)
            future.set_result(method_frame.method.queue)

        channel.queue_declare(callback=callback, queue=queue, **kwargs)
        return future

    def _queue_bind(self, channel, queue, exchange, routing_key=None, **kwargs):
        self.logger.info("binding queue: %s to exchange: %s", queue, exchange)
        future = Future()

        def callback(unframe):
            self.logger.info("bound queue: %s to exchange: %s", queue, exchange)
            future.set_result(unframe)

        channel.queue_bind(callback, queue=queue, exchange=exchange, routing_key=routing_key, **kwargs)
        return future

    @gen.coroutine
    def publish(self, exchange, routing_key, body,
                properties=None,  mandatory=True, return_callback=None, close_callback=None):
        """
        publish message. creating a brand new channel once invoke this method. After publishing, it closes the
        channel.
        :param exchange: exchange name
        :type exchange; str or unicode
        :param routing_key: routing key (e.g. dog.yellow, cat.big)
        :param body: message
        :param properties: properties
        :param mandatory: whether
        :param return_callback: failed callback
        :param close_callback: channel close callback
        :return: None
        """
        try:
            conn = yield self._publish_conn.get_connection()
            self.logger.info("preparing to publish. exchange: %s; routing_key: %s", exchange, routing_key)
            channel = yield self._create_channel(conn, return_callback=return_callback, close_callback=close_callback)
            try:
                if properties is None:
                    properties = BasicProperties(delivery_mode=2)
                channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body,
                                      mandatory=mandatory, properties=properties)
            except Exception as e:
                self.logger.error("failed to publish message. %s", e.message)
                raise RabbitMQPublishError("failed to publish message")
            finally:
                self.logger.info("closing channel")
                channel.close()
        except RabbitMQPublishError:
            raise
        except Exception:
            raise RabbitMQPublishError("failed to publish message")

    @gen.coroutine
    def receive(self, exchange, routing_key, queue_name, handler, no_ack=False, prefetch_count=0,
                return_callback=None, close_callback=None, cancel_callback=None):
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
        :param return_callback: channel return callback.
        :param close_callback: channel close callback
        :param cancel_callback: channel cancel callback
        :return: None
        """
        try:
            conn = yield self._receive_conn.get_connection()
            self.logger.info("[receive] exchange: %s; routing key: %s; queue name: %s", exchange, routing_key, queue_name)
            channel = yield self._create_channel(conn, close_callback=close_callback, cancel_callback=cancel_callback)
            yield self._queue_declare(channel, queue=queue_name, auto_delete=False, durable=True)
            if routing_key != "":
                yield self._exchange_declare(channel, exchange=exchange)
                yield self._queue_bind(channel, exchange=exchange, queue=queue_name, routing_key=routing_key)
            self.logger.info("[start consuming] exchange: %s; routing key: %s; queue name: %s",
                             exchange, routing_key, queue_name)
            channel.basic_qos(prefetch_count=prefetch_count)
            channel.basic_consume(functools.partial(self._on_message, exchange=exchange, handler=handler,
                                                    return_callback=return_callback, close_callback=close_callback)
                                  , queue=queue_name, no_ack=no_ack)
        except Exception as e:
            self.logger.error("failed to receive message. %s", e.message)
            raise RabbitMQReceiveError("failed to receive message")

    def _on_message(self, unused_channel, basic_deliver, properties, body, exchange, handler=None,
                    return_callback=None, close_callback=None):
        self.logger.info("consuming message")
        self._io_loop.spawn_callback(self._process_message, unused_channel, basic_deliver, properties, body,
                                     exchange, handler, return_callback, close_callback)

    @gen.coroutine
    def _process_message(self, unused_channel, basic_deliver, properties, body, exchange, handler=None,
                         return_callback=None, close_callback=None):
        try:
            result = yield handler(body)
            self.logger.info("message has been processed successfully")
            if properties is not None \
                    and properties.reply_to is not None:
                self.logger.info("sending result back to %s", properties.reply_to)
                yield self.publish(exchange=exchange,
                                   routing_key=properties.reply_to,
                                   properties=BasicProperties(correlation_id=properties.correlation_id),
                                   body=str(result), mandatory=False,
                                   return_callback=return_callback, close_callback=close_callback)
        except Exception:
            import traceback
            self.logger.error(traceback.format_exc())
            raise RabbitMQReceiveError("failed to handle received message.")
        finally:
            unused_channel.basic_ack(basic_deliver.delivery_tag)

    @gen.coroutine
    def rpc(self, exchange, routing_key, body, timeout, ttl,
            close_callback=None, return_callback=None, cancel_callback=None):
        """
        rpc call. It create a queue randomly when encounters first call with the same exchange name. Then, it starts
        consuming the created queue(waiting result). It publishes message to rabbitmq with properties that has correlation_id
        and reply_to. It will starts a coroutine to wait timeout and raises an `Exception("timeout")`.
        ttl is used to message's time to live in rabbitmq queue. If server has been sent result, it return it asynchronously.
        :param exchange: exchange name
        :param routing_key: routing key(e.g. dog.Yellow, cat.big)
        :param body: message
        :param timeout: rpc timeout (second)
        :param ttl: message's ttl (second)
        :type ttl: int
        :param close_callback: channel close callback
        :param return_callback: channel close callback
        :param cancel_callback: channel cancel callback
        :return: result or Exception("timeout")
        """
        try:
            conn = yield self._receive_conn.get_connection()
            self.logger.info("preparing to rpc call. exchange: %s; routing key: %s", exchange, routing_key)
            if exchange not in self._rpc_exchange_dict:
                self._rpc_exchange_dict[exchange] = Queue(maxsize=1)
                callback_queue = yield self._initialize_rpc_callback(exchange, conn, cancel_callback=cancel_callback,
                                                                     close_callback=close_callback)
                yield self._rpc_exchange_dict[exchange].put(callback_queue)
            callback_queue = yield self._rpc_exchange_dict[exchange].get()
            yield self._rpc_exchange_dict[exchange].put(callback_queue)
            corr_id = str(uuid.uuid1())
            self.logger.info("starting rpc calling correlation id: %s", corr_id)
            if corr_id in self._rpc_corr_id_dict:
                self.logger.warning("correlation id exists before calling. %s", corr_id)
                del self._rpc_corr_id_dict[corr_id]
            self._rpc_corr_id_dict[corr_id] = Future()
            properties = BasicProperties(correlation_id=corr_id, reply_to=callback_queue, expiration=str(ttl*1000))
            yield self.publish(exchange, routing_key, body,
                               properties=properties, mandatory=True,
                               close_callback=close_callback, return_callback=return_callback)
            self.logger.info("rpc message has been sent. %s", corr_id)
            result = yield self._wait_result(corr_id, timeout)
            if corr_id in self._rpc_corr_id_dict:
                del self._rpc_corr_id_dict[corr_id]
            self.logger.info("rpc message gets response. %s", corr_id)
            raise gen.Return(result)
        except (gen.Return, RabbitMQError):
            raise
        except Exception as e:
            self.logger.error("failed to rpc call. %s", e.message)
            raise RabbitMQRpcError("failed to rpc call")

    @gen.coroutine
    def _initialize_rpc_callback(self, exchange, conn, close_callback=None, cancel_callback=None):
        self.logger.info("initialize rpc callback queue")
        rpc_channel = yield self._create_channel(conn, close_callback=close_callback, cancel_callback=cancel_callback)
        callback_queue = yield self._queue_declare(rpc_channel, auto_delete=True)
        self.logger.info("callback queue: %s", callback_queue)
        if exchange != "":
            yield self._exchange_declare(rpc_channel, exchange)
            yield self._queue_bind(rpc_channel, exchange=exchange, queue=callback_queue, routing_key=callback_queue)
        rpc_channel.basic_consume(self._rpc_callback_process, queue=callback_queue)
        raise gen.Return(callback_queue)

    def _rpc_callback_process(self, unused_channel, basic_deliver, properties, body):
        self.logger.info("rpc get response, correlation id: %s", properties.correlation_id)
        if properties.correlation_id in self._rpc_corr_id_dict:
            self.logger.info("rpc get response, correlation id: %s", properties.correlation_id)
            self._rpc_corr_id_dict[properties.correlation_id].set_result(body)
        else:
            self.logger.warning("rpc get non exist response. correlation id: %s", properties.correlation_id)
        unused_channel.basic_ack(basic_deliver.delivery_tag)

    def _wait_result(self, corr_id, timeout=None):
        self.logger.info("begin waiting result. %s", corr_id)
        future = self._rpc_corr_id_dict[corr_id]

        def on_timeout():
            if corr_id in self._rpc_corr_id_dict:
                self.logger.error("rpc timeout. corr id : %s", corr_id)
                del self._rpc_corr_id_dict[corr_id]
                future.set_exception(RabbitMQTimeoutError('rpc timeout. corr id: %s' % corr_id))

        if timeout is not None:
            self._io_loop.add_timeout(datetime.timedelta(seconds=timeout), on_timeout)
        return future

    def status_check(self):
        return self._receive_conn.status_ok and self._publish_conn.status_ok
