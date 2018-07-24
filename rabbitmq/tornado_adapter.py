
import functools
import logging
import uuid
from pika import URLParameters
from pika import TornadoConnection
from pika import BasicProperties
from tornado.ioloop import Future
from tornado import gen
from tornado.queues import Queue


class TornadoaDdapter(object):
    def __init__(self, rabbitmq_url):
        self._rabbitmq_url = rabbitmq_url
        self._logger = logging.getLogger(__name__)
        self._publish_connection = None
        self._receive_connection = None
        self._rpc_exchange_dict = dict()
        self._rpc_corr_id_dict = dict()

    @gen.coroutine
    def init(self):
        self._publish_connection = yield self._create_connection()
        self._receive_connection = yield self._create_connection()
        raise gen.Return(True)

    @property
    def logger(self):
        return self._logger

    def _create_connection(self):
        future = Future()

        def open_callback(unused_connection):
            future.set_result(unused_connection)

        def open_error_callback(connection, exception):
            future.set_exception(exception)

        def close_callback(connection, reply_code, reply_text):
            pass

        TornadoConnection(URLParameters(self._rabbitmq_url),
                          on_open_callback=open_callback,
                          on_open_error_callback=open_error_callback,
                          on_close_callback=close_callback)
        return future

    def _create_channel(self, connection):
        future = Future()

        def open_callback(channel):
            future.set_result(channel)

        connection.channel(on_open_callback=open_callback)

        return future

    def _exchange_declare(self, channel, exchange=None, exchange_type='direct', **kwargs):
        future = Future()

        def callback(unframe):
            future.set_result(unframe)
            pass

        channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, **kwargs)

        return future

    def _queue_declare(self, channel, queue='', **kwargs):
        future = Future()

        def callback(method_frame):
            future.set_result(method_frame.queue)

        channel.queue_declare(callback=callback, queue=queue, **kwargs)

        return future

    def _queue_bind(self, channel, queue, exchange, routing_key=None, **kwargs):
        future = Future()

        def callback(unframe):
            future.set_result(unframe)

        channel.queue_bind(callback, queue=queue, exchange=exchange, routing_key=routing_key, **kwargs)

        return future

    @gen.coroutine
    def publish(self, exchange, routing_key, body, properties=None):
        channel = yield self._create_channel(self._publish_connection)
        channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=properties)
        pass

    @gen.coroutine
    def receive(self, exchange, routing_key, queue_name, handler, no_ack=False, prefetch_count=0):
        channel = yield self._create_channel(self._publish_connection)
        channel.basic_consume(functools.partial(self.on_message, exchange_name=exchange, handler=handler)
                              , queue=queue_name, no_ack=no_ack)
        pass

    def on_message(self, unused_channel, basic_deliver, properties, body, exchange, handler=None):
        self._ioloop.spawn_callback(self.process_message, unused_channel, basic_deliver, properties, body,
                                    exchange, handler)

    @gen.coroutine
    def process_message(self, unused_channel, basic_deliver, properties, body, exchange, handler=None):
        try:
            result = yield handler(body)
            if property is not None:
                self.publish(exchange=exchange,
                             routing_key=properties.reply_to,
                             properties=BasicProperties(correlation_id=properties.correlation_id),
                             body=str(result))
            unused_channel.basic_ack(basic_deliver.delivery_tag)
        except Exception, e:
            unused_channel.basic_ack(basic_deliver.delivery_tag)
            import traceback
            self.logger.error(traceback.format_exc())

    def rpc_process(self, unused_channel, basic_deliver, properties, body):
        if properties.correlation_id in self._rpc_corr_id_dict:
            self._rpc_corr_id_dict[properties.correlation_id].set_result(body)

    @gen.coroutine
    def rpc_init(self, exchange):
        rpc_channel = yield self._create_channel(self._receive_connection)
        yield self._exchange_declare(rpc_channel, exchange)
        callback_queue = yield self._queue_declare(rpc_channel, auto_delete=True)
        yield self._queue_bind(rpc_channel, exchange=exchange, queue=callback_queue)
        rpc_channel.basic_consume(self.rpc_process, queue=callback_queue)
        raise gen.Return(callback_queue)

    @gen.coroutine
    def rpc(self, exchange, routing_key, body, timeout=None):
        if exchange not in self._rpc_exchange_dict:
            self._rpc_exchange_dict[exchange] = Queue(maxsize=1)
            callback_queue = yield self.rpc_init(exchange)
            yield self._rpc_exchange_dict[exchange].put(callback_queue)
        callback_queue = yield self._rpc_corr_id_dict[exchange].get()
        yield self._rpc_corr_id_dict[exchange].put(callback_queue)

        result = yield self._rpc(callback_queue, exchange, routing_key, body, timeout)

        raise gen.Return(result)

    def _rpc(self, callback_queue, exchange, routing_key, body, timeout=None):
        future = Future()
        corr_id = str(uuid.uuid1())
        self._rpc_corr_id_dict[corr_id] = future
        self.publish(exchange, routing_key, body,
                     properties=BasicProperties(correlation_id=corr_id,
                                                reply_to=callback_queue))

        def on_timeout():
            del self._rpc_corr_id_dict[corr_id]
            future.set_exception(Exception('timeout'))

        if timeout is not None:
            self._ioloop.add_timeout(float(timeout), on_timeout)

        return future