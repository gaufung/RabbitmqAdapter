# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import uuid
import tornado
import tornado.web
import tornado.ioloop
from tornado.queues import Queue
from tornado import gen
from async.rabbitmq_client import SyncAMQPProducer, AsyncAMQPConsumer
from async.rabbitmq_rpc import AsyncRabbitMQ
import logging

dog_queue = Queue(maxsize=1)
cat_queue = Queue(maxsize=1)


@gen.coroutine
def _dog_process(body):
    dog_queue.put(body)
    raise gen.Return(True)


@gen.coroutine
def _cat_process(body):
    cat_queue.put(body)
    raise gen.Return(True)


url = '127.0.0.1'
exchange_name = "exchange_name_" + str(uuid.uuid4())
rpc = AsyncRabbitMQ(url, tornado.ioloop.IOLoop.current())


class HitHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self, *args, **kwargs):
        # with SyncAMQPProducer(url, exchange_name) as p:
        #     p.publish("dog.yellow", "another big dog")
        rpc.publish(exchange_name, "dog.yellow", "A big yellow dog")
        value = yield dog_queue.get()
        self.write(value)


class AnotherHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self, *args, **kwargs):
        rpc.publish(exchange_name, "cat.black", "A big black cat")
        value = yield cat_queue.get()
        self.write(value)

if __name__ == "__main__":
    application = tornado.web.Application([
        (r"/dog", HitHandler),
        (r"/cat", AnotherHandler),
    ])
    logging.basicConfig(level=logging.INFO)
    application.listen(8888)
    rpc.consume(exchange_name, "queue_routing_dog", "dog.*", _dog_process)
    rpc.consume(exchange_name, "quque_routing_cat", "cat.*", _cat_process)
    tornado.ioloop.IOLoop.current().start()

