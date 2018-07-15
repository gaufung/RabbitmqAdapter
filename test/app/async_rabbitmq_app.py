# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import uuid
import tornado
import tornado.web
import tornado.ioloop
from tornado.queues import Queue
from tornado import gen
from async.rabbitmq_client import SyncAMQPProducer, AsyncAMQPConsumer
import logging

queue = Queue(maxsize=1)


def _process(body):
    queue.put("Haha"+body)
    return True


url = '127.0.0.1'
exchange_name = "exchange_name_" + str(uuid.uuid4())


class HitHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self, *args, **kwargs):
        with SyncAMQPProducer(url, exchange_name) as p:
            p.publish("dog.yellow", "another big dog")
        value = yield queue.get()
        self.write(value)


if __name__ == "__main__":
    application = tornado.web.Application([
        (r"/hit", HitHandler)
    ])
    logging.basicConfig(level=logging.INFO)
    application.listen(8888)
    c = AsyncAMQPConsumer(url, exchange_name, _process, "dog.*", io_loop=tornado.ioloop.IOLoop.current())
    c.consume()
    tornado.ioloop.IOLoop.current().start()

