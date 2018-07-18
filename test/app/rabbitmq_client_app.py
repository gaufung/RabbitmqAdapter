# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import uuid
import logging
import tornado.web
import tornado.ioloop
from tornado.queues import Queue
from tornado import gen
from rabbitmq.rabbitmq_client import  AsyncAMQPConsumer, SyncAMQPProducer

cat_queue = Queue(maxsize=1)


@gen.coroutine
def _cat_process(channel, method, header, body):
    cat_queue.put(body)
    raise gen.Return(True)


url = "127.0.0.1"
exchange_name = "exchange_name_" + str(uuid.uuid4())

class CatHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self):
        with SyncAMQPProducer(url,exchange_name) as p:
            p.publish("cat.yellow", "A big yellow cat")
        value = yield cat_queue.get()
        self.write(value)


if __name__ == "__main__":
    application = tornado.web.Application([
        (r"/cat", CatHandler)
    ])
    logging.basicConfig(level=logging.INFO)
    application.listen(8989)
    io_loop = tornado.ioloop.IOLoop.current()
    c = AsyncAMQPConsumer(url, exchange_name, "cat.*", _cat_process, io_loop=io_loop)
    c.consume()
    io_loop.start()
