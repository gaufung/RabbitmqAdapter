# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import uuid
import tornado.web
import tornado.ioloop
import random
from tornado.queues import Queue
from tornado import gen
from rabbitmq.rabbitmq_rpc import AsyncRabbitMQ
import logging

dog_queue = Queue(maxsize=1)
cat_queue = Queue(maxsize=1)


@gen.coroutine
def _dog_process(channel, method, header, body):
    dog_queue.put(body)
    raise gen.Return(True)


@gen.coroutine
def _cat_process(channel, method, header, body):
    cat_queue.put(body)
    raise gen.Return(True)


@gen.coroutine
def fib(n):
    sleep_time = float(n)
    yield gen.sleep(sleep_time)
    raise gen.Return("sleep %s second" % (n))


def _fib(n):
    if n < 2:
        return n
    else:
        return _fib(n-1) + _fib(n-2)


@gen.coroutine
def timeout(body):
    yield gen.sleep(4)
    raise gen.Return("got result")

@gen.coroutine
def handler_color(body):
    n = random.randint(100, 500)
    sleep_time = n * 1.0 / 1000
    yield gen.sleep(sleep_time)
    raise gen.Return(body)


url = '127.0.0.1'
exchange_name = "exchange_name_" + str(uuid.uuid4())
rpc = AsyncRabbitMQ(url, tornado.ioloop.IOLoop.current())

colors = ["red", "orange", "black", "green", "gray", "purple"]


class DogHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self, *args, **kwargs):
        rpc.publish(exchange_name, "dog.yellow", "A big yellow dog")
        value = yield dog_queue.get()
        self.write(value)


class CatHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self, *args, **kwargs):
        rpc.publish(exchange_name, "cat.black", "A big black cat")
        value = yield cat_queue.get()
        self.write(value)


class RpcHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self, *args, **kwargs):
        n = random.randint(1000, 1500)
        sleep_time = n * 1.0 / 1000
        value = yield rpc.call(exchange_name, "fib.%d" % n, str(sleep_time))
        if value is not None:
            self.write(value+"\n")
        else:
            self.write("time out")


class RpcTimeoutHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self, *args, **kwargs):
        value = yield rpc.call(exchange_name, "timeout.long", "timeout", None, 3)
        if value is not None:
            self.write(value+"\n")
        else:
            self.write("time out")


class ColorDogHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self, *args, **kwargs):
        n = random.randint(0, len(colors)-1)
        color = colors[n]
        value = yield rpc.call(exchange_name, "color.%s" % color, color, "color_callback", None)
        if value is not None:
            if value == color:
                self.write(value)
            else:
                raise Exception("%s != %s" % (value, color))
        else:
            self.write(timeout)


class RpcGivenCallbackHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self, *args, **kwargs):
        n = random.randint(1000, 1500)
        sleep_time = n * 1.0 / 1000
        value = yield rpc.call(exchange_name, "fib.%d" % n, str(sleep_time), callback_queue="rpc_callback")
        if value is not None:
            self.write(value+"\n")
        else:
            self.write("timeout")


@gen.coroutine
def init():
    yield rpc.consume(exchange_name, "queue_routing_dog", "dog.*", _dog_process)
    yield rpc.consume(exchange_name, "queue_routing_cat", "cat.*", _cat_process)
    yield rpc.service(exchange_name, "rpc_call", "fib.*", fib)
    yield rpc.service(exchange_name, "rpc_call_timeout", "timeout.*", timeout)
    yield rpc.service(exchange_name, "rpc_call_color", "color.*", handler_color)


application = tornado.web.Application([
        (r"/dog", DogHandler),
        (r"/cat", CatHandler),
        (r"/fib", RpcHandler),
        (r'/fibcallback', RpcGivenCallbackHandler),
        (r"/timeout", RpcTimeoutHandler),
        (r"/color", ColorDogHandler),
    ])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    application.listen(8888)
    io_loop = tornado.ioloop.IOLoop.current()
    io_loop.spawn_callback(init)
    tornado.ioloop.IOLoop.current().start()

