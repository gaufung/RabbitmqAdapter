# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
from tornado.gen import Return, coroutine
from rabbitmq.rabbitmq_rpc import AsyncRabbitMQ
from tornado.ioloop import IOLoop
from concurrent.futures import ThreadPoolExecutor
import logging
import datetime
import random

url = "amqp://dev:aispeech2018@10.12.7.22:5672/"
io_loop = IOLoop.current()
exchange_name = "exchange_" + "unique_2"
queue_name = "queue_for" + "unique_2"
rpc = AsyncRabbitMQ(url, io_loop)
EXECUTOR = ThreadPoolExecutor(max_workers=4)


@coroutine
def _process(channel, method, props, body):
    print("precess: %s" % body)
    channel.basic_ack(delivery_tag=method.delivery_tag)
    return Return(True)


@coroutine
def fib(body):
    n = int(body)
    result = yield EXECUTOR.submit(_fib, *(n,))
    raise Return(str(result))


def _fib(n):
    if n < 2:
        return n
    else:
        return _fib(n-1) + _fib(n-2)


@coroutine
def call_fib(n):
    value = yield rpc.call(exchange_name, "fib.%d" % n, str(n), "fib_callback_now", None)
    if value is not None:
        print("fib(%d)=%s" % (n, value))
    else:
        print("failed")


@coroutine
def init():
    yield [
        rpc.consume(exchange_name, queue_name,"dog.*", _process),
        rpc.consume(exchange_name, queue_name, "cat.*", _process),
        rpc.service(exchange_name, "rpc_fib_4", "fib.*", fib),
    ]



if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    io_loop.spawn_callback(init)
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=3), rpc.publish, exchange_name=exchange_name,
                        routing_key="dog.black", body="A big black dog")
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=3), rpc.publish, exchange_name=exchange_name,
                        routing_key="cat.green", body="A big green cat")
    for i in range(10):
        n = random.randint(10, 20)
        io_loop.add_timeout(datetime.timedelta(days=0, seconds=3), call_fib, n=n)
    io_loop.start()