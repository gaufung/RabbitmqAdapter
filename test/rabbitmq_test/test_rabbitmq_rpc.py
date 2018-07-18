# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import uuid
from tornado.queues import Queue
from tornado import  gen
from rabbitmq.rabbitmq_rpc import AsyncRabbitMQ
from rabbitmq.rabbitmq_util import make_properties
from tornado.ioloop import IOLoop
from concurrent.futures import ThreadPoolExecutor
import logging
import datetime
import random

url = '127.0.0.1'
exchange_name = "exchange_name_test_rabbitmq_rpc_1"

rpc = AsyncRabbitMQ(url, IOLoop.current())

dog_queue = Queue(maxsize=1)
cat_queue = Queue(maxsize=1)

EXECUTOR = ThreadPoolExecutor(max_workers=4)


@gen.coroutine
def _dog_process(channel, method, props, body):
    dog_queue.put(body)
    rpc._channel.basic_ack(delivery_tag=method.delivery_tag)
    raise gen.Return(True)


@gen.coroutine
def _cat_process(channel, method, props, body):
    cat_queue.put(body)
    rpc._channel.basic_ack(delivery_tag=method.delivery_tag)
    raise gen.Return(True)


@gen.coroutine
def _reply_process(channel, method, props, body):
    print("receive reply to: %s" % (props.reply_to, ))
    print("receive corr id: %s " % props.correlation_id)
    print("receive body: %s" % body)
    response = body + "+" + body
    rpc.publish(exchange_name, routing_key=props.reply_to,
                properties=make_properties(correlation_id=props.correlation_id),
                message=response)
    rpc._channel.basic_ack(delivery_tag=method.delivery_tag)
    raise gen.Return(True)


@gen.coroutine
def handler_color(body):
    # n = random.randint(100, 500)
    # sleep_time = n * 1.0 / 1000
    # print("%s sleep %s" % (body, str(sleep_time)))
    #yield gen.sleep(sleep_time)
    raise gen.Return(body)


@gen.coroutine
def fib(body):
    n = int(body)
    result = yield EXECUTOR.submit(_fib, *(n,))
    raise gen.Return(str(result))


def _fib(n):
    if n < 2:
        return n
    else:
        return _fib(n-1) + _fib(n-2)


@gen.coroutine
def output_dog():
    value = yield dog_queue.get()
    print(value)


@gen.coroutine
def output_cat():
    value = yield cat_queue.get()
    print(value)


@gen.coroutine
def call():
    colors = ["red", "orange", "black", "green", "gray", "purple"]
    n = random.randint(0, len(colors) - 1)
    color = colors[n]
    value = yield rpc.call(exchange_name, "color.%s" % color, color, "color_callback", None)
    if value is not None:
        if value == color:
            print("%s = %s" % (value, color,))
        else:
            print("%s != %s" % (value, color,))


@gen.coroutine
def call_fib(n):
    value = yield rpc.call(exchange_name, "fib.%d" % n, str(n), "fib_callback", None)
    if value is not None:
        print("fib(%d)=%s" % (n, value))
    else:
        print("failed")


@gen.coroutine
def publish_reply_to():
    corr_id = "corr_id_" + str(uuid.uuid4())
    print("send to corr id: %s" % corr_id)
    print("send to reply to: %s" % "publish_reply_to_2")
    print("send body: %s" % "single")
    pros = make_properties(correlation_id=corr_id, reply_to="publish_reply_to_2")
    rpc.publish(exchange_name, "withreply.publish", "single", pros)
    rpc.consume(exchange_name, "publish_reply_to_2", "publish_reply_to_2", _publish_handler)


@gen.coroutine
def _publish_handler(channel, method, props, body):
    print("receive after publishing body: %s" % body)
    rpc._channel.basic_ack(delivery_tag=method.delivery_tag)
    raise gen.Return(True)

@gen.coroutine
def init():
    yield [
        rpc.consume(exchange_name, "queue_routing_dog_2", "dog.*", _dog_process),
        rpc.consume(exchange_name, "queue_routing_cat_2", "cat.*", _cat_process),
        rpc.consume(exchange_name, "queue_with_reply_to_2", "withreply.*", _reply_process),
        rpc.service(exchange_name, "rpc_call_color_2", "color.*", handler_color),
        rpc.service(exchange_name, "rpc_fib_2", "fib.*", fib),
        rpc.service(exchange_name, "rpc_fib_2", "fib.*", fib),
        rpc.service(exchange_name, "rpc_fib_2", "fib.*", fib),
        rpc.service(exchange_name, "rpc_fib_2", "fib.*", fib),
        rpc.service(exchange_name, "rpc_fib_2", "fib.*", fib),
        ]


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    io_loop = IOLoop.current()
    io_loop.spawn_callback(init)
    # part I
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=1), rpc.publish, exchange_name=exchange_name, routing_key="dog.yellow", message="A big yellow dog")
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=1), rpc.publish, exchange_name=exchange_name, routing_key="cat.black", message="A big black cat")
    # part II
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=1), publish_reply_to)
    # part III
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=1), call)
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=1), call)
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=1), call)
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=1), call)
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=1), call)
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=2), call_fib, n=30)
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=2), call_fib, n=25)
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=2), call_fib, n=20)
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=2), call_fib, n=15)
    io_loop.add_timeout(datetime.timedelta(days=0, seconds=2), call_fib, n=10)
    io_loop.spawn_callback(output_dog)
    io_loop.spawn_callback(output_cat)
    io_loop.start()

