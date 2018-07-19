# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import uuid
from rabbitmq_rpc_app import AsyncRabbitMQ
from tornado.gen import  coroutine, Return
from tornado.queues import Queue
from tornado.ioloop import IOLoop
import logging

url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
exchange_name = "exchange_name_" + "test"
queue_name = "queue_name_" + "test"
queue = Queue(maxsize=1)


@coroutine
def _process(channel, method, header, body):
    print(body)
    queue.put(body)
    raise Return(True)


@coroutine
def initialize_publish_consume():
    # rpc = AsyncRabbitMQ(url, IOLoop.current())
    # yield rpc._initialize(exchange_name, False, queue_name, "connection.*")
    # rpc.close()
    # rpc = AsyncRabbitMQ(url, IOLoop.current())
    # yield rpc.publish(exchange_name, "connection.publish", "message2")
    # rpc.close()
    # print("close 2")
    rpc = AsyncRabbitMQ(url, IOLoop.current())
    yield rpc.consume(exchange_name, queue_name, "connection.*", _process)
    # yield rpc.publish(exchange_name, "connection.publish", "message")
    # value = yield queue.get()
    # if value != "message":
    #     print("got message")
    # else:
    #     print("got invalid message")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    io_loop = IOLoop.current()
    io_loop.spawn_callback(initialize_publish_consume)
    io_loop.start()
