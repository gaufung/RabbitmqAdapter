# -*- coding:utf-8 -*-
from __future__ import unicode_literals
import logging
import time
import functools
import tornado.ioloop
from async.async_amqp import AsyncAMQPProducer, AsyncAMQPConsumer
from async.async_amqp_rpc import AsyncAMQPClient, AsyncAMQPServer


logging.basicConfig(level=logging.INFO)


def callback(message):
    print("callback invoke: %s" % (message,))
    return True


def fib(n, body):
    time.sleep(n)
    return "response: " + body


url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
io_loop = tornado.ioloop.IOLoop.current()
_exchange_name = "direct_logs"
_publish_routing_key1 = "topic1"
_publish_routing_key2 = "topic2"
_receive_routing_keys = ["topic1", "topic3"]
_queue_name = "queue1"

_rpc_queue = "queue_for_rpc"
_server_routing_keys = ["v1.0", "v1.1"]
_client_routing = "v1.0"


p2 = AsyncAMQPProducer(url, "message_topic2", _exchange_name, _publish_routing_key2, io_loop=io_loop)
p2.publish()
c = AsyncAMQPConsumer(url, callback, _exchange_name, _receive_routing_keys, _queue_name, io_loop=io_loop)
c.consume()

server = AsyncAMQPServer(url, functools.partial(fib, 1), routing_keys=_server_routing_keys,
                         queue_name=_rpc_queue, io_loop=io_loop)
server.service()
client = AsyncAMQPClient(url, routing_key=_client_routing, queue_name=_rpc_queue, io_loop=io_loop, timeout=4)
client.call("Hello world!")
p1 = AsyncAMQPProducer(url, "message_topic1", _exchange_name, _publish_routing_key1, io_loop=io_loop)
p1.publish()
tornado.ioloop.IOLoop().current().start()


