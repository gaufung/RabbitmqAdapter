# -*- encoding:utf-8 -*-
from tornado.ioloop import IOLoop
from rabbitmq.rabbitmq_adapter import TornadoAdapter
from tornado.gen import coroutine, sleep


RABBITMQ_SERVER = 'amqp://dev:aispeech2018@10.12.6.35:5672/'


@coroutine
def process(msg):
    print(msg)


@coroutine
def receive():
    rabbtimq_adapter = TornadoAdapter(RABBITMQ_SERVER, io_loop=IOLoop.current())
    yield rabbtimq_adapter.receive("asr_train_exchange",
                                   "routing_app_test.*",
                                   "Hello", process)


@coroutine
def send():
    rabbtimq_adapter = TornadoAdapter(RABBITMQ_SERVER, io_loop=IOLoop.current())
    yield rabbtimq_adapter.publish("asr_train_exchange",
                                   "routing_app1_test.hello",
                                   "hello world", mandatory=True)


if __name__ == "__main__":
    IOLoop.current().spawn_callback(send)
    IOLoop.current().start()
