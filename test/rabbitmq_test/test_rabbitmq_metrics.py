# -*- encoding:utf-8 -*-
import unittest

import pika
from tornado.testing import AsyncTestCase, gen_test

from common.rabbitmq import RabbitMQMetrics


class TestRabbitMQMetrics(AsyncTestCase):
    RABBITMQ_SERVER = 'amqp://dev:aispeech2018@10.12.6.35:5672/'

    def setUp(self):
        self._queue = "test_queue_metric"
        connection = pika.BlockingConnection(pika.URLParameters(self.RABBITMQ_SERVER))
        channel = connection.channel()
        channel.queue_declare(queue=self._queue)
        connection.close()
        super(TestRabbitMQMetrics, self).setUp()

    def tearDown(self):
        connection = pika.BlockingConnection(pika.URLParameters(self.RABBITMQ_SERVER))
        channel = connection.channel()
        channel.queue_delete(queue=self._queue)
        connection.close()


    def test_init(self):
        rabbitmq_metrics = RabbitMQMetrics(self.RABBITMQ_SERVER)
        self.assertEqual(rabbitmq_metrics.user_name, "dev")
        self.assertEqual(rabbitmq_metrics.password, "aispeech2018")
        self.assertEqual(rabbitmq_metrics.url_root, "http://10.12.6.35:15672/api/queues/%2F/")

    @gen_test
    def test_fetch(self):
        rabbitmq_metrics = RabbitMQMetrics(self.RABBITMQ_SERVER)
        response = yield rabbitmq_metrics._fetch(self._queue, None, self.io_loop)
        self.assertIsNotNone(response)
        data = RabbitMQMetrics._format(response)
        self.assertEqual(len(data), 4)
        # fetch non existing queue
        with self.assertRaises(Exception):
            yield rabbitmq_metrics._fetch("non_existing_queue", self.io_loop)

    @gen_test
    def test_metrics(self):
        rabbitmq_metrics = RabbitMQMetrics(self.RABBITMQ_SERVER)
        result = yield rabbitmq_metrics.metrics(self._queue, None, self.io_loop)
        self.assertIsNotNone(result)
        self.assertTrue(isinstance(result, dict))
        with self.assertRaises(Exception):
            yield rabbitmq_metrics.metrics("non_existing_queue", None, self.io_loop)


if __name__ == "__main__":
    unittest.main()