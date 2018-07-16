# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import unittest
from tornado.testing import AsyncTestCase, gen_test
from rabbitmq.rabbitmq_metrics import RabbitMQMetrics


class TestRabbitMQMetrics(AsyncTestCase):
    def test_init(self):
        url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        rabbitmq_metrics = RabbitMQMetrics(url)
        self.assertEqual(rabbitmq_metrics.user_name, "dev")
        self.assertEqual(rabbitmq_metrics.password, "aispeech2018")
        self.assertEqual(rabbitmq_metrics.url_root, "http://10.12.7.22:15672/api/queues/%2F/")

    @gen_test
    def test_fetch(self):
        url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        rabbitmq_metrics = RabbitMQMetrics(url)
        response = yield rabbitmq_metrics._fetch("lzl_test", self.io_loop)
        self.assertIsNotNone(response)
        data = RabbitMQMetrics._convert(response)
        self.assertEqual(len(data), 4)
        # fetch non existing queue
        with self.assertRaises(Exception):
            yield rabbitmq_metrics._fetch("non_existing_queue", self.io_loop)

    @gen_test
    def test_metrics(self):
        url = 'amqp://dev:aispeech2018@10.12.7.22:5672/'
        rabbitmq_metrics = RabbitMQMetrics(url)
        result = yield rabbitmq_metrics.metrics("lzl_test", self.io_loop)
        self.assertIsNotNone(result)
        self.assertTrue(isinstance(result, dict))
        with self.assertRaises(Exception):
            yield rabbitmq_metrics.metrics("non_existing_queue", self.io_loop)


if __name__ == "__main__":
    unittest.main()