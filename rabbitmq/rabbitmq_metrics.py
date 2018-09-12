# -*- coding:utf-8 -*-
"""
RabbitMQ metric
"""
import json
import logging
from pika import URLParameters
from pika.compat import url_quote
from tornado import gen
from tornado.web import HTTPError
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.ioloop import IOLoop


class RabbitMQMetrics(object):
    """
    class of RabbitMQ metrics
    """
    def __init__(self, url):
        """
        url of RabbitMQ
        example: amqp://username:password@host:port/<virtual_host>[?query-string]
        :param url: AMQP URL to connect RabbitMQ
        """
        self.user_name, self.password, self.url_root = RabbitMQMetrics._parse_url(url)
        self._logger = logging.getLogger(__name__)

    @property
    def logger(self):
        return self._logger

    @staticmethod
    def _parse_url(url):
        """
        extract fields from AMOP URL
        if using default "/" virtual host, the value should be `%2f`.
        :param url: AMOP URL
        :return: a tuple: (username, password, "http://host:port/api/queues/virtual_host/" )
        """
        url_parameters = URLParameters(url)
        host = url_parameters.host
        port = url_parameters.port + 10000
        virtual_host = url_parameters.virtual_host
        url_root = "http://%s:%s/api/queues/%s/" % (host, port, url_quote(virtual_host, safe=''))
        return url_parameters.credentials.username, url_parameters.credentials.password, url_root

    @gen.coroutine
    def metrics(self, queue_name, timeout=None, io_loop=None):
        """
        metrics of RabbitMQ by given queue name
        :param queue_name: queue name
        :param timeout: request timeout
        :param io_loop: ioloop
        :return: dictionary contains ('name', 'message', 'messages_unacknowledged', 'consumers') fields
        """
        self.logger.info("queue name %s" % (queue_name,))
        try:
            response = yield self._fetch(queue_name, timeout=timeout, io_loop=io_loop)
            if response.code != 200:
                err_msg = "fail to get metrics from RabbitMQ. response code %d" % response.code
                raise Exception(err_msg)
            result = RabbitMQMetrics._format(response)
            self.logger.info("result: %s", json.dumps(result))
            raise gen.Return(result)
        except (gen.Return, HTTPError):
            raise
        except Exception as e:
            self.logger.error("exception %s" % (e,))
            raise

    @gen.coroutine
    def _fetch(self, queue_name, timeout=None, io_loop=None):
        url = self.url_root + url_quote(queue_name)
        if io_loop is None:
            io_loop = IOLoop.current()
        client = AsyncHTTPClient(io_loop)
        request = HTTPRequest(url, request_timeout=timeout, headers={"content-type": "application/json"},
                              auth_username=self.user_name, auth_password=self.password)
        response = yield client.fetch(request)
        self.logger.info("rabbitmq queue metric request. response code: %d, resonse content %s",
                         response.code, response.body)
        raise gen.Return(response)

    @staticmethod
    def _format(response):
        data = json.loads(response.body)
        return {
            'queueName': data.get("name", ""),
            'messagesTotal': data.get('messages', 0),
            'messagesUnack': data.get('messages_unacknowledged', 0),
            'consumers': data.get('consumers', ""),
        }
