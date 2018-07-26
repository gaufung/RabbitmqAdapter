# -*- coding:utf-8 -*-
"""
RabbitMQ metric
"""
from __future__ import unicode_literals
import json
import logging
from pika import URLParameters
from pika.compat import url_quote
from tornado import gen
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
        log = self._get_log("__init__")
        log.info("url: %s" % (url,))
        self.user_name, self.password, self.url_root = RabbitMQMetrics._parse_url(url)

    @classmethod
    def _get_log(cls, *names):
        return logging.getLogger(".".join((cls.__module__, cls.__name__) + names))

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
        log = self._get_log("metrics")
        log.info("queue name %s" % (queue_name,))
        try:
            response = yield self._fetch(queue_name,timeout=timeout, io_loop=io_loop)
            log.info("response %s" % (response,))
            if response.code != 200:
                err_msg = "fail to get metrics from RabbitMQ. response code %d" % response.code
                raise Exception(err_msg)
            result = RabbitMQMetrics._convert(response)
            log.info("result: %s" % (result,))
            raise gen.Return(result)
        except gen.Return:
            raise
        except Exception as e:
            log.error("exception %s" % (e,))
            raise

    def _fetch(self, queue_name, timeout=None, io_loop=None):
        url = self.url_root + url_quote(queue_name)
        if io_loop is None:
            io_loop = IOLoop.current()
        client = AsyncHTTPClient(io_loop)
        request = HTTPRequest(url, request_timeout=timeout, headers={"content-type": "application/json"},
                              auth_username=self.user_name, auth_password=self.password)
        return client.fetch(request)

    @staticmethod
    def _convert(response):
        data = json.loads(response.body)
        return {
            'queueName': data['name'],
            'messagesTotal': data['messages'],
            'messagesUnack': data['messages_unacknowledged'],
            'consumers': data['consumers']
        }
