# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
from pika import BasicProperties

"""
tools for rabbitmq client
"""


def make_properties(correlation_id=None, reply_to=None, delivery_mode=None):
    return BasicProperties(
        correlation_id=correlation_id,
        reply_to=reply_to,
        delivery_mode=delivery_mode)

