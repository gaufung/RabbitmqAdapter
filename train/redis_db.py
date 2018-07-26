# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import redis
from train.train_status import Db


class RedisDb(Db):
    def __init__(self, redis_host, redis_port, redis_db, redis_password):
        self._redis = redis.StrictRedis(redis_host, redis_port, redis_db, redis_password)

    def get(self, key):
        """
        get value from redis by given key
        :param key: given key
        :return:
        """
        self._redis.hgetall(key)

    def set(self, key, value):
        if isinstance(value, dict):
            self._redis.hmset(key, value)
        else:
            self._redis.set(key, value)
