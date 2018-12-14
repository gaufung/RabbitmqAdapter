# -*-encoding:utf-8 -*-
"""
distrubted lock with Redis
Redis doc: http://redis.io/topics/distlock
"""
from __future__ import division
from datetime import datetime
import random
import time
import uuid

import redis

# Reference: http://redis.io/topics/distlock
_RELEASE_LUA_SCRIPT = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
"""

_DEFAULT_RETRY_TIMES = 3
_DEFAULT_RETRY_DELAY = 200
_DEFAULT_TTL = 1000 * 60 * 60 * 1
_CLOCK_DRIFT_FACTOR = 0.01


class RedLockError(Exception):
    pass


class RedLock(object):
    """
    A distrubuted lock implementation based on Redis
    It share a simliar API with the `threading.Lock` class int PSL
    """

    def __init__(self, resource, connections=None, retry_times=_DEFAULT_RETRY_TIMES,
                 retry_delay=_DEFAULT_RETRY_DELAY, ttl=_DEFAULT_TTL):
        """
        RedLock based on Redis. 
        :param resource: name for exclusive resource. It must be unique.
        :param connections: connections for redis single instance or cluster.
        :type connections, iterable:
            each `connection` in `connections` could be:
            - redis.StrictRedis
            - {"url":"redis://localhost:6379/0"}
            - {"host":"locahost", "port": 6379, "db": 0, "password":'password'}
        :retry_times: times for re-try to get lock, default: 3
        :retry_delay: time to sleep for next try to get lock, default: 200 millisecond
        :ttl: ttl value for lock in redis. It is used to avoid dead lock in case client dead. (unit: millisecond)
        Usages:
            client = redis.StrictRedis(host="127.0.0.1", port=2379, db=2, password="mypassword")
            resource = "cirtial area"
            with RedLock(resource, [client]):
                # enter critial code 
            # OR
            lock = RedLock(resource, [client])
            acquired = lock.acquire()
            if acquired:
                # enter critial code
                lock.release
        """
        self._resource = resource
        self._retry_times = retry_times
        self._retry_delay = retry_delay
        self._ttl = ttl
        self._redis_nodes = []
        if connections is None:
            connections = [{
                'host': "localhost",
                'port': 6379,
                'db': 0,
            }]
        for conn in connections:
            if isinstance(conn, redis.StrictRedis):
                node = conn
            elif 'url' in conn:
                url = conn.pop('url')
                node = redis.StrictRedis.from_url(url, **conn)
            else:
                node = redis.StrictRedis(**conn)
            node._release_script = node.register_script(_RELEASE_LUA_SCRIPT)
            self._redis_nodes.append(node)
        self._quorum = len(self._redis_nodes) // 2 + 1

    def __enter__(self):
        acquire, validity = self._acquire()
        if not acquire:
            raise RedLockError("failed to acquire lock")
        return validity

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    @staticmethod
    def _total_ms(delta):
        delta_seconds = delta.seconds + delta.days * 24 * 60 * 60
        return (delta.microseconds + delta_seconds * 10**6) / 10**3

    def locked(self):
        """
        whether resource is locked 
        """
        for node in self._redis_nodes:
            if node.get(self._resource):
                return True
        return False

    def _acquire_node(self, node):
        try:
            return node.set(self._resource, self._lock_key, nx=True, px=self._ttl)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            return False

    def _release_node(self, node):
        try:
            node._release_script(keys=[self._resource], args=[self._lock_key])
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            pass

    def acquire(self):
        acquired, validity = self._acquire()
        return acquired

    def _acquire(self):
        self._lock_key = uuid.uuid4().hex

        for retry in range(self._retry_times+1):
            acquire_node_count = 0
            start_time = datetime.utcnow()
            for node in self._redis_nodes:
                if self._acquire_node(node):
                    acquire_node_count += 1
            end_time = datetime.utcnow()
            elapsed_milliseconds = self._total_ms(end_time - start_time)

            drift = (self._ttl * _CLOCK_DRIFT_FACTOR) + 2

            validity = self._ttl - (elapsed_milliseconds + drift)

            if acquire_node_count >= self._quorum and validity > 0:
                return True, validity
            else:
                for node in self._redis_nodes:
                    self._release_node(node)
                time.sleep(random.randint(0, self._retry_delay) / 1000)
        return False, 0

    def release(self):
        for node in self._redis_nodes:
            self._release_node(node)
