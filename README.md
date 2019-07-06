# RabbitMQ's Tornado Advance APIs


## 1 Background

[`pika`](https://pika.readthedocs.io/en/stable/) is a RabbitMQ client in Python and it also supports `tornado`'s IOLoop connection. However, the APIs for asychronous method are very poor. So I extends the APIs for better asychronous support.

## 2 APIs

### 2.1 Publish

```python
@gen.coroutine
def publish(self, exchange, routing_key, body, properties=None,  mandatory=True, return_callback=None, close_callback=None):
    pass
```

### 2.2 Consumer

```python
@gen.coroutine
def receive(self, exchange, routing_key, queue_name, handler, no_ack=False, prefetch_count=0,return_callback=None, close_callback=None, cancel_callback=None):
    pass
```

### 2.3 RPC

```python
@gen.coroutine
def rpc(self, exchange, routing_key, body, timeout, ttl, close_callback=None, return_callback=None, cancel_callback=None):
    pass
```

## 3 Conclusion

This library has widly used in production environment and is proved that it is bug free.
