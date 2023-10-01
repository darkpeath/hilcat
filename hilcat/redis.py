# -*- coding: utf-8 -*-

from typing import Any, Iterable, Union
import redis
from .core import Cache

_REDIS_KEY_TYPE = Union[str, bytes]

class RedisCache(Cache):
    """
    Use redis as backend.
    Ignore scope for all cache methods.
    """

    def __init__(self, client: redis.Redis = None, url: str = None, host: str = None, port: int = None, db=0):
        if client is not None:
            self.client = client
        elif url is not None:
            self.client = redis.from_url(url)
        elif host is not None:
            self.client = redis.Redis(host=host, port=port, db=db)
        else:
            raise ValueError("One of client, url or host should given.")

    def exists(self, key: _REDIS_KEY_TYPE, scope: Any = None, **kwargs) -> bool:
        return self.client.exists(key) > 0

    def fetch(self, key: _REDIS_KEY_TYPE, default: Any = None, scope: Any = None, **kwargs) -> Any:
        value = self.client.get(key)
        if value is None:
            return default
        return value

    def set(self, key: _REDIS_KEY_TYPE, value: Any, scope: Any = None, **kwargs) -> Any:
        self.client.set(key, value, **kwargs)

    def pop(self, key: _REDIS_KEY_TYPE, scope: Any = None, **kwargs) -> Any:
        # delete a key
        self.client.delete(key)

    def keys(self, scope: Any = None) -> Iterable[_REDIS_KEY_TYPE]:
        return self.client.keys()

