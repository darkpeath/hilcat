# -*- coding: utf-8 -*-

import pytest

# use fakeredis to test without a real redis server
fakeredis = pytest.importorskip("fakeredis")
from hilcat import RedisCache

def test_redis_cache():
    cache = RedisCache(client=fakeredis.FakeRedis())
    assert not cache.exists('k')
    assert cache.fetch('k', default='d') == 'd'
    cache.set('k', 'v')
    assert cache.exists('k')
    assert cache.fetch('k') == b'v'
    assert set(cache.keys()) == {b'k'}
    cache.pop('k')
    assert not cache.exists('k')

def test_redis_get_with_func():
    cache = RedisCache(client=fakeredis.FakeRedis())
    assert cache.get('k', lambda: 'computed') == 'computed'
    # second call hits the cache (value returned as bytes by redis)
    assert cache.get('k', lambda: 'other') == b'computed'
