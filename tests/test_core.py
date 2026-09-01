# -*- coding: utf-8 -*-

import pytest
from hilcat import (
    Cache, NoOpCache, MemoryCache,
    SimpleTextFileCache, SimpleJsonFileCache,
    MemoryMiddleCache, CacheAgent,
    register_backend,
)

def test_from_uri_unknown_scheme():
    with pytest.raises(ValueError, match="Unsupported backend"):
        Cache.from_uri("unknown://localhost")

def test_from_uri_no_scheme():
    with pytest.raises(ValueError, match="scheme not given"):
        Cache.from_uri("no-scheme-at-all")

def test_noop_cache():
    cache = NoOpCache()
    assert cache.fetch('k', default=42) == 42
    assert not cache.exists('k')
    # value is always recalculated
    assert cache.get('k', lambda: 1) == 1
    assert cache.get('k', lambda: 2) == 2

def test_memory_cache():
    cache = MemoryCache()
    assert not cache.exists('k', scope='s')
    cache.set('k', 1, scope='s')
    assert cache.exists('k', scope='s')
    assert cache.fetch('k', scope='s') == 1
    assert cache.fetch('missing', default=7, scope='s') == 7
    assert cache.update('k', 2, scope='s') == 1     # return old value
    assert list(cache.scopes()) == ['s']
    assert list(cache.keys(scope='s')) == ['k']
    assert cache.pop('k', scope='s') == 2
    assert not cache.exists('k', scope='s')

def test_decorator_with_defaults_and_kwargs():
    cache = MemoryCache()
    calls = []

    @cache(scope='f')
    def f(x, y=2, **kw):
        calls.append((x, y, kw))
        return x + y + sum(kw.values())

    # f(1) and f(1, 2) should hit the same key since y defaults to 2
    assert f(1) == 3
    assert f(1, 2) == 3
    assert len(calls) == 1

    assert f(1, 3) == 4
    assert f(1, 3, z=10) == 14
    assert len(calls) == 3

def test_get_with_func_args():
    cache = MemoryCache()
    assert cache.get('k', lambda x, y: x + y, func_args=[1], func_kwargs={'y': 2}) == 3
    # value is cached, func not called again
    assert cache.get('k', lambda x, y: 0, func_args=[0], func_kwargs={'y': 0}) == 3

def test_context_manager():
    with MemoryCache() as cache:
        cache.set('k', 1)
        assert cache['k'] == 1

def test_cache_agent():
    backend = MemoryCache()
    extra = MemoryCache()
    agent = CacheAgent(backend, extra)
    agent.set('k', 1)
    # write goes to all backends
    assert backend.fetch('k') == 1
    assert extra.fetch('k') == 1
    # read from the first backend
    assert agent.fetch('k') == 1
    assert agent.exists('k')
    agent.pop('k')
    assert not backend.exists('k')
    assert not extra.exists('k')

def test_memory_middle_cache(tmp_path):
    storage = SimpleJsonFileCache(root_dir=str(tmp_path))
    cache = MemoryMiddleCache(storage)
    cache.set('k1', 'v1', scope='s')
    cache.set('k2', 'v2', scope='s')
    cache.backup()
    # a new cache can load the persisted data
    cache2 = MemoryMiddleCache(SimpleJsonFileCache(root_dir=str(tmp_path)))
    cache2.load()
    assert cache2.fetch('k1', scope='s') == 'v1'
    assert set(cache2.keys(scope='s')) == {'k1', 'k2'}

def test_simple_json_file_cache(tmp_path):
    cache = SimpleJsonFileCache(root_dir=str(tmp_path))
    cache.set('k', {'a': 1, 'b': [1, 2]})
    assert cache.fetch('k') == {'a': 1, 'b': [1, 2]}
    assert list(cache.keys()) == ['k']

def test_register_backend():
    # a cache instance as backend
    instance = MemoryCache()
    register_backend('mem-instance', instance)
    assert Cache.from_uri('mem-instance://anything') is instance

    # re-register triggers a warning
    with pytest.warns(UserWarning):
        register_backend('mem-instance', instance)

    # a function consuming **kwargs as backend
    def build1(uri, **kwargs):
        return MemoryCache()
    register_backend('mem-func1', build1)
    assert isinstance(Cache.from_uri('mem-func1://x'), MemoryCache)

    # a function consuming kwargs as a single dict arg
    def build2(uri, kwargs):
        return MemoryCache()
    register_backend('mem-func2', build2)
    assert isinstance(Cache.from_uri('mem-func2://x'), MemoryCache)

def test_decorator_ignore_first_arg():
    cache = MemoryCache()
    calls = []

    class A:
        @cache(scope='m', ignore_first_arg=True)
        def double(self, x):
            calls.append(x)
            return x * 2

    a, b = A(), A()
    assert a.double(2) == 4
    # different instance hits the same cache key
    assert b.double(2) == 4
    assert len(calls) == 1

def test_dict_protocol():
    cache = MemoryCache()
    cache['k'] = 1
    assert 'k' in cache
    assert cache['k'] == 1
    del cache['k']
    assert 'k' not in cache
    with pytest.raises(KeyError):
        cache['missing']

def test_file_cache_rejects_path_escape(tmp_path):
    cache = SimpleTextFileCache(root_dir=str(tmp_path / 'root'), suf='.txt')
    with pytest.raises(ValueError):
        cache.set('../escape', 'x')
    with pytest.raises(ValueError):
        cache.set('k', 'x', scope='../escape')

def test_simple_text_file_cache(tmp_path):
    cache = SimpleTextFileCache(root_dir=str(tmp_path), suf='.txt')
    cache.set('a/b', 'hello')
    assert cache.exists('a/b')
    assert cache.fetch('a/b') == 'hello'
    assert list(cache.keys()) == ['a/b']
    assert cache.pop('a/b', return_old=True) == 'hello'
    assert not cache.exists('a/b')
