# -*- coding: utf-8 -*-

import pytest
from hilcat import Cache, NoOpCache, MemoryCache, SimpleTextFileCache

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
