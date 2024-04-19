==========
HiLCaT
==========

High level key-value storage and cache tool.

Installation
============

::

  pip install hilcat

Usage
=======

Cache api is designed to determine a unique node by :code:`scope` and :code:`key`.

In some implements, :code:`scope` may be always :code:`None` and should be ignored, thus unique node determined only by :code:`key`.


Init by different backends
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

auto backend
^^^^^^^^^^^^^^^^^

create a cache based on redis

.. code-block:: python

  from hilcat import Cache

  cache = Cache.from_uri('redis://localhost:1458')

create a cache based on sqlite

.. code-block:: python

  from hilcat import Cache

  cache = Cache.from_uri('sqlite:///t.db')

in memory cache
^^^^^^^^^^^^^^^^

.. code-block:: python

  from hilcat import MemoryCache

  cache = MemoryCache()

file based cache
^^^^^^^^^^^^^^^^^^

cache text content in file

.. code-block:: python

  from hilcat import SimpleTextFileCache

  cache = SimpleTextFileCache()

redis
^^^^^^^^^^^^^^^^^^^^^^^

init from url

.. code-block:: python

  from hilcat import RedisCache

  cache = RedisCache(url='redis://localhost:6379')

init from host

.. code-block:: python

  from hilcat import RedisCache

  cache = RedisCache(host='localhost', port=6579)

elasticsearch
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

  from hilcat import ElasticSearchCache

  cache = ElasticSearchCache(hosts=['https://localhost:9200'])

sqlite
^^^^^^^^^^^^^

.. code-block:: python

  from hilcat import SqliteCache, SqliteScopeConfig

  cache = SqliteCache(database=db_file, scopes=[
      SqliteScopeConfig(scope='a', uniq_column='id', columns=['id', 'name', 'comment', 'count'],
                        column_types={'count': 'int'}),
      SqliteScopeConfig(scope='b', uniq_column='eid', columns=['eid', 'name', 'comment', 'status'])
  ])

postgresql
^^^^^^^^^^^^^

.. code-block:: python

  from hilcat import PostgresqlCache, PostgresqlScopeConfig

  cache = PostgresqlCache(database="postgresql://postgres:123@localhost:5432/hilcat_test", scopes=[
      PostgresqlScopeConfig(scope='a', uniq_column='id', columns=['id', 'name', 'comment', 'count'],
                            column_types={'count': 'int'}),
      # PostgresqlScopeConfig(scope='b', uniq_column='eid', columns=['eid', 'name', 'comment', 'status'])
  ])

mysql
^^^^^^^

.. code-block:: python

  from hilcat import MysqlCache, MysqlScopeConfig

  cache = MysqlCache(connection=connection, scopes=[
      MysqlScopeConfig(scope='a', uniq_column='id', columns=['id', 'name', 'comment', 'count'],
                       column_types={'id': 'varchar(50)', 'count': 'int'}),
      MysqlScopeConfig(scope='b', uniq_column='eid', columns=['eid', 'name', 'comment', 'status'],
                       column_types={'eid': "int"})
  ])

cache api
~~~~~~~~~~~~~~~~~~~~

Assume there is a cache named :code:`cache`.

exists(key, scope=None)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Test if a key exists in cache for certain scope.


fetch(key, default=None, scope=None)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If key not exists, return default value.

.. code-block:: python

  value = cache.fetch('one', 1, scope='a')

set(key, value, scope=None)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

  cache.set('one', 1, scope='a')

update(key, value, scope=None, \*\*kwargs)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Same as method :code:`set`, but return value may diff in some implements.

get(key, func=None, scope=None)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If key exists, just return value stored in cache;
else if key not exists, calculate value and store to cache, the return value.

.. code-block:: python

  value = cache.get('one', lambda: 1, scope='a')

pop(key, scope=None)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Delete value of given key for certain scope.

scopes()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Get all scopes in the cache.

May not supported for some implements.

keys(scope=None)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Get all keys for certain scope.

May not supported for some implements.

load(scopes=None)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Load scope data from persistence storage.

Some implements may have no persistence storage, thus this method do nothing.

backup(scopes=None)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Save scope data to persistence storage.

Some implements may have no persistence storage, thus this method do nothing.

Decorate a function
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import collections
    from hilcat import SqliteCache, SqliteScopeConfig

    db_file = "decorator.db"
    cache = SqliteCache(database=db_file, scopes=[
        SqliteScopeConfig(scope='f1', uniq_column='x', columns=['y']),
        SqliteScopeConfig(scope='f3', uniq_column='key', columns=['key', 'value'])
    ])

    c1 = collections.Counter()
    @cache(scope="f1")
    def f1(x: int):
        c1[x] += 1
        return x + 1 + c1[x]

    c2 = collections.Counter()
    def f2(x: int):
        c2[x] += 1
        return x + 1 + c2[x]

    def make_key(x: int, y: int):
        return '-'.join(map(str, [x, y]))
    c3 = collections.Counter()
    @cache(scope="f3", make_key_func=make_key)
    def f3(x: int, y: int):
        c3[(x, y)] += 1
        return {
            "key": make_key(x, y),
            "value": x + y + c3[(x, y)],
        }

    # with cache, same result
    assert f1(1) == 3
    assert f1(1) == 3

    # without cache, different result
    assert f2(1) == 3
    assert f2(1) == 4

    assert f3(1, 2) == {"key": "1-2", "value": 4}
    assert f3(1, 2) == {"key": "1-2", "value": 4}

