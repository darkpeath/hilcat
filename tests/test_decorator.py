import os
import collections
from hilcat import SqliteCache, SqliteScopeConfig

def test_decorator():
    db_file = "decorator.db"
    if os.path.exists(db_file):
        os.remove(db_file)
    cache = SqliteCache(database=db_file, scopes=[
        SqliteScopeConfig(scope='f1', uniq_column='x', columns=['x', 'y']),
        SqliteScopeConfig(scope='f3', uniq_column='key', columns=['key', 'value'])
    ])

    c1 = collections.Counter()
    @cache(scope="f1")
    def f1(x: int):
        c1[x] += 1
        return {
            "x": x,
            "y": x + 1 + c1[x],
        }

    c2 = collections.Counter()
    def f2(x: int):
        c2[x] += 1
        return {
            "x": x,
            "y": x + 1 + c2[x],
        }

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
    assert f1(1) == {'x': 1, 'y': 3}
    assert f1(1) == {'x': 1, 'y': 3}

    # without cache, different result
    assert f2(1) == {'x': 1, 'y': 3}
    assert f2(1) == {'x': 1, 'y': 4}

    assert f3(1, 2) == {"key": "1-2", "value": 4}
    assert f3(1, 2) == {"key": "1-2", "value": 4}

