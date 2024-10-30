# -*- coding: utf-8 -*-

import os
from hilcat import Cache
from hilcat import SqliteCache, RelationalDbScopeConfig

def clear_db(db_file: str):
    if os.path.exists(db_file):
        os.remove(db_file)

scopes = [
    RelationalDbScopeConfig(scope='a', uniq_column='id', columns=['id', 'name', 'comment', 'count'],
                            column_types={'count': 'int'}),
    RelationalDbScopeConfig(scope='b', uniq_column='eid', columns=['eid', 'name', 'comment', 'status']),
    RelationalDbScopeConfig(scope='d', uniq_columns=['id1', 'id2'], columns=['value']),
    RelationalDbScopeConfig(scope='e', uniq_columns=['id'], columns=['data']),
]
def run_test(cache: Cache):
    cache.set(key='a1', value={'name': 'jii', 'comment': 'this is a1', 'count': 1}, scope='a')
    cache.set(key='a2', value={'name': 'iiwwww', 'comment': 'this is a2', 'count': 3}, scope='a')
    cache.set(key='b1', value={'name': '12b', 'comment': 'this is b1', 'status': 7}, scope='b')
    try:
        cache.set(key='c1', value=dict(id='c1', data='iiejje'), scope='c')
    except ValueError:
        pass
    else:
        assert False
    cache.set(key='a3', value={'name': 'lli', 'comment': 'this is a3', 'count': 2}, scope='a')
    cache.set(key='a1', value={'name': 'jjii', 'comment': 'this is a1 again', 'count': 4}, scope='a')
    cache.pop(key='a2', scope='a')
    cache.set(key=("d1", "d2"), value=3, scope="d")
    assert cache.get(("d1", "d2"), scope="d") == 3
    cache.set("e1", {"a": 1, "b": "we"}, scope="e")
    assert cache.get("e1", scope="e") == {'a': 1, 'b': 'we'}

def test_sqlite():
    db_file = "t.db"
    clear_db(db_file)
    cache = SqliteCache(database=db_file, scopes=scopes)
    run_test(cache)

def test_from_uri():
    db_file = "t.db"
    clear_db(db_file)
    cache = Cache.from_uri(f"sqlite:///{db_file}", scopes=scopes)
    run_test(cache)


