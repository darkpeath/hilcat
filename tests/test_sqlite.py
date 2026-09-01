# -*- coding: utf-8 -*-

import os
from hilcat import Cache
from hilcat import SqliteCache, RelationalDbScopeConfig

def clear_db(db_file: str):
    if os.path.exists(db_file):
        os.remove(db_file)

list_scopes = [
    RelationalDbScopeConfig(scope='a', uniq_column='id', columns=['id', 'name', 'comment', 'count'],
                            column_types={'count': 'int'}),
    RelationalDbScopeConfig(scope='b', uniq_column='eid', columns=['eid', 'name', 'comment', 'status']),
    RelationalDbScopeConfig(scope='d', uniq_columns=['id1', 'id2'], columns=['value']),
    dict(
        scope='e', uniq_columns=['id'], columns=['data'],
        value_adapter='json',
    ),
]
dict_scopes = {
    "a": RelationalDbScopeConfig(scope='a', uniq_column='id', columns=['id', 'name', 'comment', 'count'],
                                 column_types={'count': 'int'}),
    "b": dict(scope='b', uniq_column='eid', columns=['eid', 'name', 'comment', 'status']),
    "d": RelationalDbScopeConfig(scope='d', uniq_columns=['id1', 'id2'], columns=['value']),
    "e": dict(
        scope='e', uniq_columns=['id'], columns=['data'],
        value_adapter='json',
    ),
}

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

def test_dict_config_args():
    db_file = "t.db"
    clear_db(db_file)
    cache = SqliteCache(database=db_file, scopes=dict_scopes)
    run_test(cache)

def test_sqlite():
    db_file = "t.db"
    clear_db(db_file)
    cache = SqliteCache(database=db_file, scopes=list_scopes)
    run_test(cache)

def test_from_uri():
    db_file = "t.db"
    clear_db(db_file)
    cache = Cache.from_uri(f"sqlite:///{db_file}", scopes=list_scopes)
    run_test(cache)

def test_keys_and_scopes():
    db_file = "t.db"
    clear_db(db_file)
    cache = SqliteCache(database=db_file, scopes=list_scopes)
    run_test(cache)
    assert set(cache.scopes()) == {'a', 'b', 'd', 'e'}
    assert sorted(cache.keys(scope='a')) == ['a1', 'a3']
    assert list(cache.keys(scope='d')) == [('d1', 'd2')]

def test_single_table_cache():
    from hilcat import SqliteSingleTableCache, SingleTableConfig
    db_file = "t_single.db"
    clear_db(db_file)
    cache = SqliteSingleTableCache(database=db_file, config=SingleTableConfig(table='t'))
    cache.set('k1', 'v1', scope='s1')
    cache.set('k2', 'v2', scope='s1')
    cache.set('k1', 'v3', scope='s2')
    assert cache.fetch('k1', scope='s1') == 'v1'
    assert cache.fetch('k1', scope='s2') == 'v3'
    assert set(cache.scopes()) == {'s1', 's2'}
    assert sorted(cache.keys('s1')) == ['k1', 'k2']
    cache.pop('k1', scope='s2')
    assert not cache.exists('k1', scope='s2')


