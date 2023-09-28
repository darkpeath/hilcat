# -*- coding: utf-8 -*-

from hilcat import SqliteCache, SqliteScopeConfig

def test_sqlite():
    cache = SqliteCache("t.db", scopes=[
        SqliteScopeConfig(scope='a', uniq_id='id', columns=['id', 'name', 'comment', 'count'],
                          column_types={'count': 'int'}),
        SqliteScopeConfig(scope='b', uniq_id='eid', columns=['eid', 'name', 'comment', 'status'])
    ])
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


