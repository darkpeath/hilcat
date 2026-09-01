# -*- coding: utf-8 -*-

import pytest
from typing import Sequence

# skip the whole module if psycopg is not installed
psycopg = pytest.importorskip("psycopg")
from hilcat import PostgresqlCache, RelationalDbScopeConfig

DATABASE = "postgresql://postgres:123@localhost:5432/hilcat_test"

def _connect():
    try:
        return psycopg.connect(DATABASE)
    except Exception as e:
        pytest.skip(f"postgresql server not available: {e}")

def drop_tables(tables: Sequence[str]):
    conn = _connect()
    cursor = conn.cursor()
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()
    cursor.close()
    conn.close()

def test_postgresql():
    scopes = [
        RelationalDbScopeConfig(scope='a', uniq_columns=['id'], columns=['id', 'name', 'comment', 'count'],
                                column_types={'count': 'int'}),
        RelationalDbScopeConfig(scope='b', uniq_columns=['eid'], columns=['eid', 'name', 'comment', 'status']),
        RelationalDbScopeConfig(
            scope='d', uniq_columns=['id1', 'id2'], columns=['value'],
            column_types={
                "value": "int",
            }
        ),
    ]
    drop_tables(tables=[x.table for x in scopes])
    cache = PostgresqlCache(database=DATABASE, scopes=scopes)
    cache.set(key='a1', value={'name': 'jii', 'comment': 'this is a1', 'count': 1}, scope='a')
    cache.set(key='a2', value={'name': 'iiwwww', 'comment': 'this is a2', 'count': 3}, scope='a')
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
