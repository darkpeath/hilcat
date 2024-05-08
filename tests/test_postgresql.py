# -*- coding: utf-8 -*-

import psycopg
from typing import Sequence
from hilcat import PostgresqlCache, RelationalDbScopeConfig

def test_connect():
    # conn = psycopg.connect(dbname='hilcat_test', user='postgres', password='123', host='localhost', port=5432)
    conn = psycopg.connect("postgresql://postgres:123@localhost:5432/hilcat_test")
    cursor = conn.cursor()
    cursor.execute("SELECT tablename FROM pg_tables")
    res = list(cursor.fetchall())
    print(res)
    conn.close()

def drop_tables(database: str, tables: Sequence[str]):
    conn = psycopg.connect(database)
    cursor = conn.cursor()
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    cursor.close()
    conn.close()

def test_postgresql():
    database = "postgresql://postgres:123@localhost:5432/hilcat_test"
    scopes = [
        RelationalDbScopeConfig(scope='a', uniq_column='id', columns=['id', 'name', 'comment', 'count'],
                                column_types={'count': 'int'}),
        RelationalDbScopeConfig(scope='b', uniq_column='eid', columns=['eid', 'name', 'comment', 'status'])
    ]
    drop_tables(database, tables=[x.table for x in scopes])
    cache = PostgresqlCache(database=database, scopes=scopes)
    cache.set(key='a1', value={'name': 'jii', 'comment': 'this is a1', 'count': 1}, scope='a')
    cache.set(key='a2', value={'name': 'iiwwww', 'comment': 'this is a2', 'count': 3}, scope='a')
    # cache.set(key='b1', value={'name': '12b', 'comment': 'this is b1', 'status': 7}, scope='b')
    try:
        cache.set(key='c1', value=dict(id='c1', data='iiejje'), scope='c')
    except ValueError:
        pass
    else:
        assert False
    cache.set(key='a3', value={'name': 'lli', 'comment': 'this is a3', 'count': 2}, scope='a')
    cache.set(key='a1', value={'name': 'jjii', 'comment': 'this is a1 again', 'count': 4}, scope='a')
    cache.pop(key='a2', scope='a')


