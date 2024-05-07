# -*- coding: utf-8 -*-

from hilcat import MysqlCache, RelationalDbScopeConfig

def test_connect1():
    import pymysql
    conn = pymysql.connect(host='localhost', user='root', database='hilcat_test')
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    res = list(cursor.fetchall())
    print(res)
    conn.close()

def test_connect2():
    import mysql.connector
    conn = mysql.connector.connect(host='localhost', user='root', database='hilcat_test')
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    res = list(cursor.fetchall())
    print(res)
    conn.close()

def pipeline(connection):
    cache = MysqlCache(connection=connection, scopes=[
        RelationalDbScopeConfig(scope='a', uniq_column='id', columns=['id', 'name', 'comment', 'count'],
                                column_types={'id': 'varchar(50)', 'count': 'int'}),
        RelationalDbScopeConfig(scope='b', uniq_column='eid', columns=['eid', 'name', 'comment', 'status'],
                                column_types={'eid': "int"})
    ])
    cache.set(key='a1', value={'name': 'jii', 'comment': 'this is a1', 'count': 1}, scope='a')
    cache.set(key='a2', value={'name': 'iiwwww', 'comment': 'this is a2', 'count': 3}, scope='a')
    cache.set(key=1, value={'name': '12b', 'comment': 'this is b1', 'status': 7}, scope='b')
    try:
        cache.set(key='c1', value=dict(id='c1', data='iiejje'), scope='c')
    except ValueError:
        pass
    else:
        assert False
    cache.set(key='a3', value={'name': 'lli', 'comment': 'this is a3', 'count': 2}, scope='a')
    cache.set(key='a1', value={'name': 'jjii', 'comment': 'this is a1 again', 'count': 4}, scope='a')
    cache.pop(key='a2', scope='a')

def test_pymysql_backend():
    import pymysql
    pipeline(pymysql.connect(host='localhost', user='root', database='hilcat_test'))

def test_mysql_connector_backend():
    import mysql.connector
    pipeline(mysql.connector.connect(host='localhost', user='root', database='hilcat_test'))
