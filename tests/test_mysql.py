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
    scopes = [
        RelationalDbScopeConfig(scope='a', uniq_column='id', columns=['id', 'name', 'comment', 'count'],
                                column_types={'id': 'varchar(50)', 'count': 'int'}),
        RelationalDbScopeConfig(scope='b', uniq_column='eid', columns=['eid', 'name', 'comment', 'status'],
                                column_types={'eid': "int"}),
        RelationalDbScopeConfig(
            scope='d', uniq_columns=['id1', 'id2'], columns=['value'],
            column_types={
                "id1": "varchar(10)",
                "id2": "varchar(10)",
                "value": "int(5)",
            }
        ),
    ]
    cursor = connection.cursor()
    for scope in scopes:
        cursor.execute(f'DROP TABLE IF EXISTS {scope.table}')
    connection.commit()
    cursor.close()
    cache = MysqlCache(connection=connection, scopes=scopes)
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
    cache.set(key=("d1", "d2"), value=3, scope="d")
    assert cache.get(("d1", "d2"), scope="d") == 3

def test_pymysql_backend():
    import pymysql
    pipeline(pymysql.connect(host='localhost', user='root', database='hilcat_test'))

def test_mysql_connector_backend():
    import mysql.connector
    pipeline(mysql.connector.connect(host='localhost', user='root', database='hilcat_test'))
