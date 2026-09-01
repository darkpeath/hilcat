# -*- coding: utf-8 -*-

import pytest
from hilcat import (
    QmarkSqlBuilder, NumericSqlBuilder, NamedSqlBuilder,
    FormatSqlBuilder, PyformatSqlBuilder,
)
from hilcat.db.relational import BaseTableConfig

@pytest.fixture()
def config():
    return BaseTableConfig(table='t', uniq_columns=('id1', 'id2'), columns=('id1', 'id2', 'data'))

def test_create_table(config):
    op = QmarkSqlBuilder().build_create_table_operation(config)
    assert op.statement == "CREATE TABLE IF NOT EXISTS t (id1 text,id2 text,data text, PRIMARY KEY (id1,id2));"
    op = QmarkSqlBuilder().build_create_table_operation(config, check_exists=False)
    assert op.statement == "CREATE TABLE t (id1 text,id2 text,data text, PRIMARY KEY (id1,id2));"

def test_qmark_select(config):
    op = QmarkSqlBuilder().build_select_operation(config, key=['a', 'b'], limit=1)
    assert op.statement == "SELECT id1,id2,data FROM t WHERE id1 = ? AND id2 = ? LIMIT 1"
    assert list(op.parameters) == ['a', 'b']

def test_qmark_select_all(config):
    op = QmarkSqlBuilder().build_select_operation(config)
    assert op.statement == "SELECT id1,id2,data FROM t"
    assert list(op.parameters) == []

def test_numeric_select(config):
    op = NumericSqlBuilder().build_select_operation(config, key=['a', 'b'])
    assert op.statement == "SELECT id1,id2,data FROM t WHERE id1 = :1 AND id2 = :2"
    assert list(op.parameters) == ['a', 'b']

def test_numeric_delete(config):
    # composite key placeholders should be numbered :1, :2 (not :1, :1)
    op = NumericSqlBuilder().build_delete_operation(config, key=['a', 'b'])
    assert op.statement == "DELETE FROM t WHERE id1 = :1 AND id2 = :2"
    assert list(op.parameters) == ['a', 'b']

def test_numeric_update(config):
    op = NumericSqlBuilder().build_update_operation(config, key=['a', 'b'], value={'data': 'x'})
    # parameters are passed twice, so the SET part should continue numbering after the VALUES part
    assert op.statement == ("INSERT INTO t(data,id1,id2) VALUES (:1,:2,:3)"
                            " ON CONFLICT(id1,id2) DO UPDATE SET data=:4,id1=:5,id2=:6")
    assert list(op.parameters) == ['x', 'a', 'b', 'x', 'a', 'b']

def test_named_select(config):
    op = NamedSqlBuilder().build_select_operation(config, key=['a', 'b'])
    assert op.statement == "SELECT id1,id2,data FROM t WHERE id1 = :id1 AND id2 = :id2"
    assert op.parameters == {'id1': 'a', 'id2': 'b'}

def test_format_select(config):
    op = FormatSqlBuilder().build_select_operation(config, key=['a', 1])
    assert op.statement == "SELECT id1,id2,data FROM t WHERE id1 = %s AND id2 = %d"
    assert list(op.parameters) == ['a', 1]

def test_pyformat_update(config):
    op = PyformatSqlBuilder().build_update_operation(config, key=['a', 'b'], value={'data': 'x'})
    assert op.statement == ("INSERT INTO t(data,id1,id2) VALUES (%(data)s,%(id1)s,%(id2)s)"
                            " ON CONFLICT(id1,id2) DO UPDATE SET data=%(data)s,id1=%(id1)s,id2=%(id2)s")
    assert op.parameters == {'data': 'x', 'id1': 'a', 'id2': 'b'}

def test_delete_all(config):
    op = QmarkSqlBuilder().build_delete_operation(config)
    assert op.statement == "DELETE FROM t"
