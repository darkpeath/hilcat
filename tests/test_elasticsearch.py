# -*- coding: utf-8 -*-

import os
import pytest
from typing import Dict, Any

# skip the whole module if dependencies or the config file are missing
pytest.importorskip("elasticsearch")
yaml = pytest.importorskip("yaml")
from hilcat.db.es import ElasticSearchCache

CONFIG_FILE = os.path.abspath(os.path.join(__file__, '../es.yml'))
if not os.path.exists(CONFIG_FILE):
    pytest.skip(f"config file not found: {CONFIG_FILE}", allow_module_level=True)

def read_es_config() -> Dict[str, Any]:
    with open(CONFIG_FILE, encoding='utf-8') as f:
        return yaml.safe_load(f)

def test_connect():
    from elasticsearch import Elasticsearch
    client = Elasticsearch(**read_es_config())
    print(client.info())

def test_elasticsearch():
    cache = ElasticSearchCache(connect_args=read_es_config())
    cache.set(key='a1', value={'name': 'jii', 'comment': 'this is a1', 'count': 1}, scope='a')
    cache.set(key='a2', value={'name': 'iiwwww', 'comment': 'this is a2', 'count': 3}, scope='a')
    cache.set(key='b1', value={'name': '12b', 'comment': 'this is b1', 'status': 7}, scope='b')
    cache.set(key='c1', value=dict(id='c1', data='iiejje'), scope='c')
    cache.set(key='a3', value={'name': 'lli', 'comment': 'this is a3', 'count': 2}, scope='a')
    cache.set(key='a1', value={'name': 'jjii', 'comment': 'this is a1 again', 'count': 4}, scope='a')
    assert cache.get(key='a1', scope='a') == {'name': 'jjii', 'comment': 'this is a1 again', 'count': 4}
    cache.pop(key='a2', scope='a')
