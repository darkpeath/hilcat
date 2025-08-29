from typing import Union
from pathlib import Path
import os
from hilcat import Cache
from hilcat import SqliteDictCache

def clear_db(db_file: Union[str, os.PathLike]):
    if os.path.exists(db_file):
        os.remove(db_file)

def run_test(cache: Cache):
    cache.set(key='a1', value={'name': 'jii', 'comment': 'this is a1', 'count': 1}, scope='a')
    cache.set(key='a2', value={'name': 'iiwwww', 'comment': 'this is a2', 'count': 3}, scope='a')
    cache.set(key='b1', value={'name': '12b', 'comment': 'this is b1', 'status': 7}, scope='b')
    cache.set(key='a3', value={'name': 'lli', 'comment': 'this is a3', 'count': 2}, scope='a')
    cache.set(key='a1', value={'name': 'jjii', 'comment': 'this is a1 again', 'count': 4}, scope='a')
    cache.pop(key='a2', scope='a')
    cache.set("e1", {"a": 1, "b": "we"}, scope="e")
    assert cache.get("e1", scope="e") == {'a': 1, 'b': 'we'}

def test_sqlite():
    db_file = Path(__file__).parent.joinpath("sqlitedict.db")
    clear_db(db_file)
    cache = SqliteDictCache(db_file=db_file)
    run_test(cache)

