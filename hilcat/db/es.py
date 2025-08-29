# -*- coding: utf-8 -*-

from typing import (
    Any, Iterable,
    Dict,
)
from ..core import RegistrableCache
import elasticsearch as es

class ElasticSearchCache(RegistrableCache):
    """
    Use elasticsearch as backend.
    Each scope corresponds to an index, and each key corresponds to a document.
    """

    @classmethod
    def from_uri(cls, uri: str, **kwargs) -> 'ElasticSearchCache':
        if not uri.startswith('es://'):
            raise ValueError(f"It's not a es uri: {uri}.")
        connect_args = kwargs.copy()
        connect_args['hosts'] = uri[5:]
        return cls(connect_args=connect_args)

    def __init__(self, client: es.Elasticsearch = None, connect_args: Dict[str, Any] = None):
        """
        Create a cache based on elasticsearch.
        :param client:              es client
        :param connect_args:        args to create a client
        """
        self.client = client or es.Elasticsearch(**(connect_args or {}))

    def exists(self, key: str, scope: str = None, **kwargs) -> bool:
        return self.client.exists(index=scope, id=key).body

    def fetch(self, key: str, default: Any = None, scope: str = None, **kwargs) -> Any:
        try:
            res = self.client.get(index=scope, id=key)
        except es.NotFoundError:
            return None
        return res.body.get('_source', default)

    def set(self, key: str, value: Dict[str, Any], scope: str = None, **kwargs) -> bool:
        res = self.client.index(index=scope, id=key, document=value)
        return res.body.get('result') in ['created', 'updated']

    def pop(self, key: str, scope: str = None, **kwargs) -> bool:
        res = self.client.delete(index=scope, id=key)
        return res.body.get('result') == 'deleted'

    def scopes(self) -> Iterable[str]:
        # it's not suggested to get all indexes in es
        raise NotImplementedError()

    def keys(self, scope: str = None) -> Iterable[str]:
        # it's not suggested to get all documents
        raise NotImplementedError()


