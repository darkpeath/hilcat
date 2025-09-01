from typing import (
    Sequence, Dict, Union, Literal, Callable,
    Any, Optional, Hashable,
)
import dataclasses
import warnings
from .encoder import ValueEncoder, NoModifyEncoder
from .decoder import ValueDecoder, NoModifyDecoder
from .adapter import ValueAdapter, _BUILTIN_ADAPTER_BUILDERS

_EXECUTE_PARAM_TYPE = Union[Sequence[Any], Dict[str, Any]]

@dataclasses.dataclass
class Operation:
    """
    define an operation for execute() or executemany()
    """
    statement: str       # arg operation for cursor.execute()
    parameters: _EXECUTE_PARAM_TYPE = dataclasses.field(default_factory=list)  # arg parameters for cursor.execute()

    # many statements in template or not, if True, use cursor.executemany() instead of cursor.execute()
    many: bool = False

class BaseTableConfig:
    def __init__(
        self,
        table: str,
        uniq_columns: Sequence[str] = ('id',),
        columns: Sequence[str] = ('data',),
        column_types: Dict[str, str] = None,
        value_adapter: Union[Literal['default', 'immutable', 'single', 'tuple', 'list', 'json'], ValueAdapter] = 'default',
        default_column_type: str = None,
        encoder: Union[None, ValueEncoder, Callable[[Any], Dict[str, Any]]] = None,
        decoder: Union[None, ValueDecoder, Callable[[Dict[str, Any]], Any]] = None,
    ):
        """
        :param table:
        :param uniq_columns:    unique columns to identify rows
        :param columns:         columns to select when invoke `cache.fetch()`
        :param column_types:    if column not specified here, type should be str
        :param value_adapter:   convert value when fetch and set
        :param default_column_type:     if column type not specified, use this value as default
        :param encoder:         convert value when set
        :param decoder:         convert value when fetch
        """
        self.table = table
        self.columns = columns
        self.column_types = dict(column_types or {})
        self.default_column_type = default_column_type
        if isinstance(uniq_columns, str):
            self.uniq_columns = (uniq_columns,)
        else:
            self.uniq_columns = uniq_columns
        if not self.uniq_columns:
            raise ValueError("uniq_columns cannot be empty.")
        self.columns_with_id = [x for x in self.uniq_columns if x not in self.columns] + list(self.columns)
        self.value_adapter = self._check_value_adapter(value_adapter, columns)
        self.encoder = encoder
        self.decoder = decoder
        if self.encoder is None:
            if self.value_adapter is not None:
                self.encoder = self.value_adapter.build_column_values
            else:
                self.encoder = NoModifyEncoder()
        if self.decoder is None:
            if self.value_adapter is not None:
                self.decoder = self.value_adapter.parse_column_values
            else:
                self.decoder = NoModifyDecoder()

    @staticmethod
    def _check_value_adapter(adapter, columns: Sequence[str]) -> ValueAdapter:
        if adapter == 'single' and len(columns) != 1:
            raise ValueError(f"columns length should be 1 when value_adapter is 'single'.")
        if isinstance(adapter, str) and adapter in _BUILTIN_ADAPTER_BUILDERS:
            return _BUILTIN_ADAPTER_BUILDERS[adapter](columns)
        if isinstance(adapter, ValueAdapter):
            return adapter
        if isinstance(adapter, str):
            msg = f"Unexpected value_adapter: {adapter}"
        else:
            msg = f"Unexpected value_adapter type: {type(adapter)}"
        raise ValueError(msg)

    def get_column_type(self, col: str) -> str:
        return self.column_types.get(col, self.default_column_type)

    @staticmethod
    def normalize_columns_values(value: Any, columns: Sequence[str]) -> Sequence[Any]:
        n = len(columns)
        if n > 1:
            if not isinstance(value, (list, tuple)):
                raise ValueError(f"value should be a list or tuple, got {type(value)}")
            m = len(value)
            if m != n:
                raise ValueError(f"value size should be {n}, but got {m}")
            return value
        else:
            return [value]

    def normalize_uniq_column_values(self, key: Any) -> Sequence[Any]:
        return self.normalize_columns_values(key, self.uniq_columns)

class RelationalDbScopeConfig(BaseTableConfig):
    def __init__(
        self,
        scope: Optional[Hashable], table: str = None,
        uniq_column: str = None,
        uniq_columns: Sequence[str] = ('id',),
        columns: Sequence[str] = ('data',),
        column_types: Dict[str, str] = None,
        value_adapter: Union[Literal['default', 'single', 'tuple', 'list', 'json'], ValueAdapter] = 'default',
        default_column_type: str = None,
        encoder: Union[None, ValueEncoder, Callable[[Any], Dict[str, Any]]] = None,
        decoder: Union[None, ValueDecoder, Callable[[Dict[str, Any]], Any]] = None,
    ):
        self.scope = scope
        if not table:
            if not isinstance(scope, str):
                raise ValueError("Arg scope must be a str when table not given.")
            table = scope
        if uniq_column:
            warnings.warn("`uniq_column` is deprecated, use `uniq_columns` instead.", DeprecationWarning)
            uniq_columns = (uniq_column,)
        super().__init__(
            table=table,
            uniq_columns=uniq_columns,
            columns=columns,
            column_types=column_types,
            value_adapter=value_adapter,
            default_column_type=default_column_type,
            encoder=encoder,
            decoder=decoder,
        )

class SingleTableConfig(BaseTableConfig):
    def __init__(self, table: str,
                 scope_columns: Sequence[str] = ('scope',),
                 key_columns: Sequence[str] = ('id',),
                 columns: Sequence[str] = ('data',),
                 column_types: Dict[str, str] = None,
                 value_adapter: Union[Literal['auto', 'default', 'single', 'tuple', 'list'], ValueAdapter] = 'auto',
                 default_column_type: str = None):
        self.scope_columns = tuple(scope_columns)
        self.key_columns = tuple(key_columns)
        uniq_columns = self.scope_columns + self.key_columns
        super().__init__(table, uniq_columns, columns, column_types, value_adapter, default_column_type)

    def normalize_scope_column_values(self, scope: Any) -> Sequence[Any]:
        return self.normalize_columns_values(scope, self.scope_columns)

    def normalize_key_column_values(self, scope: Any) -> Sequence[Any]:
        return self.normalize_columns_values(scope, self.key_columns)

