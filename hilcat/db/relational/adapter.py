from abc import ABC, abstractmethod
from typing import Any, Dict, Sequence, Type, Union
import json

# cache data format may diff from db api, we need an adapter to bridging
class ValueAdapter(ABC):
    """
    Method `build_column_values` and `parse_column_values` should be reversed one-to-one mapping.
    That is to say:
        parse_column_values(build_column_values(x)) == x
        build_column_values(parse_column_values(x)) == x
    """
    @abstractmethod
    def build_column_values(self, value: Any) -> Dict[str, Any]:
        """
        Used in method `RelationalDbCache.set()` to build column values
        """
    @abstractmethod
    def parse_column_values(self, value: Dict[str, Any]) -> Any:
        """
        Used in method `RelationalDbCache.fetch()` to parse column values.
        """

class NoModifyAdapter(ValueAdapter):
    """
    Cache value should be exactly column values and thus nothing should do.
    """

    def build_column_values(self, value: Dict[str, Any]) -> Dict[str, Any]:
        return value

    def parse_column_values(self, value: Dict[str, Any]) -> Dict[str, Any]:
        return value

class SingleAdapter(ValueAdapter):
    """
    Cache value is exactly one column.
    """

    def __init__(self, col: str):
        self.col = col

    def build_column_values(self, value: Any) -> Dict[str, Any]:
        return {self.col: value}

    def parse_column_values(self, value: Dict[str, Any]) -> Any:
        return value[self.col]

class SingleJsonValueAdapter(SingleAdapter):
    """
    Cache value is stored in one column, and can be a complex type
      such as dict or list, type in db is text in json format.
    """
    def __init__(self, col: str, ensure_ascii=True):
        super().__init__(col)
        self.ensure_ascii = ensure_ascii

    def build_column_values(self, value: Dict[str, Any]) -> Dict[str, Any]:
        value = json.dumps(value, ensure_ascii=self.ensure_ascii)
        return super().build_column_values(value)

    def parse_column_values(self, value: Dict[str, Any]) -> Any:
        s = super().parse_column_values(value)
        return json.loads(s)

class JsonValueAdapter(ValueAdapter):
    """
    All value type in db should be text.
    """
    def __init__(self, ensure_ascii=True):
        super().__init__()
        self.ensure_ascii = ensure_ascii

    def build_column_values(self, value: Dict[str, Any]) -> Dict[str, Any]:
        return {k: json.dumps(v, ensure_ascii=self.ensure_ascii) for k, v in value.items()}

    def parse_column_values(self, value: Dict[str, Any]) -> Dict[str, Any]:
        return {k: json.loads(v) for k, v in value.items()}

class SequenceAdapter(ValueAdapter):
    """
    Cache value is a list or tuple, corresponding to some columns.
    """

    def __init__(self, cols: Sequence[str], return_type: Type[Union[list, tuple]] = tuple):
        self.cols = cols
        self.return_type = return_type

    def build_column_values(self, value: Sequence[Any]) -> Dict[str, Any]:
        return dict(zip(self.cols, value))

    def parse_column_values(self, value: Dict[str, Any]) -> Sequence[Any]:
        return self.return_type(map(value.get, self.cols))

class AutoAdapter(ValueAdapter):
    def __init__(self, cols: Sequence[str]):
        self.cols = list(cols)
        self.adapter = None

    def build_column_values(self, value: Any) -> Dict[str, Any]:
        # TODO 8/29/25
        # determine the actual adapter when first time see the data
        pass

    def parse_column_values(self, value: Dict[str, Any]) -> Any:
        # TODO 8/29/25
        pass

_BUILTIN_ADAPTER_BUILDERS = {
    'default': lambda cols: SingleAdapter(cols[0]) if len(cols) == 1 else NoModifyAdapter(),
    'immutable': lambda cols: NoModifyAdapter(),
    'single': lambda cols: SingleAdapter(cols[0]),
    'tuple': lambda cols: SequenceAdapter(cols, return_type=tuple),
    'list': lambda cols: SequenceAdapter(cols, return_type=list),
    'json': lambda cols: SingleJsonValueAdapter(cols[0]) if len(cols) == 1 else JsonValueAdapter(),
}

