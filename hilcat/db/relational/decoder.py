from abc import ABC, abstractmethod
from typing import Dict, Any

class ValueDecoder(ABC):
    """
    Decode row selected from database.
    """

    @abstractmethod
    def decode(self, value: Dict[str, Any]) -> Any:
        pass

    def __call__(self, value: Dict[str, Any]) -> Any:
        return self.decode(value)

class NoModifyDecoder(ValueDecoder):
    def decode(self, value: Dict[str, Any]) -> Any:
        return value
