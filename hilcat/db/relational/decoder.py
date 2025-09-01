from abc import ABC, abstractmethod
from typing import Dict, Any

class ValueDecoder(ABC):
    """
    Decode row selected from database.
    """

    @abstractmethod
    def encode(self, value: Dict[str, Any]) -> Any:
        pass

    def __call__(self, value: Dict[str, Any]) -> Any:
        return self.encode(value)

class NoModifyDecoder(ValueDecoder):
    def encode(self, value: Dict[str, Any]) -> Any:
        return value

