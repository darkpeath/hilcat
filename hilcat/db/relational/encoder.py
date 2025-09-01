from abc import ABC, abstractmethod
from typing import Dict, Any

class ValueEncoder(ABC):
    """
    Encode cache value as dict with key to be column names.
    """
    @abstractmethod
    def encode(self, value: Any) -> Dict[str, Any]:
        pass

    def __call__(self, value: Any) -> Dict[str, Any]:
        return self.encode(value)

class NoModifyEncoder(ValueEncoder):
    def encode(self, value: Any) -> Dict[str, Any]:
        return value

