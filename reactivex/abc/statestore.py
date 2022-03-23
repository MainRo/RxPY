from abc import ABC, abstractmethod
from typing import Generic, TypeVar

_T_key = TypeVar("_T_key")
_T_val = TypeVar("_T_val")

class StateBase(Generic[_T_key, _T_val], ABC):
    @abstractmethod
    def set(self, key: _T_key, value: _T_val):
        pass

    @abstractmethod
    def get(self, key: _T_key) -> _T_val:
        pass

    @abstractmethod
    def create_key(self, key: _T_key):
        pass

    @abstractmethod
    def delete_key(self, key: _T_key):
        pass


class StateStoreBase(ABC):
    @abstractmethod
    def create_state(self, name: str) -> StateBase:
        pass
