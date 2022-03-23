from typing import TypeVar
from reactivex import abc, typing
from .markers import STATE_NOTSET, STATE_CLEARED


_T_key = TypeVar("_T_key")
_T_val = TypeVar("_T_val")

class MemoryState(abc.StateBase[_T_key, _T_val]):
    def __init__(self, name: str):
        self.name = name
        self.values: dict[_T_key, _T_val] = dict()

    def set(self, key: _T_key, value: _T_val):
        self.values[key] = value

    def get(self, key: _T_key) -> _T_val:
        return self.values[key]

    def create_key(self, key: _T_key):
        self.values[key] = STATE_NOTSET

    def delete_key(self, key: _T_key):
        del self.values[key]


class MemoryStateStore(abc.StateStoreBase):
    def __init__(self):
        self.storage: dict[str, MemoryState] = dict()
        self.unique_names_ids: dict[str, int] = dict()

    def next_unique_id(self, name: str) -> str:
        next_id = self.unique_names_ids.get(name, 0)
        self.unique_names_ids[name] = next_id + 1
        return "{}-{}".format(name, next_id)

    def create_state(self, name: str) -> MemoryState:
        if name in self.storage:
            raise ValueError("state name already exists: {}".format(name))
        state = MemoryState(name)
        self.storage['name'] = state
        return state

    def delete_state(self, MemoryState):
        pass
