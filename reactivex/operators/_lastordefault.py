from typing import Any, Callable, Optional, TypeVar

from reactivex import Observable, abc
from reactivex import operators as ops
from reactivex import typing
from reactivex.internal.exceptions import SequenceContainsNoElementsError
from reactivex.state import STATE_NOTSET, MemoryStateStore


_T = TypeVar("_T")


def last_or_default_async(
    source: Observable[_T],
    has_default: bool = False,
    default_value: Optional[_T] = None,
) -> Observable[Optional[_T]]:
    def subscribe(
        observer: abc.ObserverBase[Optional[_T]],
        scheduler: Optional[abc.SchedulerBase] = None,
        state_store: Optional[abc.StateStoreBase] = None,
    ):
        state_store = state_store or MemoryStateStore()     
        
        value = state_store.create_state(state_store.next_unique_id("last_or_default_async"))        
        value.create_key(0)
        if has_default:
            value.set(0, default_value)

        def on_next(x: _T) -> None:            
            value.set(0, x)

        def on_completed():
            v = value.get(0)
            if v is STATE_NOTSET and not has_default:
                observer.on_error(SequenceContainsNoElementsError())
            else:
                observer.on_next(v)
                observer.on_completed()

            state_store.delete_state(state_store)

        return source.subscribe(
            on_next, observer.on_error, on_completed,
            scheduler=scheduler, state_store=state_store
        )

    return Observable(subscribe)


def last_or_default(
    default_value: Optional[_T] = None, predicate: Optional[typing.Predicate[_T]] = None
) -> Callable[[Observable[_T]], Observable[Any]]:
    def last_or_default(source: Observable[Any]) -> Observable[Any]:
        """Return last or default element.

        Examples:
            >>> res = _last_or_default(source)

        Args:
            source: Observable sequence to get the last item from.

        Returns:
            Observable sequence containing the last element in the
            observable sequence.
        """

        if predicate:
            return source.pipe(
                ops.filter(predicate),
                ops.last_or_default(default_value),
            )

        return last_or_default_async(source, True, default_value)

    return last_or_default


__all__ = ["last_or_default", "last_or_default_async"]
