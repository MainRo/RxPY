from typing import Callable
from rx.core import abc
from .observable import Observable


class AnonymousObservable(Observable):
    """Class to create an Observable instance from a delegate-based
    implementation of the Subscribe method."""

    def __init__(self, subscribe: Callable) -> None:
        """Creates an observable sequence object from the specified
        subscription function.

        Args:
            subscribe: Subscribe method implementation.
        """

        self._subscribe = subscribe
        super().__init__()

    def _subscribe_core(self, observer: abc.Observer, scheduler: abc.Scheduler = None):
        return self._subscribe(observer, scheduler)
