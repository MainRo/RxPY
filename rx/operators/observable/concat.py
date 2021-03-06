from typing import Iterable, Union, cast

from rx.core import ObservableBase, AnonymousObservable, Disposable
from rx.disposables import SingleAssignmentDisposable, CompositeDisposable, SerialDisposable
from rx.concurrency import current_thread_scheduler


def concat(*args: Union[ObservableBase, Iterable[ObservableBase]]) -> ObservableBase:
    """Concatenates all the observable sequences.

    1 - res = concat(xs, ys, zs)
    2 - res = concat([xs, ys, zs])

    Returns an observable sequence that contains the elements of each given
    sequence, in sequential order.
    """

    if args and isinstance(args[0], Iterable):
        sources = iter(args[0])
    else:
        sources = iter(cast(Iterable, args))

    def subscribe(observer, scheduler=None):
        scheduler = scheduler or current_thread_scheduler

        subscription = SerialDisposable()
        cancelable = SerialDisposable()
        is_disposed = False

        def action(action1, state=None):
            nonlocal is_disposed
            if is_disposed:
                return

            def on_completed():
                cancelable.disposable = scheduler.schedule(action)

            try:
                current = next(sources)
            except StopIteration:
                observer.on_completed()
            except Exception as ex:
                observer.on_error(ex)
            else:
                d = SingleAssignmentDisposable()
                subscription.disposable = d
                d.disposable = current.subscribe_(observer.on_next, observer.on_error, on_completed, scheduler)

        cancelable.disposable = scheduler.schedule(action)

        def dispose():
            nonlocal is_disposed
            is_disposed = True

        return CompositeDisposable(subscription, cancelable, Disposable.create(dispose))
    return AnonymousObservable(subscribe)
