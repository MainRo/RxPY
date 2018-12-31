from rx.core import AnonymousObserver, Disposable, Observable
from rx.disposables import CompositeDisposable
from .subscription import Subscription


class ColdObservable(Observable):
    def __init__(self, scheduler, messages):
        super().__init__()

        self.scheduler = scheduler
        self.messages = messages
        self.subscriptions = []

    def _subscribe_core(self, observer, scheduler=None):
        clock = self.scheduler.to_relative(self.scheduler.now)
        self.subscriptions.append(Subscription(clock))
        index = len(self.subscriptions) - 1
        disposable = CompositeDisposable()

        def get_action(notification):
            def action(scheduler, state):
                notification.accept(observer)
                return Disposable.empty()
            return action

        for message in self.messages:
            notification = message.value

            # Don't make closures within a loop
            action = get_action(notification)
            disposable.add(self.scheduler.schedule_relative(message.time, action))

        def dispose():
            start = self.subscriptions[index].subscribe
            end = self.scheduler.to_relative(self.scheduler.now)
            self.subscriptions[index] = Subscription(start, end)
            disposable.dispose()

        return Disposable.create(dispose)
