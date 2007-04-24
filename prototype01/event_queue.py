import bisect
class EventQueue(object):
    class EmptyQueue(Exception): pass

    def __init__(self):
        self._sorted_events = []
        self._handlers = {}
        self._latest_handled_timestamp = -1

    def add_event(self, event):
        assert event not in self._sorted_events # TODO: slow assert, disable for production
        assert event.timestamp >= self._latest_handled_timestamp
        # insert mainting sort
        bisect.insort(self._sorted_events, event)

    def remove_event(self, event):
        assert event in self._sorted_events
        self._sorted_events.remove(event)

    @property
    def events(self):
        return set(self._sorted_events)

    @property
    def empty(self):
        return len(self) == 0

    def __len__(self):
        return len(self._sorted_events)

    def _assert_not_empty(self):
        if self.empty:
            raise self.EmptyQueue()

    def pop(self):
        self._assert_not_empty()
        return self._sorted_events.pop(0)

    def _get_event_handlers(self, event_type):
        if event_type in self._handlers:
            return self._handlers[event_type]
        else:
            return []

    def advance(self):
        self._assert_not_empty()
        event = self.pop()
        for handler in self._get_event_handlers( type(event) ):
            handler(event)
        self._latest_handled_timestamp = event.timestamp

    def add_handler(self, event_type, handler):
        self._handlers.setdefault(event_type, [])
        self._handlers[event_type].append(handler)

    def __str__(self):
        return "EventQueue<num_events=%s>" % len(self._sorted_events)
