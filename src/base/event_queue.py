from simple_heap import Heap

class EventQueue(object):
    def __init__(self):
        self._events_heap = Heap()
        self._handlers = {}
        self._latest_handled_timestamp = -1

    def add_event(self, event):
        assert (event.timestamp, event) not in self._events_heap # TODO: slow assert, disable for production
        assert event.timestamp >= self._latest_handled_timestamp

        # insert into heap
        self._events_heap.push( (event.timestamp, event) )

    def remove_event(self, event):
        assert (event.timestamp, event) in self._events_heap
        self._events_heap.remove( (event.timestamp, event) )

    @property
    def events(self):
        "All events, used for testing"
        return set(event for (timestamp, event) in self._events_heap)

    @property
    def sorted_events(self):
        "Sorted events, used for testing"
        return sorted(self.events)

    @property
    def is_empty(self):
        return len(self) == 0

    def __len__(self):
        return len(self._events_heap)

    def pop(self):
        assert not self.is_empty
        timestamp, event = self._events_heap.pop()
        return event

    def _get_event_handlers(self, event_type):
        if event_type in self._handlers:
            return self._handlers[event_type]
        else:
            return []

    def advance(self):
        assert not self.is_empty
        event = self.pop()
        for handler in self._get_event_handlers( type(event) ):
            handler(event)
        self._latest_handled_timestamp = event.timestamp

    def add_handler(self, event_type, handler):
        self._handlers.setdefault(event_type, [])
        self._handlers[event_type].append(handler)

    def __str__(self):
        return "EventQueue<num_events=%s>" % len(self)
