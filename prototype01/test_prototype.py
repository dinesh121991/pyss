from unittest import TestCase

import prototype

class test_EventQueue(TestCase):
    def setUp(self):
        self.queue = prototype.EventQueue()
    def tearDown(self):
        del self.queue

    def test_empty_initialization(self):
        self.assertEqual( 0, len(list(self.queue._events)) )

    def test_events_empty(self):
        self.assertEqual( 0, len(list(self.queue._events)) )

    def test_add_event_sanity(self):
        self.queue.add_event( prototype.JobEvent(0, 0) )

    def test_add_event_simple(self):
        events = [prototype.JobEvent(timestamp=0, job_id=i) for i in xrange(10)]
        for event in events:
            self.queue.add_event(event)
        self.assertEqual( event, list(self.queue._events) )

if __name__ == "__main__":
    try:
        from testoob import main
    except ImportError:
        print "Can't find Testoob, using unittest"
        from unittest import main
    main()
