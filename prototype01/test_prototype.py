from unittest import TestCase

import random, copy

import prototype

class test_JobEvent(TestCase):
    def test_sort_order_random(self):
        random_events = [prototype.JobEvent(timestamp=random.randrange(0,100), job_id=0) for i in xrange(30)]
        sorted_events = sorted(random_events, key=lambda event:event.timestamp)
        self.assertEqual( sorted_events, sorted(random_events) )

class test_EventQueue(TestCase):
    def setUp(self):
        self.queue = prototype.EventQueue()
        self.event = prototype.JobEvent(timestamp=0, job_id=0)
        self.events = [
                prototype.JobEvent(timestamp=0, job_id=i)
                for i in xrange(10)
            ]

        def handler(event):
            handler.called = True
        handler.called = False
        self.handler = handler

    def tearDown(self):
        del self.queue
        del self.event

    def test_empty_initialization(self):
        self.assertEqual( 0, len(list(self.queue._events)) )

    def test_events_empty(self):
        self.assertEqual( 0, len(list(self.queue._events)) )

    def test_add_event_sanity(self):
        self.queue.add_event( self.event )

    def test_add_event_single_event(self):
        self.queue.add_event(self.event)
        self.assertEqual( [self.event], self.queue._events )

    def test_add_event_simple(self):
        for event in self.events:
            self.queue.add_event(event)
        self.assertEqual( self.events, list(self.queue._events) )

    def test_pop_one_job(self):
        self.queue.add_event( self.event )
        assert self.queue.pop() is self.event

    def test_pop_many_jobs(self):
        for event in self.events:
            self.queue.add_event(event)
        for event in self.events:
            assert self.queue.pop() is event

    def test_pop_empty(self):
        self.assertRaises(prototype.EventQueue.EmptyQueue, self.queue.pop)

    def test_empty_true(self):
        self.failUnless( self.queue._empty )

    def test_empty_false(self):
        self.queue.add_event( self.event )
        self.failIf( self.queue._empty )

    def test_add_handler_sanity(self):
        self.queue.add_handler(prototype.JobEvent, self.handler)
        self.queue.add_event(self.event)
        self.failIf( self.handler.called )

    def test_add_handler_called(self):
        self.queue.add_handler(prototype.JobEvent, self.handler)
        self.queue.add_event(self.event)
        self.queue.advance()

        self.failUnless( self.handler.called )

    def test_get_event_handlers_empty(self):
        self.assertEqual(
            0, len(self.queue._get_event_handlers( prototype.JobEvent ))
        )

    def test_get_event_handlers_empty(self):
        self.queue.add_handler(prototype.JobEvent, self.handler)
        self.assertEqual(
            1, len(self.queue._get_event_handlers( prototype.JobEvent ))
        )


if __name__ == "__main__":
    try:
        from testoob import main
    except ImportError:
        print "Can't find Testoob, using unittest"
        from unittest import main
    main()
