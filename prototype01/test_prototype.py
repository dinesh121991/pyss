from unittest import TestCase

import random, copy

import prototype

def _gen_random_timestamp_events():
    return [
        prototype.JobEvent(timestamp=random.randrange(0,100), job_id=0)
        for i in xrange(30)
    ]

def _create_handler():
    def handler(event):
        handler.called = True
    handler.called = False
    return handler

class test_JobEvent(TestCase):
    def test_sort_order_random(self):
        random_events = _gen_random_timestamp_events()
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

        self.handler = _create_handler()

    def tearDown(self):
        del self.queue
        del self.event

    def test_events_empty(self):
        self.assertEqual( 0, len(list(self.queue._sorted_events)) )

    def test_add_event_sanity(self):
        self.queue.add_event( self.event )

    def test_add_event_single_event(self):
        self.queue.add_event(self.event)
        self.assertEqual( [self.event], self.queue._sorted_events )

    def test_add_event_simple(self):
        for event in self.events:
            self.queue.add_event(event)
        self.assertEqual( self.events, list(self.queue._sorted_events) )

    def test_add_event_sorting(self):
        random_events = _gen_random_timestamp_events()
        for event in random_events:
            self.queue.add_event(event)
        self.assertEqual( sorted(random_events), self.queue._sorted_events )

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

    def test_get_event_handlers_empty(self):
        self.assertEqual(
            0, len(self.queue._get_event_handlers( prototype.JobEvent ))
        )

    def test_get_event_handlers_nonempty(self):
        self.queue.add_handler(prototype.JobEvent, self.handler)
        self.assertEqual(
            1, len(self.queue._get_event_handlers( prototype.JobEvent ))
        )

    def test_advance_empty_queue(self):
        self.assertRaises(prototype.EventQueue.EmptyQueue, self.queue.advance)

    def test_advance_eats_event(self):
        self.queue.add_event(self.event)
        self.queue.advance()
        self.failUnless(self.queue._empty)

    def test_advance_one_handler_handles(self):
        self.queue.add_handler(prototype.JobEvent, self.handler)
        self.queue.add_event(self.event)
        self.queue.advance()

        self.failUnless( self.handler.called )

    def test_advance_one_handler_doesnt_handle(self):
        self.queue.add_handler(prototype.JobStartEvent, self.handler)
        self.queue.add_event(self.event) # JobEvent, different type
        self.queue.advance()

        self.failIf( self.handler.called )

    def test_advance_many_handlers(self):
        matching_handlers = [ _create_handler() for i in xrange(5) ]
        nonmatching_handlers = [ _create_handler() for i in xrange(5) ]

        # register handlers that should run
        for handler in matching_handlers:
            self.queue.add_handler(prototype.JobEvent, handler)

        # register handlers that shouldn't run with a different event type
        for handlers in nonmatching_handlers:
            self.queue.add_handler(prototype.JobStartEvent, handler)

        self.queue.add_event(self.event)
        self.queue.advance()

        for handler in matching_handlers:
            self.failUnless( handler.called )

        for handler in nonmatching_handlers:
            self.failIf( handler.called )

class test_simple_job_generator(TestCase):
    def test_unique_id(self):
        previously_seen = set()
        for job in prototype.simple_job_generator(num_jobs=200):
            self.failIf( job.id in previously_seen )
            previously_seen.add( job.id )

if __name__ == "__main__":
    try:
        from testoob import main
    except ImportError:
        print "Can't find Testoob, using unittest"
        from unittest import main
    main()
