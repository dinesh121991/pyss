#!/usr/bin/env python2.4
from unittest import TestCase

import random

import prototype

def _gen_random_timestamp_events():
    return [
        prototype.JobEvent(timestamp=random.randrange(0,100), job=str(i))
        for i in xrange(30)
    ]

def _create_handler():
    def handler(event):
        handler.called = True
    handler.called = False
    return handler

class test_Job(TestCase):
    def test_str_sanity(self):
        str(prototype.Job(None, None, None, None))

class test_JobEvent(TestCase):
    def test_str_sanity(self):
        str(prototype.JobEvent(timestamp=11, job=None))

    def test_equal(self):
        e1 = prototype.JobEvent(timestamp=10, job="abc")
        e2 = prototype.JobEvent(timestamp=10, job="abc")
        self.assertEqual(e1, e2)

    def test_nonequal_timestamp(self):
        e1 = prototype.JobEvent(timestamp=10, job="abc")
        e2 = prototype.JobEvent(timestamp=22, job="abc")
        self.assertNotEqual(e1, e2)

    def test_nonequal_job(self):
        e1 = prototype.JobEvent(timestamp=10, job="abc")
        e2 = prototype.JobEvent(timestamp=10, job="def")
        self.assertNotEqual(e1, e2)

    def test_sort_order(self):
        e1 = prototype.JobEvent(timestamp=10, job="abc")
        e2 = prototype.JobEvent(timestamp=22, job="abc")
        self.failUnless( e1 < e2 )
        self.failIf( e1 >= e2 )
        
    def test_sort_order_random(self):
        random_events = _gen_random_timestamp_events()
        sorted_events = sorted(random_events, key=lambda x:(x.timestamp, x.job))
        self.assertEqual( sorted_events, sorted(random_events) )

class test_EventQueue(TestCase):
    def setUp(self):
        self.queue = prototype.EventQueue()
        self.event = prototype.JobEvent(timestamp=0, job=None)
        self.events = [
                prototype.JobEvent(timestamp=i, job=None)
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

    def test_add_same_event_fails(self):
        self.queue.add_event(self.event)
        self.assertRaises(Exception, self.queue.add_event, self.event)

    def test_add_event_simple(self):
        for event in self.events:
            self.queue.add_event(event)
        self.assertEqual( self.events, list(self.queue._sorted_events) )

    def test_add_event_sorting(self):
        random_events = _gen_random_timestamp_events()
        for event in random_events:
            self.queue.add_event(event)
        self.assertEqual( sorted(random_events), self.queue._sorted_events )

    def test_remove_event_fails_on_empty(self):
        self.assertRaises(Exception, self.queue.remove_event, self.event)

    def test_remove_event_fails_on_missing_event(self):
        event1 = prototype.JobEvent(0, 0)
        event2 = prototype.JobEvent(0, 1)
        assert event1 != event2 # different events
        self.queue.add_event(event1)
        self.assertRaises(Exception, self.queue.remove_event, event2)

    def test_remove_event_succeeds(self):
        self.queue.add_event(self.event)
        self.queue.remove_event(self.event)
        self.failUnless( self.queue.empty )

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
        self.failUnless( self.queue.empty )

    def test_empty_false(self):
        self.queue.add_event( self.event )
        self.failIf( self.queue.empty )

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
        self.failUnless(self.queue.empty)

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
        for handler in nonmatching_handlers:
            self.queue.add_handler(prototype.JobStartEvent, handler)

        self.queue.add_event(self.event)
        self.queue.advance()

        for handler in matching_handlers:
            self.failUnless( handler.called )

        for handler in nonmatching_handlers:
            self.failIf( handler.called )

    def test_sometimes_relevant_handler(self):
        self.queue.add_handler(prototype.JobEvent, self.handler)
        self.queue.add_event(prototype.JobEvent(timestamp=0, job="x"))
        self.queue.advance()
        self.failUnless(self.handler.called)
        self.handler.called = False
        self.queue.add_event(prototype.JobStartEvent(timestamp=1, job="x"))
        self.queue.advance()
        self.failIf(self.handler.called)
        self.queue.add_event(prototype.JobEvent(timestamp=2, job="x"))
        self.queue.advance()
        self.failUnless(self.handler.called)

class test_Simulator(TestCase):
    def setUp(self):
        self.start_time_and_jobs = list(prototype.simple_job_generator(10))
        self.simulator = prototype.Simulator(self.start_time_and_jobs)
        self.jobs = set(job for start_time, job in self.start_time_and_jobs)

    def tearDown(self):
        del self.simulator

    def test_init_empty(self):
        self.assertEqual(0, len(prototype.Simulator([]).jobs))

    def test_init_jobs(self):
        self.assertEqual(self.jobs, set(self.simulator.jobs.values()))

    def test_init_event_queue(self):
        self.assertEqual(
            set(job.id for job in self.jobs), 
            set(event.job.id for event in self.simulator.event_queue._sorted_events)
        )

    def test_job_started_handler_registers_end_events(self):
        done_jobs_ids=[]
        def job_done_handler(event):
            done_jobs_ids.append(event.job.id)

        job = prototype.Job(
                id=0,
                estimated_run_time=10,
                actual_run_time=10,
                num_required_processors=1,
            )

        simulator = prototype.Simulator( job_source = ((0, job),) )
        simulator.event_queue.add_handler(prototype.JobEndEvent, job_done_handler)

        simulator.run()

        self.failUnless( job.id in done_jobs_ids )


class test_simple_job_generator(TestCase):
    def test_unique_id(self):
        previously_seen = set()
        for start_time, job in prototype.simple_job_generator(num_jobs=200):
            self.failIf( job.id in previously_seen )
            previously_seen.add( job.id )

    def test_nondescending_start_times(self):
        prev_time = 0
        for start_time, job in prototype.simple_job_generator(num_jobs=200):
            self.failUnless( start_time >= prev_time )
            prev_time = start_time

if __name__ == "__main__":
    try:
        from testoob import main
    except ImportError:
        from unittest import main
    main()
