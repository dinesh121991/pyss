#! /usr/bin/env python2.4
import sys
import time

from base.event_queue import EventQueue
from base.prototype import Simulator, Machine, StupidScheduler, parse_job_lines_quick_and_dirty
from base import workload_parser

from fcfs_scheduler import FcfsScheduler

if __debug__:
    import warnings
    warnings.warn("Running in debug mode, this will be slow... try 'python2.4 -O %s'" % sys.argv[0])

def main():
    print "Reading input from stdin..."

    event_queue = EventQueue()
    machine = Machine(num_processors=10000, event_queue=event_queue)
    scheduler = FcfsScheduler(event_queue=event_queue, machine=machine)

    simulator = Simulator(
        job_source = parse_job_lines_quick_and_dirty(sys.stdin),
        event_queue = event_queue,
        machine = machine,
        scheduler = scheduler,
    )

    simulator.run()

if __name__ == "__main__":
    main()
