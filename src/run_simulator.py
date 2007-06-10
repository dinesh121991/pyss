#! /usr/bin/env python2.4
import sys
import time

from base.event_queue import EventQueue
from base.prototype import Machine, StupidScheduler, _job_inputs_to_jobs
from base import workload_parser

from schedulers.simulator import Simulator

from fcfs_scheduler import FcfsScheduler

if __debug__:
    import warnings
    warnings.warn("Running in debug mode, this will be slow... try 'python2.4 -O %s'" % sys.argv[0])

def main():
    print "Reading input from stdin..."

    event_queue = EventQueue()
    scheduler = FcfsScheduler(event_queue=event_queue, num_processors=num_processors)

    simulator = Simulator(
        jobs = _job_inputs_to_jobs(workload_parser.parse_lines(sys.stdin)),
        event_queue = event_queue,
        num_processors = 1000,
        scheduler = scheduler,
    )

    simulator.run()

if __name__ == "__main__":
    main()
