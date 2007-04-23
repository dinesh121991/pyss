#! /usr/bin/env python2.4
import sys
import time

from event_queue import EventQueue
from prototype import Simulator, Machine, StupidScheduler
import workload_parser

def main():
    print "Reading input from stdin..."

    job_inputs = workload_parser.parse_lines( sys.stdin )
    event_queue = EventQueue()
    machine = Machine(num_processors=10000, event_queue=event_queue)
    scheduler = StupidScheduler(event_queue)

    simulator = Simulator(
        job_inputs,
        event_queue = event_queue,
        machine = machine,
        scheduler = scheduler,
    )

    simulator.run()

if __name__ == "__main__":
    main()
