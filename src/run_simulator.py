#! /usr/bin/env python2.4

import sys
if __debug__:
    import warnings
    #warnings.warn("Running in debug mode, this will be slow... try 'python2.4 -O %s'" % sys.argv[0])

from base.workload_parser import parse_lines
from base.prototype import _job_inputs_to_jobs
from schedulers.simulator import run_simulator
import optparse

from schedulers.fcfs_scheduler import FcfsScheduler

from schedulers.conservative_scheduler import ConservativeScheduler
from schedulers.double_conservative_scheduler import DoubleConservativeScheduler

from schedulers.easy_scheduler import EasyBackfillScheduler
from schedulers.double_easy_scheduler import DoubleEasyBackfillScheduler
from schedulers.greedy_easy_scheduler import GreedyEasyBackFillScheduler
from schedulers.easy_plus_plus_scheduler import EasyPlusPlusScheduler
from schedulers.lookahead_easy_scheduler import LookAheadEasyBackFillScheduler
from schedulers.shrinking_easy_scheduler import ShrinkingEasyScheduler


def parse_options():
    parser = optparse.OptionParser()
    parser.add_option("--num-processors", type="int")
    parser.add_option("--input-file")

    options, args = parser.parse_args()

    if options.num_processors is None:
        parser.error("missing num processors")

    if options.input_file is None:
        parser.error("missing input file")

    if args:
        parser.error("unknown extra arguments: %s" % args)

    return options

def main():
    options = parse_options()

    input_file = open(options.input_file)
    
    scheduler =  LookAheadEasyBackFillScheduler(options.num_processors)
    try:
        run_simulator(
                num_processors = options.num_processors,
                jobs = _job_inputs_to_jobs(parse_lines(input_file)),
                scheduler = scheduler 
            )
        
        print "Num of Processors: ", options.num_processors
        print "Input file: ", options.input_file
        print "Scheduler:", type(scheduler)
    finally:
        input_file.close()

if __name__ == "__main__":
    main()
