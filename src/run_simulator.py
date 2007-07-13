#! /usr/bin/env python2.4

import sys
if __debug__:
    import warnings
    warnings.warn("Running in debug mode, this will be slow... try 'python2.4 -O %s'" % sys.argv[0])

from base.workload_parser import parse_lines
from base.prototype import _job_inputs_to_jobs
from schedulers.simulator import run_simulator

def main():
    input_filename = sys.argv[1]
    print "Reading %r..." % input_filename

    NUM_PROCESSORS = 100 # TODO: take from input instead of magic number

    input_file = open(input_filename)
    try:
        run_simulator(
                num_processors = NUM_PROCESSORS,
                jobs = _job_inputs_to_jobs(parse_lines(input_file)),
                scheduler = MauiScheduler(NUM_PROCESSORS)
            )

        print "done."

    finally:
        input_file.close()

if __name__ == "__main__":
    main()
