#! /usr/bin/env python2.4

import sys
if __debug__:
    import warnings
    #warnings.warn("Running in debug mode, this will be slow... try 'python2.4 -O %s'" % sys.argv[0])

from base.workload_parser import parse_lines
from base.prototype import _job_inputs_to_jobs
from schedulers.simulator import run_simulator
from schedulers.easy_scheduler import EasyBackfillScheduler

import optparse

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

    print "num_processors =", options.num_processors
    print "Reading %r..." % options.input_file

    input_file = open(options.input_file)
    try:
        run_simulator(
                num_processors = options.num_processors,
                jobs = _job_inputs_to_jobs(parse_lines(input_file)),
                scheduler = EasyBackfillScheduler(options.num_processors)
            )

        print "done."

    finally:
        input_file.close()

if __name__ == "__main__":
    main()
