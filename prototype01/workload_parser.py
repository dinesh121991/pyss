#! /usr/bin/env python2.4

# A parser for parallel workloads in the Standard Workload Format.
#
# Information on the format and sample workloads are available at
# the Parallel Workloads Archive:
#
# http://www.cs.huji.ac.il/labs/parallel/workload/

class Job(object):
    def __init__(self, line):
        self.fields = line.split()
        assert len(self.fields) == 18

    # lazy access as properties, for efficiency
    @property
    def number(self):
        return int(self.fields[0])
    @property
    def submit_time(self):
        return int(self.fields[1])
    @property
    def wait_time(self):
        return int(self.fields[2])
    @property
    def run_time(self):
        return float(self.fields[3])
    @property
    def num_allocated_processors(self):
        return int(self.fields[4])
    @property
    def average_cpu_time_used(self):
        return int(self.fields[5])
    @property
    def used_memory(self):
        return int(self.fields[6])
    @property
    def num_requested_processors(self):
        return int(self.fields[7])
    @property
    def requested_time(self):
        return int(self.fields[8])
    @property
    def requested_memory(self):
        return int(self.fields[9])
    @property
    def status(self):
        return int(self.fields[10]) # TODO: parse into different meanings
    @property
    def user_id(self):
        return int(self.fields[11])
    @property
    def group_id(self):
        return int(self.fields[12])
    @property
    def executable_number(self):
        return int(self.fields[13])
    @property
    def queue_number(self):
        return int(self.fields[14])
    @property
    def partition_number(self):
        return int(self.fields[15])
    @property
    def preceding_job_number(self):
        return int(self.fields[16])
    @property
    def think_time_from_preceding_job(self):
        return int(self.fields[17])

    def __str__(self):
        return "Job<number=%s>" % self.number

def parse_lines(lines_iterator):
    "returns an iterator of Job objects"

    def _is_comment(line):
        return line.lstrip().startswith(';')

    return (
        Job(line)
        for line in lines_iterator
        if not _is_comment(line)
    )

def _measure_performance():
    import sys
    import time
    print "reading from stdin"
    start_time = time.time()
    jobs = parse_lines(sys.stdin)
    counter = 0
    for job in jobs:
        counter += 1
    end_time = time.time()
    total_time = end_time - start_time
    print "no. of jobs:", counter
    print "total time (seconds):", total_time
    print "jobs per second: %3.1f" % (float(counter) / total_time)

def _test():
    job = Job("   59    26613      0    716   32     -1    -1   -1     -1    -1 -1   4   1   3  0 -1 -1 -1")
    assert str(job).startswith("Job")
    assert job.number == 59

if __name__ == "__main__":
    import optparse

    parser = optparse.OptionParser(usage="%prog <test/performance>")
    options, args = parser.parse_args()
    if len(args) == 0: parser.error("no action given")

    action = args[0]

    if action == "test":
        _test()
    elif action == "performance":
        _measure_performance()
    else:
        parser.error("unknown action '%s'" % action)
