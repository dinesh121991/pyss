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
        return int(fields[0])
    @property
    def submit_time(self):
        return int(fields[1])
    @property
    def wait_time(self):
        return int(fields[2])
    @property
    def run_time(self):
        return float(fields[3])
    @property
    def num_allocated_processors(self):
        return int(fields[4])
    @property
    def average_cpu_time_used(self):
        return int(fields[5])
    @property
    def used_memory(self):
        return int(fields[6])
    @property
    def num_requested_processors(self):
        return int(fields[7])
    @property
    def requested_time(self):
        return int(fields[8])
    @property
    def requested_memory(self):
        return int(fields[9])
    @property
    def status(self):
        return int(fields[10]) # TODO: parse into different meanings
    @property
    def user_id(self):
        return int(fields[11])
    @property
    def group_id(self):
        return int(fields[12])
    @property
    def executable_number(self):
        return int(fields[13])
    @property
    def queue_number(self):
        return int(fields[14])
    @property
    def partition_number(self):
        return int(fields[15])
    @property
    def preceding_job_number(self):
        return int(fields[16])
    @property
    def think_time_from_preceding_job(self):
        return int(fields[17])

    def __str__(self):
        fields_str = ", ".join([
            field_name + " = " + str(getattr(self, field_name))
            for field_name in (
                "number",
                "submit_time",
                "wait_time",
                "run_time",
                "num_allocated_processors",
                "average_cpu_time_used",
                "used_memory",
                "num_requested_processors",
                "requested_time",
                "requested_memory",
                "status",
                "user_id",
                "group_id",
                "executable_number",
                "queue_number",
                "partition_number",
                "preceding_job_number",
                "think_time_from_preceding_job",
            )
        ])
        return "Jobs: " + fields_str

def parse_lines(lines_iterator):
    "returns an iterator of Job objects"

    def _is_comment(line):
        return line.lstrip().startswith(';')

    return (
        Job(line)
        for line in lines_iterator
        if not _is_comment(line)
    )

if __name__ == "__main__":
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
