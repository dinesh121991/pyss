#! /usr/bin/env python2.4

# A parser for parallel workloads in the Standard Workload Format.
#
# Information on the format and sample workloads are available at
# the Parallel Workloads Archive:
#
# http://www.cs.huji.ac.il/labs/parallel/workload/

class Job(object):
    def __init__(self, line):
        fields = line.split()
        assert len(fields) == 18
        self.number = int(fields[0])
        self.submit_time = int(fields[1])
        self.wait_time = int(fields[2])
        self.run_time = float(fields[3])
        self.num_allocated_processors = int(fields[4])
        self.average_cpu_time_used = int(fields[5])
        self.used_memory = int(fields[6])
        self.num_requested_processors = int(fields[7])
        self.requested_time = int(fields[8])
        self.requested_memory = int(fields[9])
        self.status = int(fields[10]) # TODO: parse into different meanings
        self.user_id = int(fields[11])
        self.group_id = int(fields[12])
        self.executable_number = int(fields[13])
        self.queue_number = int(fields[14])
        self.partition_number = int(fields[15])
        self.preceding_job_number = int(fields[16])
        self.think_time_from_preceding_job = int(fields[17])

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
