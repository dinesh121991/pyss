#! /usr/bin/env python

class JobEvent(object):
    def __init__(self, timestamp, job):
        self.timestamp = timestamp
        self.job = job

    def __repr__(self):
        return type(self).__name__ + "<timestamp=%(timestamp)s, job=%(job)s>" % vars(self)

    def __cmp__(self, other):
        "Compare by timestamp first, job second. Also ensure only same types are equal."
        return cmp((self.timestamp, self.job, type(self)), (other.timestamp, other.job, type(other)))

class JobSubmitEvent(JobEvent): pass
class JobStartEvent(JobEvent): pass
class JobEndEvent(JobEvent): pass

class Job(object):
    def __init__(self, id, estimated_run_time, actual_run_time, num_required_processors, \
            submit_time=0, admin_QoS=0, user_QoS=0): # TODO: are these defaults used?
        
        assert num_required_processors > 0, "job_id=%s"%id
        assert actual_run_time > 0, "job_id=%s"%id
        assert estimated_run_time > 0, "job_id=%s"%id
        
        self.id = id
        self.estimated_run_time = estimated_run_time
        self.actual_run_time = actual_run_time
        self.num_required_processors = num_required_processors

        # not used by base
        self.submit_time = submit_time # Assumption: submission time is greater than zero 
        self.start_to_run_at_time = -1 

        # the next are essentially for the MauiScheduler
        self.admin_QoS = admin_QoS # the priority given by the system administration  
        self.user_QoS = user_QoS # the priority given by the user
        self.maui_bypass_counter = 0
        self.maui_timestamp = 0

    def __str__(self):
        return "job_id=" + str(self.id) + ", submit_time=" + str(self.submit_time) + \
               ", dur=" + str(self.estimated_run_time) + ",act_dur=" + str(self.actual_run_time) + \
               ", #num_required_processors=" + str(self.num_required_processors) + \
               ", startTime=" + str(self.start_to_run_at_time)  
    
class StupidScheduler(object):
    "A very simple scheduler - schedules jobs one after the other with no chance of overlap"
    def __init__(self, event_queue):
        self.event_queue = event_queue
        self.next_free_time = None
        self.event_queue.add_handler(JobSubmitEvent, self.job_submitted)

    def job_submitted(self, event):
        assert type(event) == JobSubmitEvent

        # init next_free_time to the first timestamp seen
        if self.next_free_time is None:
            self.next_free_time = event.timestamp

        self.event_queue.add_event(
            JobStartEvent(timestamp=self.next_free_time, job=event.job)
        )
        self.next_free_time += event.job.estimated_run_time

class Machine(object):
    "Represents the actual parallel machine ('cluster')"
    def __init__(self, num_processors, event_queue):
        self.num_processors = num_processors
        self.event_queue = event_queue
        self.jobs = set()
        self.event_queue.add_handler(JobStartEvent, self._start_job_handler)
        self.event_queue.add_handler(JobEndEvent, self._remove_job_handler)

    def add_job(self, job, current_timestamp):
        assert job.num_required_processors <= self.free_processors
        self.jobs.add(job)
        self.event_queue.add_event(JobEndEvent(job=job, timestamp=current_timestamp+job.actual_run_time))

    def _remove_job_handler(self, event):
        assert type(event) == JobEndEvent
        self.jobs.remove(event.job)

    def _start_job_handler(self, event):
        assert type(event) == JobStartEvent
        self.add_job(event.job, event.timestamp)

    @property
    def free_processors(self):
        return self.num_processors - self.busy_processors

    @property
    def busy_processors(self):
        return sum(job.num_required_processors for job in self.jobs)

def parse_job_lines_quick_and_dirty(lines):
    """
    parses lines in Standard Workload Format, yielding pairs of (submit_time, <Job instance>)

    This should have been:

      for job_input in workload_parser.parse_lines(lines):
        yield job_input.submit_time, Simulator._job_input_to_job(job_input)

    But instead everything is hard-coded (also hard to read and modify) for
    performance reasons.

    Pay special attention to the indices and see that you're using what you
    expect, check out the workload_parser.JobInput properties and
    Simulator._job_input_to_job comments to see extra logic that isn't
    represented here.
    """
    for line in lines:
        x = line.split()
        yield int(x[1]), Job(
            id = int(x[0]),
            estimated_run_time = int(x[8]),
            actual_run_time = int(x[3]),
            num_required_processors = max(int(x[7]), int(x[4])), # max(num_requested,max_allocated)
        )

class Simulator(object):
    def __init__(self, job_source, event_queue, machine, scheduler):
        self.event_queue = event_queue
        self.machine = machine
        self.scheduler = scheduler

        for submit_time, job in job_source:
            self.event_queue.add_event(
                    JobSubmitEvent(timestamp = submit_time, job = job)
                )

    @staticmethod
    def _job_input_to_job(job_input):
        return Job(
            id = job_input.number,
            estimated_run_time = job_input.requested_time,
            actual_run_time = job_input.run_time,
            # TODO: do we want the no. of allocated processors instead of the no. requested?
            num_required_processors = job_input.num_requested_processors,
        )

    def run(self):
        while not self.event_queue.empty:
            self.event_queue.advance()

def simple_job_generator(num_jobs):
    import random
    start_time = 0
    for id in xrange(num_jobs):
        start_time += random.randrange(0, 15)
        yield start_time, Job(
            id=id,
            estimated_run_time=random.randrange(400, 2000),
            actual_run_time=random.randrange(30, 1000),
            num_required_processors=random.randrange(2,100),
        )
