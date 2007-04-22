#! /usr/bin/env python

class JobEvent(object):
    def __init__(self, timestamp, job):
        self.timestamp = timestamp
        self.job = job

    def __repr__(self):
        return type(self).__name__ + "<timestamp=%(timestamp)s, job=%(job)s>" % vars(self)

    def __cmp__(self, other):
        "compare by timestamp first, job second"
        return cmp((self.timestamp, self.job), (other.timestamp, other.job))

class JobSubmitEvent(JobEvent): pass
class JobStartEvent(JobEvent): pass
class JobEndEvent(JobEvent): pass

class Job(object):
    def __init__(self,
            id, estimated_run_time, actual_run_time, num_required_processors
        ):
        assert num_required_processors > 0
        assert actual_run_time > 0
        assert estimated_run_time > 0
        self.id = id
        self.estimated_run_time = estimated_run_time
        self.actual_run_time = actual_run_time
        self.num_required_processors = num_required_processors

    def __str__(self):
        return "Job<id=%s>" % self.id

class StupidScheduler(object):
    "A very simple scheduler - schedules jobs one after the other with no chance of overlap"
    def __init__(self, event_queue):
        self.event_queue = event_queue
        self.next_free_time = 0

    def job_submitted(self, event):
        assert type(event) == JobSubmitEvent
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
        self.event_queue.add_handler(JobEndEvent, self._remove_job_handler)

    def add_job(self, job, current_timestamp):
        assert job.num_required_processors <= self.free_processors
        self.jobs.add(job)
        self.event_queue.add_event(JobEndEvent(job=job, timestamp=current_timestamp+job.actual_run_time))

    def _remove_job_handler(self, event):
        assert type(event) == JobEndEvent
        self.jobs.remove(event.job)

    @property
    def free_processors(self):
        return self.num_processors - self.busy_processors

    @property
    def busy_processors(self):
        return sum(job.num_required_processors for job in self.jobs)

class Simulator(object):
    def __init__(self, job_input_source):
        from event_queue import EventQueue
        self.event_queue = EventQueue()
        self.jobs = {}

        for job_input in job_input_source:
            job = self._job_input_to_job(job_input)

            self.jobs[job.id] = job

            self.event_queue.add_event(
                    # TODO: shouldn't use the submit time, should let the scheduler decide
                    JobStartEvent(timestamp = job_input.submit_time, job = job)
                )

        self.event_queue.add_handler(JobStartEvent, self.job_started_handler)

    def _job_input_to_job(self, job_input):
        return Job(
            id = job_input.number,
            estimated_run_time = job_input.requested_time,
            actual_run_time = job_input.run_time,
            # TODO: do we want the no. of allocated processors instead of the no. requested?
            num_required_processors = job_input.num_requested_processors,
        )

    def job_started_handler(self, event):
        assert event.job.id in self.jobs
        job = self.jobs[event.job.id]
        self.event_queue.add_event(
            JobEndEvent(
                timestamp = event.timestamp + job.actual_run_time,
                job = job,
            )
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
