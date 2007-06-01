from schedulers.common import CpuSnapshot
from base.prototype import JobSubmitEvent, JobStartEvent, JobEndEvent

class FcfsScheduler(object):
    def __init__(self, event_queue, num_processors):
        self.event_queue = event_queue
        self.cpu_snapshot = CpuSnapshot(num_processors)
        self.waiting_queue_of_jobs = []

        # register event handlers
        self.event_queue.add_handler(JobSubmitEvent, self.handleArrivalOfJobEvent)
        self.event_queue.add_handler(JobEndEvent, self.handleTerminationOfJobEvent)
        
    def handleArrivalOfJobEvent(self, event):
        self.cpu_snapshot.archive_old_slices(event.timestamp)
        self.waiting_queue_of_jobs.append(event.job)
        return self._schedule_jobs(event.timestamp)

    def handleTerminationOfJobEvent(self, event):
        self.cpu_snapshot.archive_old_slices(event.timestamp)
        self.cpu_snapshot.delTailofJobFromCpuSlices(event.job)
        return self._schedule_jobs(event.timestamp)
 
    def _schedule_jobs(self, current_time):
        first_failure_has_not_occured = True
        while len(self.waiting_queue_of_jobs) > 0 and first_failure_has_not_occured:
            job = self.waiting_queue_of_jobs[0]
            earliest_possible_time = self.cpu_snapshot.jobEarliestAssignment(job, current_time)
            if earliest_possible_time == current_time:
                del self.waiting_queue_of_jobs[0]
                self.cpu_snapshot.assignJob(job, current_time)     

                self.event_queue.add_event( JobStartEvent(timestamp=current_time, job=job) )
            else:
                first_failure_has_not_occured = False
