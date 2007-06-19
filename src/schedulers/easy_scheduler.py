from common import Scheduler, CpuSnapshot
from base.prototype import JobStartEvent

class EasyBackfillScheduler(Scheduler):

    def __init__(self, num_processors):
        Scheduler.__init__(self, num_processors)
        self.cpu_snapshot = CpuSnapshot(num_processors)
        self.waiting_list_of_unscheduled_jobs = []

    def handleSubmissionOfJobEvent(self, just_submitted_job, current_time):
        """ Here we first add the new job to the waiting list. We then try to schedule
        the jobs in the waiting list, returning a collection of new termination events """
        self.cpu_snapshot.archive_old_slices(current_time)
        self.waiting_list_of_unscheduled_jobs.append(just_submitted_job)
        newEvents = []
        if len(self.waiting_list_of_unscheduled_jobs) == 1:
            start_time = self.cpu_snapshot.jobEarliestAssignment(just_submitted_job, current_time)
            if start_time == current_time:
                self.waiting_list_of_unscheduled_jobs = []
                self.cpu_snapshot.assignJob(just_submitted_job, current_time)
                newEvents.append( JobStartEvent(current_time, just_submitted_job) )
        else: # there are at least 2 jobs in the waiting list
            if self.canBeBackfilled(just_submitted_job, current_time):
                    self.waiting_list_of_unscheduled_jobs.pop()
                    self.cpu_snapshot.assignJob(just_submitted_job, current_time)
                    newEvents.append( JobStartEvent(current_time, just_submitted_job) )
        return newEvents

    def handleTerminationOfJobEvent(self, job, current_time):
        """ Here we first delete the tail of the just terminated job (in case it's
        done before user estimation time). We then try to schedule the jobs in the waiting list,
        returning a collection of new termination events """
        self.cpu_snapshot.archive_old_slices(current_time)
        self.cpu_snapshot.delTailofJobFromCpuSlices(job)
        return self._schedule_jobs(current_time)

    def _schedule_jobs(self, current_time):
        if len(self.waiting_list_of_unscheduled_jobs) == 0:
            return []

        newEvents = self._schedule_the_head_of_the_waiting_list(current_time)
        newEvents += self._schedule_the_tail_of_the_waiting_list(current_time)
        return newEvents

    def _schedule_the_head_of_the_waiting_list(self, current_time):
        result = []
        while len(self.waiting_list_of_unscheduled_jobs) > 0:
            first_job = self.waiting_list_of_unscheduled_jobs[0]
            start_time_of_first_job = self.cpu_snapshot.jobEarliestAssignment(first_job, current_time)
            if start_time_of_first_job == current_time:
                self.waiting_list_of_unscheduled_jobs.pop(0)
                self.cpu_snapshot.assignJob(first_job, current_time)
                result.append( JobStartEvent(current_time, first_job) )
            else:
                break
        return result

    def _backfill_jobs(self, current_time):
        """
        Find jobs that can be backfilled and update the cpu snapshot.
        """
        result = []
        for job in self.waiting_list_of_unscheduled_jobs:
            if self.canBeBackfilled(job, current_time):
                result.append(job)
                self.cpu_snapshot.assignJob(job, current_time)

        for job in result:
            self.waiting_list_of_unscheduled_jobs.remove(job)

        return result

    def _schedule_the_tail_of_the_waiting_list(self, current_time):
        if len(self.waiting_list_of_unscheduled_jobs) <= 1:
            return []

        backfilled_jobs = self._backfill_jobs(current_time)

        return [
            JobStartEvent(current_time, job)
            for job in backfilled_jobs
        ]

    def canBeBackfilled(self, second_job, current_time):

        first_job = self.waiting_list_of_unscheduled_jobs[0]

        if self.cpu_snapshot.free_processors_available_at(current_time) < second_job.num_required_processors:
            return False

        shadow_time = self.cpu_snapshot.jobEarliestAssignment(first_job, current_time)
        self.cpu_snapshot.assignJob(first_job, shadow_time)
        start_time_of_2nd_if_1st_job_is_assigned = self.cpu_snapshot.jobEarliestAssignment(second_job, current_time)
        self.cpu_snapshot.delJobFromCpuSlices(first_job)

        if start_time_of_2nd_if_1st_job_is_assigned == current_time:
            return True # this means that the 2nd is "independent" of the 1st, and thus can be backfilled
        else:
            return False

