from common import CpuSnapshot
from base.prototype import JobStartEvent

from easy_scheduler import EasyBackfillScheduler

default_sort_key_functions = (
    lambda job : -job.submit_time, # sort by reverse submission time
    lambda job : job.submit_time,
    lambda job : job.num_processors,
    lambda job : job.estimated_run_time,
    # TODO: why don't we use this one?
    #lambda job : job.num_processors * job.estimated_run_time,
)

def basic_score_function(list_of_jobs):
    return sum(job.num_processors * job.estimated_run_time for job in list_of_jobs)

class  GreedyEasyBackFillScheduler(EasyBackfillScheduler):
    def __init__(self, num_processors, sort_key_functions=None, score_function=None):
        super(GreedyEasyBackFillScheduler, self).__init__(num_processors)

        if sort_key_functions is None:
            self.sort_key_functions = default_sort_key_functions
        else:
            self.sort_key_functions = sort_key_functions

        if score_function is None:
            self.score_function = basic_score_function
        else:
            self.score_function = score_function

    def _schedule_jobs(self, current_time):
        self.unscheduled_jobs.sort(key = self._submit_job_sort_key)

        result = super(GreedyEasyBackFillScheduler, self)._schedule_jobs(current_time)

        self.unscheduled_jobs.sort(key = self._submit_job_sort_key)

        return result

    def _backfill_jobs(self, current_time):
        "Overriding parent method"
        self._find_an_approximate_best_order_of_the_jobs(current_time)
        return super(GreedyEasyBackFillScheduler, self)._backfill_jobs(current_time)

    def canBeBackfilled(self, job, current_time):
        "Overriding parent method"
        return self.cpu_snapshot.canJobStartNow(job, current_time)

    def _find_an_approximate_best_order_of_the_jobs(self, current_time):
        if len(self.unscheduled_jobs) == 0:
            return

        first_job = self.unscheduled_jobs.pop(0) ## +

        cpu_snapshot_copy = self.cpu_snapshot.copy()

        cpu_snapshot_copy.assignJobEarliest(first_job, current_time)

        max_score_sort_key_func = self.sort_key_functions[0]
        max_score = 0.0
        for sort_key_func in self.sort_key_functions:
            tmp_cpu_snapshot = cpu_snapshot_copy.copy()
            tentative_list_of_jobs = []
            self.unscheduled_jobs.sort(key=sort_key_func)
            for job in self.unscheduled_jobs:
                if tmp_cpu_snapshot.canJobStartNow(job, current_time):
                    tmp_cpu_snapshot.assignJob(job, current_time)
                    tentative_list_of_jobs.append(job)

            score = self.score_function(tentative_list_of_jobs)
            if max_score < score:
                max_score = score
                max_score_sort_key_func = sort_key_func

        self.unscheduled_jobs.sort(key=max_score_sort_key_func)
        self.unscheduled_jobs.append(first_job)

    def _submit_job_sort_key(self, job):
        return job.submit_time

    def print_waiting_list(self):
        for job in self.unscheduled_jobs:
            print job
        print
