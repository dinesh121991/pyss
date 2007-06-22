from common import CpuSnapshot
from base.prototype import JobStartEvent

from easy_scheduler import EasyBackfillScheduler

default_compare_functions = (
    lambda job_a, job_b : cmp(job_b.submit_time, job_a.submit_time),
    lambda job_a, job_b : cmp(job_a.submit_time, job_b.submit_time),
    lambda job_a, job_b : cmp(job_a.num_processors, job_b.num_processors),
    lambda job_a, job_b : cmp(job_a.estimated_run_time, job_b.estimated_run_time),
    # TODO: why don't we use this one?
    #lambda job_a, job_b : cmp(job_a.num_processors * job_a.estimated_run_time, job_b.num_processors * job_b.estimated_run_time),
)

def basic_score_function(list_of_jobs):
    val = 0.0
    for job in list_of_jobs:
         val += job.num_processors * job.estimated_run_time
    return val

class  GreedyEasyBackFillScheduler(EasyBackfillScheduler):
    def __init__(self, num_processors, compare_functions=None, score_function=None):
        super(GreedyEasyBackFillScheduler, self).__init__(num_processors)

        if compare_functions is None:
            self.compare_functions = default_compare_functions
        else:
            self.compare_functions = compare_functions

        if score_function is None:
            self.score_function = basic_score_function
        else:
            self.score_function = score_function

    def _schedule_jobs(self, current_time):
        self.unscheduled_jobs.sort(self.submit_time_compare)

        result = super(GreedyEasyBackFillScheduler, self)._schedule_jobs(current_time)

        self.unscheduled_jobs.sort(self.submit_time_compare)

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

        max_score_compare_func = self.compare_functions[0]
        max_score = 0.0
        for compare_func in self.compare_functions:
            tmp_cpu_snapshot = cpu_snapshot_copy.clone()
            tentative_list_of_jobs = []
            self.unscheduled_jobs.sort(compare_func)
            for job in self.unscheduled_jobs:
                if tmp_cpu_snapshot.canJobStartNow(job, current_time):
                    tmp_cpu_snapshot.assignJob(job, current_time)
                    tentative_list_of_jobs.append(job)

            score = self.score_function(tentative_list_of_jobs)
            if max_score < score:
                max_score = score
                max_score_compare_func = compare_func

        self.unscheduled_jobs.sort(max_score_compare_func)
        self.unscheduled_jobs.append(first_job)

    def submit_time_compare(self, job_a, job_b):
        return cmp(job_a.submit_time, job_b.submit_time)

    def print_waiting_list(self):
        for job in self.unscheduled_jobs:
            print job
        print
