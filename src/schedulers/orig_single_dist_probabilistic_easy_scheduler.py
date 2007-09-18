from common import Scheduler, CpuSnapshot, list_copy
from base.prototype import JobStartEvent, Job
from math import log

    
class Distribution(object):
    def __init__(self, job):
        self.bins    = {}
        self.bins[1] = 1 # adding the first entry to the main data structure of the distribution 
        self.number_of_jobs_added = 1

        if job is not None: # we init the distribution to be uniform w.r.t. to the user estimation 
            self.touch(job.user_estimated_run_time)
            

    def touch(self, time): # just add bins.
        rounded_up_time = pow(2, int(log(max(2 * time - 1, 1), 2)))
       	while rounded_up_time > 1: 
            if not self.bins.has_key(rounded_up_time) or self.bins[rounded_up_time] == 0:  
                self.bins[rounded_up_time] = 1  
		self.number_of_jobs_added += 1
            rounded_up_time = rounded_up_time / 2
            

    def empty_touch(self, time): # just add bins.
        rounded_up_time = pow(2, int(log(max(2 * time - 1, 1) , 2))) 
       	while rounded_up_time > 1: 
            if not self.bins.has_key(rounded_up_time):  
                self.bins[rounded_up_time] = 0  
            rounded_up_time = rounded_up_time / 2

            
    def add_job(self, job): #to be called when a termination event has occured
        assert job.actual_run_time > 0
        
        rounded_up_run_time = pow(2, int(log(max(2 * job.actual_run_time - 1, 1), 2)))
        self.number_of_jobs_added += 1
        
        if self.bins.has_key(rounded_up_run_time):
            self.bins[rounded_up_run_time] += 1 # incrementing the numbers of terminated jobs encountered so far
        else: 
            self.bins[rounded_up_run_time]  = 1   # we add a new entry initialized to 1

        
            
        
class  OrigSingleDistProbabilisticEasyScheduler(Scheduler):
    """ This algorithm implements a version of Feitelson and Nissimov, June 2007
        In this simplified version we have single distribution for all the jobs and users 
    """
    
    def __init__(self, num_processors, threshold = 0.2):
        super(OrigSingleDistProbabilisticEasyScheduler, self).__init__(num_processors)
        self.threshold = threshold
        self.cpu_snapshot = CpuSnapshot(num_processors)
        self.distribution = Distribution(Job(1,1,1,1,1)) 
        self.unscheduled_jobs  = []
        self.currently_running_jobs = []
     
    
    
    def new_events_on_job_submission(self, job, current_time):
        # print "arrived:", job
        self.cpu_snapshot.archive_old_slices(current_time)
        self.unscheduled_jobs.append(job)
        return [
            JobStartEvent(current_time, job)
            for job in self._schedule_jobs(current_time)
        ]


    def new_events_on_job_termination(self, job, current_time):
        self.distribution.add_job(job)
        self.currently_running_jobs.remove(job)
        self.cpu_snapshot.archive_old_slices(current_time)
        self.cpu_snapshot.delTailofJobFromCpuSlices(job)
        return [
            JobStartEvent(current_time, job)
            for job in self._schedule_jobs(current_time)
        ]


    def _schedule_jobs(self, current_time):
        "Schedules jobs that can run right now, and returns them"
        jobs  = self._schedule_head_of_list(current_time)
        jobs += self._backfill_jobs(current_time)
        return jobs


    def _schedule_head_of_list(self, current_time):     
        result = []
        while True:
            if len(self.unscheduled_jobs) == 0:
                break
            # Try to schedule the first job
            if self.cpu_snapshot.free_processors_available_at(current_time) >= self.unscheduled_jobs[0].num_required_processors:
                job = self.unscheduled_jobs.pop(0)
                self.currently_running_jobs.append(job)
                self.cpu_snapshot.assignJob(job, current_time)
                result.append(job)
            else:
                # first job can't be scheduled
                break
        return result
    

    def _backfill_jobs(self, current_time):
        if len(self.unscheduled_jobs) <= 1:
            return []

        result    = []  
        first_job = self.unscheduled_jobs[0]        
        tail      = self.unscheduled_jobs[1:]
                
        for job in tail:
            if self.can_be_probabilistically_backfilled(job, current_time):
                # print "+++ job can be backfilled", job
                self.unscheduled_jobs.remove(job)
                self.currently_running_jobs.append(job)
                self.cpu_snapshot.assignJob(job, current_time)
                result.append(job)
                
        return result


    def can_be_probabilistically_backfilled(self, job, current_time):
        assert len(self.unscheduled_jobs) >= 2
        assert job in self.unscheduled_jobs[1:]

        if self.cpu_snapshot.free_processors_available_at(current_time) < job.num_required_processors:
            return False

        first_job = self.unscheduled_jobs[0]
        self.distribution.touch(job.user_estimated_run_time)
      
        prediction  = 0.0
        max_bottle_neck = 0.0 
        bottle_neck = 0.0 
        t = 1

        C = first_job.num_required_processors + job.num_required_processors
        if C <= self.num_processors:
	    flag = 1
	else: 
	    flag = 2

	if flag == 1:         
            while t <= max(2*job.user_estimated_run_time-1,1):
            	job_probability_to_end_at_t = self.probability_to_end_at(t, job)
            	max_bottle_neck = max(max_bottle_neck, self.bottle_neck(t, job, first_job, current_time, flag))
            	prediction += job_probability_to_end_at_t * max_bottle_neck
	    	# print "t is:", t
            	t = t * 2 
    
            if prediction <= self.threshold:
            	# print "prediction:", prediction
            	return True
	else: 
            while t <= max(2*job.user_estimated_run_time-1, 1):
            	job_probability_to_end_at_t = self.probability_to_end_at(t, job)
            	bottle_neck = self.bottle_neck(t, job, first_job, current_time, flag)
            	prediction += job_probability_to_end_at_t * bottle_neck
            	t = t * 2 

            if prediction >= 1 - self.threshold:
            	return True        
        return False
        

    def bottle_neck(self, time, second_job, first_job, current_time, flag):
        result = 0.0
        M = {}
        C = first_job.num_required_processors + second_job.num_required_processors
        
        # M[n,c] denotes the probablity that at time the first n jobs among those that
        # are currently running have released at least c processors
        # print ">>> in bottle neck, current time is:", current_time

        for c in range(C + 1): 
            M[-1, c] = 0.0
            
        for n in range(len(self.currently_running_jobs)):
            M[n, 0] = 1.0

        for n in range(len(self.currently_running_jobs)):
            job = self.currently_running_jobs[n]
            # print "current job:", job
            Pn = self.probability_of_running_job_to_end_upto(time, current_time, job)
            # print "self.probability_of_running_job_to_end_upto", time, "is: ", Pn 
            for c in range (C + 1):
                # print "current c", c
                if c > self.num_processors:
                    M[n, c] = 0
                elif c >= job.num_required_processors:  
                    M[n, c] = M[n-1, c] + (M[n-1, c - job.num_required_processors] - M[n-1, c]) * Pn
                    # print "case 1"
                elif c >= 1:
                    M[n, c] = M[n-1, c] + (1 - M[n-1, c]) * Pn
                    # print "case 2"

                M[n, c] = float(int(1000 * M[n, c])) / 1000 # rounding step 
                
                # print "[", n, ",",  c, "]", M[n, c]

                    
        # for c in range (C + 1):
            # for n in range(len(self.currently_running_jobs)):
                # print "[", n, ",",  c, "]", M[n, c]
	if   flag == 1:  
        	result = M[n, first_job.num_required_processors] - M[n, C]
	elif flag == 2: 
		result = 1 - M[n, first_job.num_required_processors]

        if   result < 0:
            result = 0
        elif result > 1:
            result = 1

        # print ">>> TiME: ", time, "result", result  
        assert 0 <= result <= 1  
        return result 


    def probability_of_running_job_to_end_upto(self, time, current_time, job):

        rounded_down_run_time = pow(2, int(log(max(current_time - job.start_to_run_at_time, 1), 2)))
        rounded_up_estimated_remaining_duration = pow(2, int(log(max(2*(job.user_estimated_run_time - rounded_down_run_time)-1, 1), 2)))
	if time >=  rounded_up_estimated_remaining_duration:
        	# print "prob job upto time:", time, "is: >>> 1"
		return 1.0

        num_of_jobs_in_first_bins  = 0
        num_of_jobs_in_middle_bins = 0.0
        num_of_jobs_in_last_bins   = 0
        job_distribution = self.distribution

        #print "in function probability_of_running_job_to_end_upto:"
        #print "---- current time, job_start", current_time, job.start_to_run_at_time, job 
        #print "---- rounded_down_run_time", rounded_down_run_time
        #print "---- rounded_up_estimated_remaining_duration", rounded_up_estimated_remaining_duration
        
        time = min(time, rounded_up_estimated_remaining_duration) 
        #print "up to time:", time
        
        for key in sorted(job_distribution.bins.keys()):
            #print "+ key, num: ", key, job_distribution.bins[key] 
            if key <= rounded_down_run_time:
                #print "case 1 key, num: ", key, job_distribution.bins[key] 
                num_of_jobs_in_first_bins += job_distribution.bins[key]

            elif key > rounded_down_run_time + rounded_up_estimated_remaining_duration:
                #print "case 4 key, num: ", key, job_distribution.bins[key] 
                num_of_jobs_in_last_bins  += job_distribution.bins[key]  

            elif key < time + rounded_down_run_time:
                #print "case 2 key, num: ", key, job_distribution.bins[key] 
                num_of_jobs_in_middle_bins += float(job_distribution.bins[key]) 
                #print "num of mid bins:", num_of_jobs_in_middle_bins

            elif key >= time + rounded_down_run_time > key / 2 :
                #print "case 3 key, num: ", key, job_distribution.bins[key] 
                num_of_jobs_in_middle_bins += float(job_distribution.bins[key] * (time + rounded_down_run_time - (key / 2))) / (key 
/ 2) 
                #print "num of mid bins:", num_of_jobs_in_middle_bins
          	
  
        num_of_irrelevant_jobs = num_of_jobs_in_first_bins + num_of_jobs_in_last_bins
        num_of_relevant_jobs = job_distribution.number_of_jobs_added - num_of_irrelevant_jobs

	result = 0.0 
	if num_of_relevant_jobs > 0: 
        	result = num_of_jobs_in_middle_bins / num_of_relevant_jobs
    
        #print "prob job upto time:", time, "is: >>>", result
        #print "num_of_jobs_in_first_bins", num_of_jobs_in_first_bins
        #print "num_of_jobs_in_middle_bins", num_of_jobs_in_middle_bins
        #print "num_of_jobs_in_last_bins", num_of_jobs_in_last_bins

        assert 0 <= result <= 1
        return result 


    def probability_to_end_at(self, time, job):         
        assert self.distribution.bins.has_key(time) == True
        
        job_distribution = self.distribution
        result = 0.0
        num_of_jobs_in_last_bins = 0
        # print "in  probability_to_end_at:"
        for key in job_distribution.bins.keys():
            #print "- key, num: ", key, job_distribution.bins[key]
            rounded_up_user_estimated_run_time = 2 * job.user_estimated_run_time - 1
            if key > rounded_up_user_estimated_run_time:
                # print "print added to last bins"
                num_of_jobs_in_last_bins  += job_distribution.bins[key]  
 
        num_of_relevant_jobs = job_distribution.number_of_jobs_added - num_of_jobs_in_last_bins

	if num_of_relevant_jobs > 0: 
        	result = float(job_distribution.bins[time]) / num_of_relevant_jobs

        #print "probability to finish at time: ", time, "is: ", result, job
        #print "num of relevant jobs: ", num_of_relevant_jobs
        #print "num_of_jobs_in_last_bins:", num_of_jobs_in_last_bins
    
        assert 0 <= result <= 1
        return result 
     
     

     
