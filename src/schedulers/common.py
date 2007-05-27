import sys

class Job:
    def __init__(self, job_id, estimated_run_time, job_nodes, \
                 job_arrival_time=0, job_actual_run_time=0, job_admin_QoS=0, job_user_QoS=0):
        
        #assert job_nodes > 0
        #assert job_actual_run_time >= 0
        #assert estimated_run_time >= job_actual_run_time
        #assert job_arrival_time >= 0
        
        self.id = job_id
        self.estimated_run_time = estimated_run_time
        self.num_required_processors = job_nodes
        self.arrival_time = job_arrival_time # Assumption: arrival time is greater than zero 
        self.start_to_run_at_time = -1 
        self.actual_run_time = job_actual_run_time

        # the next are essentially for the MauiScheduler
        self.admin_QoS = job_admin_QoS # the priority given by the system administration  
        self.user_QoS = job_user_QoS # the priority given by the user
        self.maui_bypass_counter = 0
        self.maui_timestamp = 0
        

    def __str__(self):
        return "job_id=" + str(self.id) + ", arrival=" + str(self.arrival_time) + \
               ", dur=" + str(self.estimated_run_time) + ",act_dur=" + str(self.actual_run_time) + \
               ", #num_required_processors=" + str(self.num_required_processors) + \
               ", startTime=" + str(self.start_to_run_at_time)  
    


class Scheduler:
    """ Assumption: every handler returns a (possibly empty) collection of new events """
    
    def handleArrivalOfJobEvent(self, job, current_time):
        raise NotImplementedError()
    
    def handleTerminationOfJobEvent(self, job, current_time):
        raise NotImplementedError()
     


class CpuTimeSlice:
    ''' represents a "tentative feasible" snapshot of the cpu between the start_time until start_time + dur_time.
        It is tentative since a job might be rescheduled to an earlier slice. It is feasible since the total demand
        for processors ba all the jobs assigned to this slice never exceeds the amount of the total processors available.
        Assumption: the duration of the slice is never changed.
        We can replace this slice with a new slice with shorter duration.'''
    
    total_nodes = 0 # a class variable
    
    def __init__(self, free_nodes, start_time=0, duration=1):
        #assert duration > 0
        #assert start_time >= 0
        
        self.free_nodes = free_nodes
        self.start_time = start_time
        self.duration = duration                
        self.end_time = start_time + duration
        

    def addJob(self, job_nodes):
        # assert self.free_nodes >= job_nodes
        self.free_nodes -= job_nodes


    def delJob(self, job_nodes):
        self.free_nodes += job_nodes
        # assert self.free_nodes <= CpuTimeSlice.total_nodes

    def __str__(self):
        return '%d %d %d' % (self.start_time, self.duration, self.free_nodes)


        
        
class CpuSnapshot(object):
    """ represents the time table with the assignments of jobs to available processors. """
    
    def __init__(self, total_nodes=100):
        CpuTimeSlice.total_nodes = total_nodes
        self.total_nodes = total_nodes
        self.slices=[] # initializing the main structure of this class 
        self.slices.append(CpuTimeSlice(self.total_nodes)) # Assumption: the snapshot always has at least one slice 
        self.archive_of_old_slices=[]
        self.archive_of_scratch_slices=[]
    

    def _add_slice(self, index, free_nodes, start_time, duration):
        if len(self.archive_of_scratch_slices) > 0:  
            s = self.archive_of_scratch_slices.pop()
            s.free_nodes = free_nodes
            s.start_time = start_time
            s.duration = duration
            s.end_time = start_time + duration
            self.slices.insert(index, s)
        else:
            self.slices.insert(index, CpuTimeSlice(free_nodes, start_time, duration))


    def _ensure_a_slice_starts_at(self, start_time):
        """ A preprocessing stage. Usage: 
        First, to ensure that the assignment time of the new added job will start at a beginning of a slice.
        Second, to ensure that the actual end time of the job will end at the ending of slice.
        we need this when we add a new job, or delete a tail of job when the user estimation is larger than the actual
        duration.
        The idea: we first append 2 slices, just to make sure that there's a slice which ends after the start_time.
        We add one more slice just because we actually use list.insert() when we add a new slice.
        After that we itterate through the slices and split a slice if needed"""

        last = self.slices[-1]
        length = len(self.slices)
        self._add_slice(length, self.total_nodes, last.end_time, start_time + 1) # duration is huge 
        self._add_slice(length+1, self.total_nodes, last.end_time + start_time + 1, 1000) # duration is arbitrary

        index = -1
        for s in self.slices:
            index += 1 
            if s.start_time > start_time:
                break
            if s.start_time == start_time:  
                return # we already have such a slice
     
        # splitting slice s with respect to the start time
        s = self.slices[index-1]
        s.duration = start_time - s.start_time
        self._add_slice(index, s.free_nodes, start_time, s.end_time - start_time)
        s.end_time = s.start_time + s.duration
        return



    def free_nodes_available_at(self, time):
        for s in self.slices:
            if s.end_time <= time:
                continue
            return s.free_nodes
        return self.total_nodes
        

      

      
    def jobEarliestAssignment(self, job, time=0):
        """ returns the earliest time right after the given time for which the job can be assigned
        enough processors for job.estimated_run_time unit of times in an uninterrupted fashion.
        Assumption: number of requested processors is not greater than number of total processors.
        Assumptions: the given is greater than the arrival time of the job >= 0."""
        
        last = self.slices[-1]  
        length = len(self.slices)
        self._add_slice(length, self.total_nodes, last.end_time, time + job.estimated_run_time + 10)

        partially_assigned = False         
        tentative_start_time = accumulated_duration = 0
        
        # assert time >= 0
        
        for s in self.slices: # continuity assumption: if t' is the successor of t, then: t' = t + duration_of_slice_t
            

            feasible = s.end_time > time and s.free_nodes >= job.num_required_processors
            
            if not feasible: # then surely the job cannot be assigned to this slice
                partially_assigned = False
                accumulated_duration = 0
                        
            elif feasible and not partially_assigned:
                # we'll check if the job can be assigned to this slice and perhaps to its successive 
                partially_assigned = True
                tentative_start_time =  max(time, s.start_time)
                accumulated_duration = s.end_time - tentative_start_time

            else:
                # it's a feasible slice and the job is partially_assigned:
                accumulated_duration += s.duration
            
            if accumulated_duration >= job.estimated_run_time:
                return tentative_start_time
    

   
     
    def assignJob(self, job, job_start):         
        """ assigns the job to start at the given job_start time.        
        Important assumption: job_start was returned by jobEarliestAssignment. """
        job.start_to_run_at_time = job_start 
        job_estimated_finish_time = job.start_to_run_at_time + job.estimated_run_time
        self._ensure_a_slice_starts_at(job_start)
        self._ensure_a_slice_starts_at(job_estimated_finish_time)
        
        for s in self.slices:
            if s.start_time < job_start:
                continue
            elif s.start_time < job_estimated_finish_time:  
                s.addJob(job.num_required_processors) 
            else:
                return

        
    
        
    def delJobFromCpuSlices(self, job):        
        """ Deletes an _entire_ job from the slices. 
        Assumption: job resides at consecutive slices (no preemptions), and nothing is archived! """
        job_estimated_finish_time = job.start_to_run_at_time + job.estimated_run_time
        job_start = job.start_to_run_at_time
        self._ensure_a_slice_starts_at(job_start)
        self._ensure_a_slice_starts_at(job_estimated_finish_time)

        for s in self.slices:
            if s.start_time < job_start:
                continue
            elif s.start_time < job_estimated_finish_time:  
                s.delJob(job.num_required_processors) 
            else:
                return



    def delTailofJobFromCpuSlices(self, job):
        """ This function is used when the actual duration is smaller than the estimated duration, so the tail
        of the job must be deleted from the slices.
        We itterate trough the sorted slices until the critical point is found: the point from which the
        tail of the job starts. 
        Assumption: job is assigned to successive slices. Specifically, there are no preemptions."""

        if job.actual_run_time ==  job.estimated_run_time: 
            return
        job_finish_time = job.start_to_run_at_time + job.actual_run_time
        job_estimated_finish_time = job.start_to_run_at_time + job.estimated_run_time
        self._ensure_a_slice_starts_at(job_finish_time)
        self._ensure_a_slice_starts_at(job_estimated_finish_time)
        
        for s in self.slices:
            if s.start_time < job_finish_time:
                continue
            elif s.start_time < job_estimated_finish_time:  
                s.delJob(job.num_required_processors) 
            else:
                return

            
    def archive_old_slices(self, current_time):
        for s in self.slices[ : -1] :
            if s.end_time < current_time:
                self.archive_of_old_slices.append(s)
                self.slices.pop(0)
            else:
                self.unify_some_slices()
                return


    def unify_some_slices(self):
        prev = self.slices[0]
        for s in self.slices[1: -5]:
            if prev.free_nodes == s.free_nodes:
                prev.duration += s.duration
                prev.end_time += s.duration
                self.archive_of_scratch_slices.append(s)
                self.slices.remove(s)
            else: 
                prev = s
                
        last = self.slices[-1]
        if last.free_nodes == self.total_nodes and last.duration > 1000:
            last.duration = 1000
            last.end_time = last.start_time + 1000
        

        
    def _restore_old_slices(self):
        size = len(self.archive_of_old_slices)                   
        while size > 0:
            size -= 1
            s = self.archive_of_old_slices.pop()
            self.slices.insert(0, s)



    def printCpuSlices(self):
        print "start time | duration | #free processors "            
        for s in self.slices: 
            print s
        print
        


    def CpuSlicesTestFeasibility(self):
        self._restore_old_slices()
        duration = 0
        time = 0
        
        for s in self.slices:
            prev_duration = duration
            prev_time = time
            
            if s.free_nodes < 0 or s.free_nodes > self.total_nodes:  
                print ">>> PROBLEM: number of free processors is either negative or huge", s
                return False

            if s.start_time != prev_time + prev_duration:
                print ">>> PROBLEM: non successive slices", s.start_time, prev_time 
                return False
                
            duration = s.duration
            time = s.start_time
 
        print "TEST is OK!!!!" 
        return True



    def CpuSlicesTestEmptyFeasibility(self):
        self._restore_old_slices()
        duration = 0
        time = 0
        
        for s in self.slices:
            prev_duration = duration
            prev_time = time
            
            if s.free_nodes != self.total_nodes:  
                print ">>> PROBLEM: number of free processors is not the total processors", s
                return False

            if s.start_time != prev_time + prev_duration:
                print ">>> PROBLEM: non successive slices", s.start_time, prev_time 
                return False
                
            duration = s.duration
            time = s.start_time
 
        print "TEST EMPTY is OK!!!!" 
        return True

            

