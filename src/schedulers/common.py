import sys

class Job:
    def __init__(self, job_id, user_predicted_duration, job_nodes, \
                 job_arrival_time=0, job_actual_duration=0, job_admin_QoS=0, job_user_QoS=0):
        
        #assert job_nodes > 0
        #assert job_actual_duration >= 0
        #assert user_predicted_duration >= job_actual_duration
        #assert job_arrival_time >= 0
        
        self.id = job_id
        self.user_predicted_duration = user_predicted_duration
        self.nodes = job_nodes
        self.arrival_time = job_arrival_time # Assumption: arrival time is greater than zero 
        self.start_to_run_at_time = -1 
        self.actual_duration = job_actual_duration

        # the next are essentially for the MauiScheduler
        self.admin_QoS = job_admin_QoS # the priority given by the system administration  
        self.user_QoS = job_user_QoS # the priority given by the user
        self.maui_bypass_counter = 0
        self.maui_timestamp = 0
        

    def __str__(self):
        return "job_id=" + str(self.id) + ", arrival=" + str(self.arrival_time) + \
               ", dur=" + str(self.user_predicted_duration) + ",act_dur=" + str(self.actual_duration) + \
               ", #nodes=" + str(self.nodes) + \
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
        for nodes ba all the jobs assigned to this slice never exceeds the amount of the total nodes available.
        Assumption: the duration of the slice is never changed.
        We can replace this slice with a new slice with shorter duration.'''
    
    total_nodes = 0 # a class variable
    
    def __init__(self, free_nodes, start_time=0, duration=1):
        #assert duration > 0
        #assert start_time >= 0
        
        self.free_nodes = free_nodes
        self.start_time = start_time
        self.duration = duration                
            

    def addJob(self, job_nodes):
        # assert self.free_nodes >= job_nodes
        self.free_nodes -= job_nodes


    def delJob(self, job_nodes):
        self.free_nodes += job_nodes
        # assert self.free_nodes <= CpuTimeSlice.total_nodes

    def __str__(self):
        return '%d %d %d' % (self.start_time, self.duration, self.free_nodes)


        
        
class CpuSnapshot(object):
    """ represents the time table with the assignments of jobs to available nodes. """
    
    def __init__(self, total_nodes=100):
        CpuTimeSlice.total_nodes = total_nodes
        self.total_nodes = total_nodes
        self.slices=[] # initializing the main structure of this class 
        self.slices.append(CpuTimeSlice(self.total_nodes)) # Assumption: the snapshot always has at least one slice 
        self.archive_of_old_slices=[]

    

    def jobEarliestAssignment(self, job, time=0):
        """ returns the earliest time right after the given time for which the job can be assigned
        enough nodes for job.user_predicted_duration unit of times in an uninterrupted fashion.
        Assumption: number of requested nodes is not greater than number of total nodes.
        Assumptions: the given is greater than the arrival time of the job >= 0."""
        
        partially_assigned = False         
        tentative_start_time = accumulated_duration = 0
        
        # assert time >= 0
        
        for s in self.slices: # continuity assumption: if t' is the successor of t, then: t' = t + duration_of_slice_t
            
            end_of_this_slice = s.start_time +  s.duration

            feasible = end_of_this_slice > time and s.free_nodes >= job.nodes
            
            if not feasible: # then surely the job cannot be assigned to this slice
                partially_assigned = False
                accumulated_duration = 0
                        
            elif feasible and not partially_assigned:
                # we'll check if the job can be assigned to this slice and perhaps to its successive 
                partially_assigned = True
                tentative_start_time =  max(time, s.start_time)
                accumulated_duration = end_of_this_slice - tentative_start_time

            else:
                # it's a feasible slice and the job is partially_assigned:
                accumulated_duration += s.duration
            
            if accumulated_duration >= job.user_predicted_duration:
                return tentative_start_time
    
            # end of for loop, we've examined all existing slices
            
        if partially_assigned: #and so there are not enough slices in the tail, then:
            return tentative_start_time

        # otherwise, the job will be assigned right after the last slice or later
        last = self.slices[-1]
        last_slice_end_time =  last.start_time + last.duration
        return max(time, last_slice_end_time)  

   
    

    def _ensure_a_slice_starts_at(self, start_time):
        """ A preprocessing stage. Usage: 
        First, to ensure that the assignment time of the new added job will start at a beginning of a slice.
        Second, to ensure that the actual end time of the job will end at the ending of slice.
        we need this when we add a new job, or delete a tail of job when the user estimation is larger than the actual
        duration. """

        last = self.slices[-1]
        last_slice_end_time =  last.start_time + last.duration
        
        if last_slice_end_time < start_time: #we add an intermediate "empty" slice to maintain the "continuity" of slices
            self.slices.append( CpuTimeSlice(self.total_nodes, last_slice_end_time, start_time - last_slice_end_time) )
            self.slices.append( CpuTimeSlice(self.total_nodes, start_time, 9999) ) # duration is arbitrary here
            return 
        
        elif last_slice_end_time == start_time:
            self.slices.append( CpuTimeSlice(self.total_nodes, start_time, 9999) ) # duration is arbitrary here
            return

        else:  # just to make sure that always there's a last slice 
            self.slices.append( CpuTimeSlice(self.total_nodes, last_slice_end_time, 9999) )

        index = -1
        for s in self.slices:
            index += 1 
            if s.start_time > start_time:
                break
            if s.start_time == start_time:  
                return # we already have such a slice
     
        # splitting slice s with respect to the start time
        s = self.slices[index-1]
        end_of_this_slice = s.start_time +  s.duration
        s.duration = start_time - s.start_time
        self._add_slice(index, s.free_nodes, start_time, end_of_this_slice - start_time)
        return



    def _add_slice(self, index, free_nodes, start_time, duration):
        # if the last slice is empty (without any assigned job) we take this slice,
        # otherwise we allocate a new slice object
        if self.slices[-1].free_nodes == self.total_nodes: 
            s = self.slices.pop()
            s.free_nodes = free_nodes
            s.start_time = start_time
            s.duration = duration
            self.slices.insert(index, s)
        else:
            self.slices.insert(index, CpuTimeSlice(free_nodes, start_time, duration))

      
        

     
    def assignJob(self, job, job_start):         
        """ assigns the job to start at the given assignment time.        
        Important assumption: assignment_time was returned by jobEarliestAssignment. """
        job.start_to_run_at_time = job_start 
        job_predicted_finish_time = job.start_to_run_at_time + job.user_predicted_duration
        self._ensure_a_slice_starts_at(job_start)
        self._ensure_a_slice_starts_at(job_predicted_finish_time)
        
        for s in self.slices:
            if s.start_time < job_start:
                continue
            elif s.start_time < job_predicted_finish_time:  
                s.addJob(job.nodes) 
            else:
                return

        
            
        
    def delJobFromCpuSlices(self, job):        
        """ Deletes an _entire_ job from the slices. 
        Assumption: job resides at consecutive slices (no preemptions) """
        
        job_predicted_finish_time = job.start_to_run_at_time + job.user_predicted_duration
        job_start = job.start_to_run_at_time
        self._ensure_a_slice_starts_at(job_start)
        self._ensure_a_slice_starts_at(job_predicted_finish_time)

        for s in self.slices:
            if s.start_time < job_start:
                continue
            elif s.start_time < job_predicted_finish_time:  
                s.delJob(job.nodes) 
            else:
                return



    def delTailofJobFromCpuSlices(self, job):
        """ This function is used when the actual duration is smaller than the predicted duration, so the tail
        of the job must be deleted from the slices.
        We itterate trough the sorted slices until the critical point is found: the point from which the
        tail of the job starts. 
        Assumption: job is assigned to successive slices. Specifically, there are no preemptions."""

        if job.actual_duration ==  job.user_predicted_duration: 
            return
        job_finish_time = job.start_to_run_at_time + job.actual_duration
        job_predicted_finish_time = job.start_to_run_at_time + job.user_predicted_duration
        self._ensure_a_slice_starts_at(job_finish_time)
        self._ensure_a_slice_starts_at(job_predicted_finish_time)
        
        for s in self.slices:
            if s.start_time < job_finish_time:
                continue
            elif s.start_time < job_predicted_finish_time:  
                s.delJob(job.nodes) 
            else:
                return

            
    def archive_old_slices(self, current_time):
        for s in self.slices[ : -1] :
            if s.start_time + s.duration < current_time:
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
                self.slices.remove(s)
            else: 
                prev = s
            
        
        
    def _restore_old_slices(self):
        size = len(self.archive_of_old_slices)                   
        while size > 0:
            size -= 1
            s = self.archive_of_old_slices.pop()
            self.slices.insert(0, s)



    def printCpuSlices(self):
        print "start time | duration | #free nodes "            
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
                print ">>> PROBLEM: number of free nodes is either negative or huge", s
                return False

            if s.start_time != prev_time + prev_duration:
                print ">>> PROBLEM: non successive slices", s.start_time, prev_time 
                return False
                
            duration = s.duration
            time = s.start_time

 
        print "TEST is OK!!!!" 
        return True
    
            

