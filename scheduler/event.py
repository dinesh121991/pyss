from sim import * 

class Event:

    def __init__(self, job=None):
        self.job = job
    

class EndOfSimulationEvent(Event):
    def __str__(self):
        return "End of Simulation Event" 

class JobArrivalEvent(Event):
    def __str__(self):
        return "Job Arrival Event: " + str(self.job)

class JobTerminationEvent(Event):
     def __str__(self):
        return "Job Termination Event: " + str(self.job)



class JobArrivalEventGeneratorViaLogFile:
    
    def __init__(self, input_file):
        """Assumption: Job details are 'correct': arrival_time, nodes and duration are non-negative,

        and the amount of nodes requested by the job is never more than the total available nodes"""
        
        self.file = file(input_file) #open the specified file for reading 
        self.events = {}
        
        while True: 
            line = self.file.readline()
            if len(line) == 0: # zero length indicates end-of-file
                break
            
            (job_arrival_time, job_id, job_duration, job_nodes, job_actual_duration ) = line.split()
            newJob = Job(job_id, int(job_duration), int(job_nodes), int(job_arrival_time), int(job_actual_duration))

            newEvent = JobArrivalEvent(newJob)

            if self.events.has_key(int(job_arrival_time)):
                self.events[int(job_arrival_time)].append(newEvent)
            else:
                self.events[int(job_arrival_time)] = []
                self.events[int(job_arrival_time)].append(newEvent)
            
        self.file.close()


    def printEvents(self):
        times = self.events.keys()
        times.sort()
        for t in times:
            for element in self.events[t]: 
                print element 
        print

            
        

class Simulator:

    def startSimulation(self):
        pass
    
    def __init__(self, total_nodes=100, input_file='input', current_time=0, scheduler="ConservativeScheduler"):

        self.current_time = current_time
        events_generated_by_input_file = JobArrivalEventGeneratorViaLogFile(input_file)
        self.events = events_generated_by_input_file.events
        self.scheduler = ConservativeScheduler(total_nodes) 
        self.startSimulation()

    def addEvent(self, event, time):
         if self.events.has_key(time):
             self.events[time].append(event)
         else:
             self.events[time] = []
             self.events[time].append(event)
    
    
    def startSimulation(self):
        """ Assumption1: times are non-negative numbers.
            Assumption2: if handling an event at time t generates a new event, then the
                         time of the new event must be greater than t+1. """
        """ Assumption3: if the duration of job is zero then it is not assigned to run ????? """


        termination_event_has_not_occured = True 

        while termination_event_has_not_occured and len(self.events) > 0:
 
            times_of_events = self.events.keys() #*** I couldn't do the sorting nicely as Ori suggested 
            times_of_events.sort()
            current_time = times_of_events.pop(0)

            while len(self.events[current_time]) > 0:
                
                print len(self.events[current_time])
                event = self.events[current_time].pop()
                print str(current_time) + ", " + str(event)

                if isinstance(event, JobArrivalEvent):
                    start_time_of_job = self.scheduler.addJob(event.job, int(current_time))
                    newEvent = JobTerminationEvent(event.job)
                    termination_time_of_job = start_time_of_job + event.job.actual_duration
                    self.addEvent(newEvent, termination_time_of_job)
                    continue

                elif isinstance(event, JobTerminationEvent):
                    
                    self.scheduler.cpu_snapshot.printCpuSlices()
                    if (event.job.start_to_run_at_time + event.job.actual_duration < current_time):
                        #this is event should be ignored #we should maybe implement events removal from the queue
                        continue
                    
                    #ELIMINATE the leftovers of the job from the cpu slices and reschedule 
                    newEvents = self.scheduler.handleTerminationOfJob(event.job, current_time)
                    for time, event in newEvents.iteritems():
                         self.addEvent(event, time)
                         print "_______Adding new Event after reschedule", time
                    self.scheduler.cpu_snapshot.printCpuSlices()
                    continue

                elif isinstance(event, EndOfSimulationEvent):
                    termination_event_has_not_occured = False 
                    break

                else:
                    assert False # should never reach here
                
            
            del self.events[current_time] #removing the events that were just handled
    

        print
        self.scheduler.cpu_snapshot.printCpuSlices()
            




class ConservativeScheduler:

    def __init__(self, total_nodes = 100):
        self.cpu_snapshot = CpuSnapshot(total_nodes)
        self.list_of_unfinished_jobs_arranged_by_arrival_times = []

        
    def addJob(self, job, time):
        self.list_of_unfinished_jobs_arranged_by_arrival_times.append(job)        
        start_time_of_job = self.cpu_snapshot.jobEarliestAssignment(job, time)
        self.cpu_snapshot.assignJob(job, start_time_of_job)
        return start_time_of_job


    def handleTerminationOfJob(self, job, time):
        """ this handler deletes the tail of job if it was ended before the duration declaration.
        It then reschedule the remaining jobs and returns a collection of new termination events
        (using the dictionary data structure) """
        self.list_of_unfinished_jobs_arranged_by_arrival_times.remove(job)
        if job.actual_duration < job.duration: 
            self.cpu_snapshot.delTailofJobFromCpuSlices(job)
            return self.reschedule_jobs(time)
        else:
            return {}


    def reschedule_jobs(self, time):

        newEvents ={}
        for job in self.list_of_unfinished_jobs_arranged_by_arrival_times:

            print str(job), "reschedule....."
            
            if job.start_to_run_at_time <= time:
                print "time is: ", time
                print "job.start_to_run_at_time: ", job.start_to_run_at_time
                continue # job started to run before, so it cannot be rescheduled (preemptions are not allowed)

            prev_start_to_run_at_time = job.start_to_run_at_time
            print "previous start time: ", prev_start_to_run_at_time
            self.cpu_snapshot.delJobFromCpuSlices(job)
            start_time_of_job = self.cpu_snapshot.jobEarliestAssignment(job, time)
            self.cpu_snapshot.assignJob(job, start_time_of_job)
            print "current start time of job: ", start_time_of_job
            if prev_start_to_run_at_time > job.start_to_run_at_time:
                new_event = JobTerminationEvent(job)
                new_termination_time = job.start_to_run_at_time + job.actual_duration
                print "new termination time", new_termination_time
                newEvents[new_termination_time] = new_event
                print "And here are the new events: ", newEvents
                
        return newEvents
                
                
                


###############

sim = Simulator()

            

        
        
        

    
        




"""
            while len(self.events[current_time]) > 0:
                print len(self.events[current_time])
                
                event = self.events[current_time].pop()

                print str(event)
                
                if isinstance(event, JobTerminationEvent):
                    termination_event_occured = True
                    break


            

job10 = Job('job10', 1000, 10)
job15 = Job('job15', 100, 10)
job18 = Job('job18', 1000, 10)
job19 = Job('job19', 1000, 10)


print "start_time, duration, free_nodes, jobs"
print
print
print

cs = CpuSnapshot(100)
cs.printCpuSlices()


start_time = cs.jobEarliestAssignment(job10, 0)
cs.assignJob(job10, start_time)
cs.printCpuSlices()
print


start_time = cs.jobEarliestAssignment(job15, 10)
print "eraliest start time", start_time, " for job15"  
cs.assignJob(job15, start_time)
#print job15 
cs.printCpuSlices()
print

start_time = cs.jobEarliestAssignment(job18, 0)
print "eraliest start time", start_time, " for job18"  
cs.assignJob(job18, start_time)
#print job18 
cs.printCpuSlices()
print

start_time = cs.jobEarliestAssignment(job19, 30)
print "eraliest start time", start_time, " for job19"  
cs.assignJob(job19, start_time)
#print job19 
cs.printCpuSlices()
print

cs.delJobFromCpuSlices(job15)
cs.printCpuSlices()
print




cs.assignJob('j15', 1000, 60)
cs.printCpuSlices()
print

cs.assignJob('j18', 1000, 60)
cs.printCpuSlices()
print


cs.assignJob('j20', 1000, 30)
cs.printCpuSlices()
print

cs.assignJob('j21', 1000, 20)
cs.printCpuSlices()
print

cs.assignJob('j22', 1000, 10)
cs.printCpuSlices()
print




cpu=CpuTimeSlice(0, 100)

print cpu

cpu.addJob(job10)
dict = cpu.getJobs(); 
print "dict", dict

cpu.addJob(job15)

print "cpu", cpu
print "dict", dict


"""
