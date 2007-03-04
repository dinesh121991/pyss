
from sim import * 

class Event:

    def __init__(self, event_type, job=None):
        self.type = event_type
        self.job = job

    def __str__(self):
        return "Type: " + str(self.type) + ", Job: " + str(self.job)
    
   

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
            
            (job_arrival_time, job_id, job_duration, job_nodes) = line.split()
            newJob = Job(job_id, int(job_duration), int(job_nodes), int(job_arrival_time))

            newEvent = Event("job_arrival_event", newJob)

            if self.events.has_key(job_arrival_time):
                self.events[job_arrival_time].append(newEvent)
            else:
                self.events[job_arrival_time] = []
                self.events[job_arrival_time].append(newEvent)
            
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
    
    def __init__(self, total_nodes=100, input_file='input', current_time=0, scheduler=ConservativeScheduler):

        self.current_time = current_time
        events_generated_by_input_file = JobArrivalEventGeneratorViaLogFile(input_file)
        self.events = events_generated_by_input_file.events
        self.startSimulation()
        self.scheduler = ConservativeScheduler(total_nodes) 
        print self.scheduler.cpu_snapshot.printCpuSlices()
         
    def startSimulation(self):
        """ Assumption1: times are non-negative numbers.
            Assumption2: if handling an event at time t generates a new event, then the
                         time of the new event must be greater than t+1."""  
        
        times_of_events = self.events.keys() #*** I couldn't do the sorting nicely as Ori suggested 
        times_of_events.sort()            
        
        for t in times_of_events:
            for event in self.events[t]:
                if event.type == "job_arrival_event":
                    print str(event)
                    # self.scheduler.addJob(event.job, t) 
                    #here we should handle the event (e.g. notify the schedule about job arrival) 
                else: 
                    print "bla bla"

            self.events[t]=[] # removing the handled events of time t 

        print self.scheduler.cpu_snapshot.printCpuSlices()
            


sim = Simulator()

            

        
        
        

    
        


"""

job10 = Job('job10', 1000, 10)
job15 = Job('job15', 1000, 10)
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
