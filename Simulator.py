import numpy as np
import itertools
import enum


class Event_type(enum.Enum):
    Arrival = 1
    Departure = 2
    Expire_check = 3
    
   
class Expire_type(enum.Enum):
    Not_expired = 1
    Scheduler_queue = 2
    Scheduler_server = 3
    Server_queue = 4


class Job:
    id_iter = itertools.count()
    def __init__(self, priority, arrival_to_scheduler_time, scheduler_server):
        self.priority = priority
        self.arrival_times = [arrival_to_scheduler_time] 
        self.start_times = []
        self.service_times = []
        self.servers = [scheduler_server]
        self.cores = []
        self.expired = False
        self.expire_time = None
        self.job_id = next(Job.id_iter)
    
    def get_priority(self):
        return self.priority
    
    def get_arrival_times(self):
        return self.arrival_times
    
    def get_service_times(self):
        return self.service_times
    
    def get_servers(self):
        return self.servers
    
    def get_cores(self):
        return self.cores
    
    def get_start_times(self):
        return self.start_times
    
    def get_expired(self):
        return self.expired
    
    def get_expire_time(self):
        return self.expire_time
    
    def get_job_id(self):
        return self.job_id
    
    def set_expired(self, expired):
        self.expired = expired
        
    def set_expire_time(self, time):
        self.expire_time = time 
        
    def get_expire_type(self):
        if len(self.start_times) == 0:
            return Expire_type.Scheduler_queue
        elif len(self.start_times) == 1 and len(self.arrival_times) == 1:
            return Expire_type.Scheduler_server
        elif len(self.start_times) == 1 and len(self.arrival_times) == 2:
            return Expire_type.Server_queue
        elif len(self.start_times) ==2 :
            return Expire_type.Not_expired

class Event:
    def __init__(self, type, time, job: Job):
        self.type = type
        self.time = time
        self.job = job
       
    def get_type(self):
        return self.type
    
    def get_time(self):
        return self.time
    
    def get_job(self):
        return self.job 
    
class Queue:
    def __init__(self ):
        self.queue_ = []
        self.change_times = [0]
        self.len_over_time = [0] 
        
    def get_length(self):
        return len(self.queue_)
    
    def append(self, job, time):
        self.queue_.append(job)
        self.change_times.append(time)
        self.len_over_time.append(len(self.queue_))
    
    def find_next_job_according_to_discipline(self, time):
        self.change_times.append(time)
        self.len_over_time.append(len(self.queue_) - 1)
        for i in range(0, len(self.queue_)):
            job = self.queue_[i]
            if job.get_priority() == 1:
                return self.queue_.pop(i)
        return self.queue_.pop(0)
        
    def get_average_lenght(self, curr_time):
        sum_ = 0
        for i in range(0, len(self.len_over_time) - 1):
            sum_ += (self.change_times[i+1] - self.change_times[i]) * self.len_over_time[i]
        
        sum_ += (curr_time - self.change_times[-1]) * self.len_over_time[-1]
        return sum_/ curr_time
    
    def put_job_out(self, job):
        for each_job in self.queue_:
            if job.get_job_id() == each_job.get_job_id():
                self.queue_.remove(job)
                return
    
class Server:
    def __init__(self, core_rates, type_):
        self.core_rates = core_rates
        self.queue = Queue()
        self.busy_status = [False for i in range(len(core_rates))]
        self.type_ = type_
        
    def generate_process_time(self, core_index):
        return np.random.exponential(1/ self.core_rates[core_index])
        
    def handle_event(self, event, events, servers):
        if event.get_type() == Event_type.Arrival:
            self.handle_arrival(event, events)
        if event.get_type() == Event_type.Departure:
            self.handle_departure(event, events, servers)
            
    def handle_arrival(self, event, events):
        if False in self.busy_status:
            core_index = self.busy_status.index(False)
            self.busy_status[core_index] = True
            job = event.get_job()
            service_time = self.generate_process_time(core_index)
            job.get_start_times().append(event.get_time())
            job.get_service_times().append(service_time)
            job.get_cores().append(core_index)
            
            new_event = Event(Event_type.Departure, event.get_time() + service_time, job)
            add_event_to_events(new_event, events)
        else:
            self.queue.append(event.get_job(), event.get_time())

            
    def handle_departure(self, event, events, servers):
        # if len(self.core_rates) == 1:
        #     print(event.get_time(), "dep")
        self.departure(event.get_job(), event.get_time(), events)
        if self.type_ == "scheduler":
            job = event.get_job()
            new_event = Event(Event_type.Arrival,  event.get_time(), job)
            server = Server.get_server_according_to_discipline(servers)
            job.get_servers().append(server)
            job.get_arrival_times().append(event.get_time())
            add_event_to_events(new_event, events)
            
    def get_server_according_to_discipline(servers):
        queue_lenghts = []
        for server in servers:
            queue_lenghts.append(server.queue.get_length())
        min_index = np.argmin(queue_lenghts)
        return servers[min_index]
        

        
    def departure(self, old_job, time, events):
        if self.queue.get_length() != 0:
            core_index = old_job.get_cores() [-1]
            new_job = self.queue.find_next_job_according_to_discipline(time)
            service_time = self.generate_process_time(core_index)
            
            new_job.get_start_times().append(time)
            new_job.get_service_times().append(service_time)
            new_job.get_cores().append(core_index)
            
            new_event = Event(Event_type.Departure, time + service_time, new_job)
            add_event_to_events(new_event, events)
        else: 
            core_index = old_job.get_cores() [-1]
            self.busy_status[core_index] = False

    def put_job_out_of_queue(self, job, time):
        job.set_expired(True)
        job.set_expire_time(time)
        self.queue.put_job_out(job)
    
    def put_job_out_of_server(self, job, time, events):
        job.set_expired(True)
        job.set_expire_time(time)
        self.queue.put_job_out(job)
        self.departure(job, time, events)
        
def generate_interarrival_time(lambda_):
    return np.random.exponential(1/lambda_)
    
def generate_expire_time(alpha):
    return np.random.exponential(alpha)   

def generate_priority():
    R = np.random.uniform(0,1)
    if R< 0.1:
        return 1
    return 2

def add_event_to_events(new_event, events: list):
    i = 0
    while i < len(events):
        if new_event.get_time() < events[i].get_time():
            break
        i +=1
    events.insert(i, new_event)

def expire_check(event, events):
    job = event.get_job()
    expire_type = job.get_expire_type()
    if expire_type == Expire_type.Not_expired:
        return
    elif expire_type == Expire_type.Scheduler_queue or expire_type == Expire_type.Server_queue:
        server = job.get_servers() [-1]
        server.put_job_out_of_queue(job, event.get_time())
    elif expire_type == Expire_type.Scheduler_server:
        server = job.get_servers() [-1]
        server.put_job_out_of_server(job, event.get_time(), events)
        delete_events_for_job(job, events)
        
def delete_events_for_job(job, events):
    events_copy = events
    for event in events_copy:
        if event.get_job().get_job_id() == job.get_job_id():
            events.remove(event)
            

def calc_num_each_type(jobs):
    num_type_1 = 0
    num_type_2 = 0
    for job in jobs:
        if job.get_priority() == 1:
            num_type_1 += 1
        elif job.get_priority() == 2:
             num_type_2 +=1 
    return num_type_1, num_type_2
        
def calc_avg_system_time_and_avg_waiting_time(jobs):
    sum_system_time_type_1 = 0
    sum_system_time_type_2 = 0
    sum_waiting_type_1 = 0
    sum_waiting_type_2 = 0

    for job in jobs:
        if not job.get_expired():
            waiting_time0 = job.get_start_times()[0] - job.get_arrival_times()[0] 
            waiting_time1 = job.get_start_times()[1] - job.get_arrival_times()[1]
            service_time = np.sum(job.get_service_times())
        else:
            expire_type = job.get_expire_type()
            if expire_type == Expire_type.Scheduler_queue:
                waiting_time0 = job.get_expire_time() - job.get_arrival_times()[0] 
                waiting_time1 = 0
                service_time = 0
            elif expire_type == Expire_type.Scheduler_server:
                waiting_time0 = job.get_start_times()[0] - job.get_arrival_times()[0]
                waiting_time1 = 0
                service_time = job.get_expire_time() - job.get_start_times()[0]
            elif expire_type == Expire_type.Server_queue:
                waiting_time0 = job.get_expire_time() - job.get_arrival_times()[1] 
                waiting_time1 = 0
                service_time = job.get_service_times()[0]
        
        if job.get_priority() == 1:
            sum_system_time_type_1 += waiting_time0 + waiting_time1 + service_time
            sum_waiting_type_1 += waiting_time0 + waiting_time1
        else:
            sum_system_time_type_2 += waiting_time0 + waiting_time1 + service_time
            sum_waiting_type_2 += waiting_time0 + waiting_time1
            
    num_type_1, num_type_2 = calc_num_each_type(jobs)
    
    avg_system_time_1 = sum_system_time_type_1 / num_type_1
    avg_system_time_2 = sum_system_time_type_2 / num_type_2
    avg_system_time = (sum_system_time_type_1 + sum_system_time_type_2) / (num_type_1 + num_type_2)
    
    avg_waiting_1 = sum_waiting_type_1 / num_type_1
    avg_waiting_2 = sum_waiting_type_2 / num_type_2
    avg_waiting = (sum_waiting_type_1 + sum_waiting_type_2) / (num_type_1 + num_type_2)
    
    return avg_system_time_1, avg_system_time_2, avg_system_time,\
        avg_waiting_1, avg_waiting_2, avg_waiting


def calc_expired_percent(jobs):
    number_of_type_1_expired = 0
    number_of_type_2_expired = 0
    for job in jobs:
        if job.get_expired():
            if job.get_priority() == 1:
                number_of_type_1_expired += 1
            else: 
                number_of_type_2_expired += 1
    
    num_type_1, num_type_2 = calc_num_each_type(jobs)
    percent_1 = number_of_type_1_expired/ num_type_1
    percent_2 = number_of_type_2_expired/ num_type_2  
    percent = (number_of_type_1_expired + number_of_type_2_expired) / (num_type_1 + num_type_2)
    return percent_1, percent_2, percent  

def generate_jobs_and_arrival(number_of_jobs, lambda_, alpha, shceduler_server, jobs: list, events: list):
    time = 0
    events1 = []
    events2 = []
    for number in range(0, number_of_jobs):
        time += generate_interarrival_time(lambda_)
        job = Job(generate_priority(), time, shceduler_server)
        jobs.append(job)
        event = Event(Event_type.Arrival, time, job)
        events1.append(event)
        # add_event_to_events(event, events)
        event = Event(Event_type.Expire_check, time + generate_expire_time(alpha) , job)
        events2.append(event)
        # add_event_to_events(event, events)
        
        

    events2.sort(key=lambda x: x.time)
    i = 0
    j = 0

    while i < len(events1) and j < len(events2):
        if events1[i].get_time() < events2[j].get_time():
            events.append(events1[i])
            i += 1
        else:
            events.append(events2[j])
            j += 1

    if i == len(events1) and j< len(events2):
        events = events + events2[j:]
    if i < len(events1) and j == len(events2):
        events = events + events1[i:]

        
    # print(len(events), len(merged_events))
    # for i in range(0, len(merged_events)):
    #     print(merged_events[i].get_time(), events[i].get_time())

def initialize(number_of_jobs):
    input_ = input("Please enter lambda, miu and alpha: \n")
    lambda_, alpha, miu= [float(x) for x in input_.split()]

    scheduler = Server([miu], "scheduler")
    servers = []
    for i in range(0, 5):
        input_ = input(f"Please enter core rates for server {i}  \n")
        core_rates = [float(x) for x in input_.split()]
        new_server = Server(core_rates, "server")
        servers.append(new_server)
            
    jobs = []
    events = []
    generate_jobs_and_arrival(number_of_jobs, lambda_, alpha, scheduler, jobs, events)
    return jobs, events, servers, scheduler

def simulate(jobs, events, servers, scheduler):
    simulation_time = 0
    
    iteration_count = 0
    while(len(events) != 0):
        
        iteration_count += 1
        if iteration_count % 1000 == 0:
            print(iteration_count,flush = True)
        event = events.pop(0)
        simulation_time = event.get_time()
        
        if event.get_type() == Event_type.Arrival or event.get_type() == Event_type.Departure:
            server = event.get_job().get_servers()[-1]
            server.handle_event(event, events, servers)
        
        elif event.get_type() == Event_type.Expire_check:
            expire_check(event, events)
            
        
    avg_system_1, avg_system_2, avg_system, \
        avg_waiting_1, avg_waiting_2, avg_waiting = calc_avg_system_time_and_avg_waiting_time(jobs)
    percent_1, percent_2, percent = calc_expired_percent(jobs)
    scheduler_avg_length = scheduler.queue.get_average_lenght(simulation_time)
    # print(scheduler.queue.change_times, scheduler.queue.len_over_time)
    servers_avg_lenght = []
    for server in servers:
        avg_length = server.queue.get_average_lenght(simulation_time)
        servers_avg_lenght.append(avg_length)
    

    result = {} 
    result['avg system_times'] = [avg_system_1, avg_system_2, avg_system]
    result['avg waiting_times'] = [avg_waiting_1, avg_waiting_2, avg_waiting]
    result['expired percents'] = [percent_1, percent_2, percent]
    result['avg queue lenghts'] = [scheduler_avg_length, servers_avg_lenght]
        
    return result

jobs, events, servers, shceduler = initialize(10000)

result = simulate(jobs, events, servers, shceduler)
print('                                    type 1           type 2                all')
print('avg system_times:  ', result['avg system_times'])
print('----------------------------------------------------------------------------------')
print('                                    type 1           type 2                all')
print('avg waiting_times: ', result['avg waiting_times'])
print('----------------------------------------------------------------------------------')
print('                                    type 1           type 2                all')
print('expired percents:  ', result['expired percents'])
print('----------------------------------------------------------------------------------')
print('scheduler avg queue lenghts: ', result['avg queue lenghts'][0])
print('----------------------------------------------------------------------------------')
print('servers avg queue lenghts: ', result['avg queue lenghts'][1])







        
    

