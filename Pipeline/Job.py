from .PyDAG import DAG
from .Backend import Backend
from .Tasks import task_types,Task,JobSignal
from queue import Queue
from threading import Thread
import time

class TaskStatus:
    READY=4
    RUNNING=3
    FAILED=2
    PREVIOUS=1
    COMPLETE=0

class Job(DAG):
    def __init__(self,tenant_conn_details):
        super(Job,self).__init__()
        self.backend=Backend(tenant_conn_details=tenant_conn_details)
        self.job_id=None
        self.job_run_id=None
        self.task_list = {}
        self.task_status = {}
        self.running_procs = {}
        self.task_output={}
        self.scheduler = None
        self.completed_task = Queue(maxsize=1000)
        self.job_parameters={}
        self.end_scheduler=JobSignal.SUCCESS

    def get_backend(self):
        return self.backend

    def get_job_id(self):
        return self.job_id

    def set_job_id(self,job_id):
        self.job_id=job_id

    def get_run_id(self):
        return self.job_run_id

    def set_run_id(self,job_run_id):
        self.job_run_id=job_run_id

    def terminate_scheduler(self,job_signal=JobSignal.ABORT):
        self.end_scheduler=job_signal

    def list_task_types(self):
        global task_types
        return task_types

    def run_scheduler(self):

        if not self.scheduler:
            self.scheduler=Thread(target=self.schedule_tasks)
            self.scheduler.start()

    def is_job_end(self):
        if sum(self.task_status.values())==TaskStatus.COMPLETE or self.end_scheduler < JobSignal.SUCCESS:
            return True
        return False

    def schedule_tasks(self):

        while True:
            self.check_complete()
            if self.is_job_end():
                break

            next_tasks=self.get_next_task()

            if next_tasks is None:
                time.sleep(5)
                continue
            for task in next_tasks:
                self.run_if_ready(task)

    def run_if_ready(self,task_id):
        dependencies=self.predecessors(task_id)

        for dep in dependencies:
            if self.task_status[dep] != TaskStatus.COMPLETE:
                return
            if not self.task_output[dep]:
                return
        self.run_task(task_id)

    def check_complete(self):
        for task in self.task_status.keys():
            if self.task_status[task]==TaskStatus.RUNNING:
                prc=self.running_procs.get(task,None)
                if (prc and not prc.is_alive()) or not prc:
                   self.set_task_status(task,TaskStatus.COMPLETE)
                   self.task_output[task]=self.task_list[task].output()
                   self.completed_task.put(task)
            elif self.task_status[task]==TaskStatus.PREVIOUS:
                self.set_task_status(task, TaskStatus.COMPLETE)
                self.task_output[task] = self.task_list[task].output()
                self.completed_task.put(task)

    def get_next_task(self):
        next_tasks=[]
        if self.completed_task.empty():
            return None

        while not self.completed_task.empty():
            task=self.completed_task.get()
            next_tasks+=self.downstream(task)

        return next_tasks

    def set_task_status(self,task_id,status):
        self.task_status[task_id]=status

    def run_task(self,task_id):

        if self.task_status.get(task_id,None) in (TaskStatus.COMPLETE,TaskStatus.RUNNING):
            return
        if self.task_status.get(task_id,None)== TaskStatus.PREVIOUS:
            self.set_task_status(task_id, TaskStatus.RUNNING)
            return

        task = self.task_list[task_id]
        task.set_runtime_parameters(self.job_parameters)
        prc=Thread(target=task.run)
        prc.start()
        self.set_task_status(task_id,TaskStatus.RUNNING)
        self.running_procs[task_id]=prc

    def update_job_status(self):
        if self.end_scheduler == JobSignal.ABORT:
            self.backend.update_job_run(self.job_id,self.job_run_id,"ABORT")
        elif self.end_scheduler == JobSignal.FAILURE:
            self.backend.update_job_run(self.job_id, self.job_run_id, "FAILURE")
        elif self.end_scheduler == JobSignal.SUCCESS:
            self.backend.update_job_run(self.job_id, self.job_run_id, "SUCCESS")




    def start_job(self,job_id,runtime_job_parameters=None):
        self.job_parameters=runtime_job_parameters if runtime_job_parameters else {}
        self.create_job(job_id)
        for task in self.ind_nodes():
            self.run_task(task)
        self.run_scheduler()

        self.scheduler.join()
        self.update_job_status()

        return self.end_scheduler


    def create_job(self,job_id):
        self.set_job_id(job_id)
        job_detail=self.backend.get_job_detail(job_id)
        tasks=job_detail['tasks']
        job_run_id = self.backend.create_job_run(job_id,self.job_parameters)
        self.set_run_id(job_run_id)

        for task in tasks:
            self.add_task(task)

        for task in tasks:
            if task['task_dependency']:
                dependencies=task['task_dependency'].split(',')
                for dep in dependencies:
                    self.add_dependency(str(dep),str(task['task_id']))

        valid,message=self.validate()
        if not valid:
            raise ValueError(message)

        return self.graph


    def add_task(self,task,task_status=TaskStatus.READY):
        t=Task()
        task_types = self.list_task_types()
        if task['task_type'] not in task_types.keys():
            raise KeyError("Tasktype %s not defined" % task['task_type'])
        task_inst =t.create_task(task['task_type'],str(task['task_id']),eval(task['task_input']),self)
        self.task_list[str(task['task_id'])] = task_inst
        self.add_node(str(task['task_id']))
        self.set_task_status(str(task['task_id']),task_status)

    def add_dependency(self,from_task_id,to_task_id):
        self.add_edge(from_task_id,to_task_id)

    def restart_job(self,job_id,job_run_id,start_task_id,runtime_job_parameters=None):
        job_log = self.backend.get_job_log(job_id, job_run_id)
        if not runtime_job_parameters:
            runtime_job_parameters=eval(job_log['job_parameter'])
        self.job_parameters = runtime_job_parameters if runtime_job_parameters else {}
        self.recreate_job(job_id,job_run_id,start_task_id)

        for task in self.ind_nodes():
            self.run_task(task)
        self.run_scheduler()

        self.scheduler.join()
        self.update_job_status()
        return self.end_scheduler

    def recreate_job(self,job_id,job_run_id,start_task_id):

        def find(lst, key, value):
            for i, dic in enumerate(lst):
                if dic.get(key,None) == value:
                    return i
            return -1

        self.set_job_id(job_id)
        self.set_run_id(job_run_id)
        job_detail=self.backend.get_job_detail(job_id)
        job_log=self.backend.get_task_log(job_run_id,job_id)
        tasks = job_detail['tasks']

        for task in tasks:
            idx=find(job_log,'task_id',task['task_id'])


            if idx<0:
                self.add_task(task,TaskStatus.READY)
            else:
                task_status=job_log[idx]['task_status']
                if task_status=='SUCCESS':
                  self.add_task(task,TaskStatus.PREVIOUS)
                elif task_status in ('FAILURE','STARTED'):
                    self.add_task(task,TaskStatus.FAILED)

        for task in tasks:
            if task['task_dependency']:
                dependencies = task['task_dependency'].split(',')
                for dep in dependencies:
                    self.add_dependency(str(dep), str(task['task_id']))

        if start_task_id:
            self.set_task_status(str(start_task_id), TaskStatus.READY)
            all_children=self.all_downstreams(str(start_task_id))
            for child in all_children:
                self.set_task_status(child,TaskStatus.READY)

        valid, message = self.validate()
        if not valid:
            raise ValueError(message)

        return self.graph




