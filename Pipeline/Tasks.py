from .Backend import  Backend
from multiprocessing import Pool
from Controllers.GenerateReportController import GenerateReportController
from Controllers.LoadDataController import LoadDataController
from Controllers.ViewDataController import ViewDataController
task_types = {
    'DummyTask': "Dummy Task",
    'CreateReportTask':"Create Report",
    'LoadData': 'Load Data',
    'ApplyRules': 'Apply Business Rule',
}

class JobSignal:
    SUCCESS=1
    ABORT=0
    FAILURE=-1



class Task:
    def create_task(self,task_type,task_id=None,input=None,parent_job=None):
        if task_type not in task_types.keys():
            raise TypeError("Task type {} is not defined".format(task_type))
        task=eval(task_type+"()")
        task.set_input(input)
        task.set_run_id(parent_job,task_id)
        task.set_runtime_parameters({})
        return task

    def set_runtime_parameters(self,parameters):
        self.runtime_parameters=parameters

    def set_run_id(self,parent_job,task_id):
        self.parent_job=parent_job
        if self.parent_job:
            self.run_id=self.parent_job.get_run_id()
            self.job_id=self.parent_job.get_job_id()
            self.backend=self.parent_job.get_backend()
        else:
            self.run_id = None
            self.job_id = None
            self.backend = None

        self.task_id=task_id

    def set_input(self,input):
        self.input=input

    def inputs(self):
        pass

    def parameters(self):
        pass

    def run(self,**kwargs):
        pass

    def output(self,decision=None):
        pass

class DummyGenerator:
    def calc_square(self,x):
        return x * x
    def generate_squares(self,int_array):
        pool = Pool(2)
        result = pool.map(self.calc_square, int_array)
        pool.close()
        pool.join()
        result_sum = sum(result)
        return result_sum

class DummyTask(Task):
    def inputs(self):
        inp={"Input1":"string"}
        return inp

    def parameters(self):
        param={"Param1":"string"}
        return param

    def run(self):
        try:
            #print(self.runtime_parameters)
            dm=DummyGenerator()
            int_array=[i for i in range(100)]

            self.backend.create_task_run(self.run_id,self.job_id,self.task_id)
            input=self.inputs()
            for key in input:
                if key not in self.input.keys():
                    raise KeyError("%s not present in inputs" % key)

            param=self.parameters()
            for key in param:
                if key not in self.runtime_parameters.keys():
                    raise KeyError("%s not present in parameters" %key)

            result_sum=dm.generate_squares(int_array)

            print("Dummy task run with input:{0},parameter:{1}.And the result is {2}".format(self.input['Input1'],self.runtime_parameters["Param1"],result_sum))

            self.backend.update_task_run(self.run_id,self.job_id,self.task_id,"SUCCESS","Dummy task completed successfully","200")
        except Exception as e:
            self.backend.update_task_run(self.run_id, self.job_id, self.task_id, "FAILURE",
                                         "Dummy task failed", "403")
            raise e

    def output(self):
        decision=self.runtime_parameters.get(self.task_id,True)

        if not decision:
            self.parent_job.terminate_scheduler(JobSignal.ABORT)
        return decision


class CreateReportTask(Task):
    def inputs(self):
        inp={"report_id":"string",
             "reporting_currency":"string",
             'ref_date_rate':"string",
             'rate_type':"string"
             }

        return inp

    def parameters(self):
        param={'business_date_from':"date",
               'business_date_to':"date",
               "reporting_parameters":"dictionary"}
        return param

    def run(self):
        try:
            report=GenerateReportController()

            self.backend.create_task_run(self.run_id, self.job_id, self.task_id)
            input = self.inputs()
            for key in input:
                if key not in self.input.keys():
                    raise KeyError("%s not present in inputs" % key)

            param = self.parameters()
            for key in param:
                if key not in self.runtime_parameters.keys():
                    raise KeyError("%s not present in parameters" % key)

            report_parameters={**self.runtime_parameters["reporting_parameters"],
                               "report_id":self.input["report_id"],
                               "reporting_currency":self.input["reporting_currency"],
                               "business_date_from":self.runtime_parameters["business_date_from"],
                               "business_date_to": self.runtime_parameters["business_date_to"],
                               'ref_date_rate': self.input["ref_date_rate"],
                               'rate_type': self.input["rate_type"]

                               }

            msg,status_code=report.create_report(report_parameters)
            status="SUCCESS" if status_code ==200 else "FAILURE"

            self.backend.update_task_run(self.run_id, self.job_id, self.task_id, status,
                                         msg["msg"], status_code)
        except Exception as e:
            self.backend.update_task_run(self.run_id, self.job_id, self.task_id, "FAILURE",
                                         str(e), "403")
            raise(e)



    def output(self):
        decision = self.runtime_parameters.get(self.task_id, True)
        if not decision:
            self.parent_job.terminate_scheduler(JobSignal.ABORT)
            return decision

        task_status=self.backend.get_task_status(self.run_id,self.job_id,self.task_id)
        decision= True if task_status=="SUCCESS" else False

        if not decision:
            self.parent_job.terminate_scheduler(JobSignal.FAILURE)

        return decision


class LoadData(Task):
    def inputs(self):
        inp={"source_id":"number"}

        return inp

    def parameters(self):
        param={
                "business_date":"string",
                "data_file": "string",
                "selected_file":"string",
                }
        return param

    def run(self):
        try:
            ldc=LoadDataController()

            self.backend.create_task_run(self.run_id, self.job_id, self.task_id)
            input = self.inputs()
            for key in input:
                if key not in self.input.keys():
                    raise KeyError("%s not present in inputs" % key)

            param = self.parameters()
            for key in param:
                if key not in self.runtime_parameters.keys():
                    raise KeyError("%s not present in parameters" % key)

            msg,status_code=ldc.load_data(self.input['source_id'],
                                        self.runtime_parameters['data_file'],
                                        self.runtime_parameters['business_date'],
                                        self.runtime_parameters['selected_file'])
            status="SUCCESS" if status_code ==200 else "FAILURE"

            self.backend.update_task_run(self.run_id, self.job_id, self.task_id, status,
                                         msg["msg"], status_code)
        except Exception as e:
            self.backend.update_task_run(self.run_id, self.job_id, self.task_id, "FAILURE",
                                         str(e), "403")
            raise(e)



    def output(self):
        decision = self.runtime_parameters.get(self.task_id, True)
        if not decision:
            self.parent_job.terminate_scheduler(JobSignal.ABORT)
            return decision

        task_status=self.backend.get_task_status(self.run_id,self.job_id,self.task_id)
        decision= True if task_status=="SUCCESS" else False

        if not decision:
            self.parent_job.terminate_scheduler(JobSignal.FAILURE)

        return decision

class ApplyRules(Task):
    def inputs(self):
        inp={"source_id":"number"}

        return inp

    def parameters(self):
        param={
                "business_date":"string",
                }
        return param

    def run(self):
        try:
            vdc=ViewDataController()

            self.backend.create_task_run(self.run_id, self.job_id, self.task_id)
            input = self.inputs()
            for key in input:
                if key not in self.input.keys():
                    raise KeyError("%s not present in inputs" % key)

            param = self.parameters()
            for key in param:
                if key not in self.runtime_parameters.keys():
                    raise KeyError("%s not present in parameters" % key)

            msg,status_code=vdc.run_rules_engine(self.input['source_id'],
                                        self.runtime_parameters['business_date'])
            status="SUCCESS" if status_code ==200 else "FAILURE"

            self.backend.update_task_run(self.run_id, self.job_id, self.task_id, status,
                                         msg["msg"], status_code)
        except Exception as e:
            self.backend.update_task_run(self.run_id, self.job_id, self.task_id, "FAILURE",
                                         str(e), "403")
            raise(e)



    def output(self):
        decision = self.runtime_parameters.get(self.task_id, True)
        if not decision:
            self.parent_job.terminate_scheduler(JobSignal.ABORT)
            return decision

        task_status=self.backend.get_task_status(self.run_id,self.job_id,self.task_id)
        decision= True if task_status=="SUCCESS" else False

        if not decision:
            self.parent_job.terminate_scheduler(JobSignal.FAILURE)

        return decision
