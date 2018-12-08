from Pipeline.Tasks import *
from Pipeline.Job  import Job
from app import *
from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *
import json
from Pipeline.Tasks import Task

class JobController(Resource):
    def __init__(self):
        self.domain_info = autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.job=Job(self.tenant_info)

    def post(self):
        if request.endpoint == "run_job_ep":
            try:
                job_detail=request.get_json(force=True)
                job_id=job_detail['job_id']
                job_parameters=job_detail.get('job_parameters',{})
                job_decision=job_detail.get('job_decision',{})
                runtime_job_parameters={**job_parameters,**job_decision}
                self.job.start_job(job_id,runtime_job_parameters)
            except Exception as e:
                raise(e)

        if request.endpoint=="restart_job_ep":
            try:
                job_detail=request.get_json(force=True)
                job_id=job_detail['job_id']
                job_parameters=job_detail.get('job_parameters',{})
                job_decision=job_detail.get('job_decision',{})
                runtime_job_parameters={**job_parameters,**job_decision}
                job_run_id=job_detail['job_run_id']
                restart_task_id=job_detail.get('restart_task_id',None)

                self.job.restart_job(job_id,job_run_id,restart_task_id,runtime_job_parameters)
            except Exception as e:
                raise(e)

    def get(self):
       if request.endpoint=="get_job_parameter_ep":
           job_id=request.args.get("job_id")
           return self.get_input_parameter_list(job_id)

    def get_input_parameter_list(self,job_id):
        task_inst=Task()
        backend=self.job.get_backend()
        job_detail=backend.get_job_detail(job_id)
        task_list=[]
        for task in job_detail['tasks']:
            task_list.append(task_inst.create_task(task['task_type']))
        input_list={}
        parameter_list={}
        for task in task_list:
            input_list.update(task.inputs())
            parameter_list.update(task.parameters())

        return {'inputs':input_list,'parameters':parameter_list}

