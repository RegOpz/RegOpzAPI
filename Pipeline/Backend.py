from Helpers.DatabaseHelper import DatabaseHelper
from datetime import datetime


class Backend:
    def __init__(self,tenant_conn_details):
        self.db=DatabaseHelper(tenant_conn_details=tenant_conn_details)

    def get_job_detail(self,job_id):
      job=self.db.query("select * from jobs where id=%s",(int(job_id),)).fetchone()
      tasks=self.db.query("select * from tasks where job_id=%s",(int(job_id),)).fetchall()
      return {'job':job,'tasks':tasks}

    def create_job_run(self,job_id,job_parameter):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row_id=self.db.transact("insert into job_log(job_id,job_start_time,job_parameter) values(%s,%s,%s)",(int(job_id),current_time,\
                                                                                                             str(job_parameter)))
        self.db.commit()
        return row_id

    def get_job_log(self,job_id,job_run_id):
        job_log=self.db.query("select * from job_log where id=%s and job_id=%s",(job_run_id,int(job_id))).fetchone()
        return job_log

    def update_job_run(self,job_id,job_run_id,job_status):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row_id=self.db.transact("update job_log set job_end_time=%s,job_status=%s where id=%s and job_id=%s",\
                                (current_time,job_status,job_run_id,int(job_id)))
        self.db.commit()
        return row_id

    def create_task_run(self,job_run_id,job_id,task_id,task_status='STARTED'):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row_id=self.db.transact("insert into task_log(job_run_id,job_id,task_id,task_start_time,task_status) \
                values(%s,%s,%s,%s,%s)",(job_run_id,int(job_id),int(task_id),current_time,task_status))

        self.db.commit()
        return row_id

    def update_task_run(self,job_run_id,job_id,task_id,task_status,task_status_messgae=None,task_status_code=None):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        row_id=self.db.transact("update task_log set task_end_time=%s,task_status=%s,task_status_messgae=%s,\
                task_status_code=%s where job_run_id=%s and job_id=%s and task_id=%s",(current_time,task_status,task_status_messgae,\
                task_status_code,job_run_id,int(job_id),int(task_id)))
        self.db.commit()
        return row_id

    def get_task_log(self,job_run_id,job_id):
        task_log= self.db.query("select a.task_id,a.task_status,a.task_status_code from task_log a,(select task_id, max(id) id from task_log \
                                 where job_run_id=%s and job_id=%s group by task_id) b where a.task_id=b.task_id and a.id=b.id",
                                (job_run_id,int(job_id))).fetchall()

        return task_log




