from app import *
from flask import Flask, jsonify, request
import json
from Helpers.utils import autheticateTenant
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
from datetime import datetime
from Models.Token import Token

class OperationalLogController(Resource):
    def __init__(self):
        self.domain_info = autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.db = DatabaseHelper(self.tenant_info)
            self.user_id=Token().authenticate()

    def get(self):
        entity_type =request.args.get('entity_type')
        entity_id=request.args.get('entity_id')
        return self.fetch_log_details(entity_type=entity_type,
                                                entity_id=entity_id)

    def write_log_master(self, operation_type, operation_status, operation_narration, entity_type,
                         entity_name, entity_table_name, entity_id):

        try:
            current_date = datetime.now().strftime('%Y%m%d')
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # change by system time

            app.logger.info("Making entry into operational log master")
            row_id = self.db.transact("insert into operational_log_master(operation_type,operation_date,operation_start_time,operation_status,\
                                    operation_narration,operation_maker,entity_type,entity_name,entity_table_name,entity_id ) \
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                      (operation_type, current_date, current_time, operation_status, \
                                       operation_narration, self.user_id, entity_type, entity_name,
                                       entity_table_name, entity_id))
            self.db.commit()
            return row_id

        except Exception as e:
            app.logger.error(str(e))
            self.db.rollback()
            raise e

    def write_log_detail(self, master_id, operation_sub_type, operation_status, operation_narration):
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # change by system time
            app.logger.info("Making entry into operational log detail")
            row_id = self.db.transact("insert into operational_log_detail(master_id,operation_sub_type,operation_time,operation_status,operation_narration) \
                                      values(%s,%s,%s,%s,%s)",
                                      (master_id, operation_sub_type, current_time, operation_status, \
                                       operation_narration,))

            self.db.commit()
            return row_id

        except Exception as e:
            app.logger.error(str(e))
            self.db.rollback()
            raise e

    def update_master_status(self, id, operation_status):
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # change by system time
            app.logger.info("Updating status in operational master log")
            row_id = self.db.transact(
                "update operational_log_master set operation_status=%s,operation_end_time=%s where id=%s", \
                (operation_status, current_time,id))

            self.db.commit()
            return row_id

        except Exception as e:
            app.logger.error(str(e))
            self.db.rollback()
            raise e

    def fetch_log_details(self, entity_type, entity_id):

        try:

            app.logger.info("INSIDE FETCH_FROM_MASTER")

            sql_query="select * from operational_log_master " + \
                        " where entity_type=%s AND entity_id=%s"
            params = (entity_type, entity_id)
            data=self.db.query(sql_query,params).fetchall()

            for i,d in enumerate(data):
                for k,v in d.items():
                    if isinstance(v,datetime):
                        d[k] = d[k].isoformat()
                d['log_details'] = self.fetch_log_from_detail(master_id=d['id'])

            return data

        except Exception as e:
            app.logger.info(str(e))
            return {"msg": "Error while geting details of the operation logs. Error:  {}".format(str(e),)}, 400

    def fetch_log_from_detail(self, master_id):
        try:
            app.logger.info('INSIDE FETCH FROM DETAIL {}'.format(master_id,))

            sql_query="select * from operational_log_detail where master_id=%s;"
            params=(master_id,)
            detail_rows=self.db.query(sql_query,params).fetchall()

            for i,d in enumerate(detail_rows):
                # app.logger.info(d)
                for k,v in d.items():
                    if isinstance(v,datetime):
                        d[k] = d[k].isoformat()
            # app.logger.info("After loop...{}".format(detail_rows))
            return detail_rows
        except Exception as e:
            app.logger.info(str(e))
            raise e
