from app import *
from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.DatabaseOps import DatabaseOps
from Helpers.AuditHelper import AuditHelper
import json
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *
import pandas as pd
import json
from datetime import datetime

class DefChangeController(Resource):
    def __init__(self,tenant_info=None):
        if tenant_info:
            self.tenant_info=tenant_info
        else:
            self.domain_info = autheticateTenant()
            if self.domain_info:
                tenant_info = json.loads(self.domain_info)
                self.tenant_info = json.loads(tenant_info['tenant_conn_details'])

        self.db=DatabaseHelper(self.tenant_info)
        self.audit=AuditHelper('def_change_log',self.tenant_info)

    #@authenticate
    def get(self):
        if request.endpoint=="get_audit_list":
            table_name=request.args.get("table_name")
            id_list=request.args.get("id_list")
            return self.get_audit_list(id_list,table_name)


    #@authenticate
    def post(self):
        if request.endpoint=="get_record_detail":
            record_list=request.get_json(force=True)
            return self.get_record_detail(record_list)

        data=request.get_json(force=True)
        return self.audit_decision(data)



    def get_audit_list(self,id_list=None,table_name=None):
        app.logger.info("Getting audit list")
        try:
            pending_audit_df=pd.DataFrame(self.db.query("select * from def_change_log where status='PENDING'").fetchall())
            pending_audit_df['date_of_change']=pending_audit_df['date_of_change'].astype(dtype=datetime,errors='ignore')
            pending_audit_df['date_of_checking']=pending_audit_df['date_of_checking'].astype(dtype=datetime,errors='ignore')

            audit_list=[]
            for grp in pending_audit_df['group_id'].unique():
                group=[]
                df=pending_audit_df.loc[pending_audit_df['group_id']==grp]
                maker=pending_audit_df['maker'].unique()[0]
                tenant_id=pending_audit_df['tenant_id'].unique()[0]
                non_update_df=df.loc[df['change_type']!='UPDATE']
                update_df=df.loc[df['change_type']=='UPDATE']

                #print(non_update_df,update_df)

                if not non_update_df.empty:
                    group += non_update_df.to_dict(orient='records')

                if not update_df.empty:
                    update_list = []

                    for idx,ugrp in update_df.groupby(['table_name','id']):
                        update_info = []
                        for ix, row in ugrp.iterrows():
                            update_info.append(
                                {"field_name": row['field_name'], "old_val": row["old_val"], "new_val": row["new_val"]})

                        update_list.append({"id": str(ugrp['id'].unique()[0]),
                                            "table_name": ugrp['table_name'].unique()[0],
                                            "change_type": ugrp['change_type'].unique()[0],
                                            "date_of_change": ugrp['date_of_change'].unique()[0],
                                            "maker": ugrp['maker'].unique()[0],
                                            "maker_comment": ugrp['maker_comment'].unique()[0],
                                            "checker": ugrp['checker'].unique()[0],
                                            "checker_comment": ugrp['checker_comment'].unique()[0],
                                            "status": ugrp['status'].unique()[0],
                                            "date_of_checking": ugrp['date_of_checking'].unique()[0],
                                            "change_reference": ugrp['change_reference'].unique()[0],
                                            "group_id": str(ugrp['group_id'].unique()[0]),
                                            "tenant_id":ugrp['tenant_id'].unique()[0],
                                            "update_info": update_info
                                            })

                    group +=update_list

                audit_list.append({'group_id':grp,'maker':maker,'tenant_id':tenant_id,'group':group})
            for lst in audit_list:
                # app.logger.info('Processing index {}'.format(lst))
                for d in lst['group']:
                    for k,v in d.items():
                        if isinstance(v,datetime):
                            d[k] = d[k].isoformat()
                            # app.logger.info("{0} {1}".format(d[k], type(d[k])))

            #app.logger.info("audit_list {}".format(audit_list,))
            return audit_list
        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500

    def get_record_detail(self,record_list):
        app.logger.info("Getting record detail")
        try:
            record_details=[]
            for rec in record_list:
                if rec["table_name"]=="business_rules":
                    detail=self.fetch_business_rule_detail(rec["id"])
                    record_details.append({"type":"business_rules","payload":detail})

            return record_details

        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500

    def fetch_business_rule_detail(self,id):
        app.logger.info("Getting record detail for business rules id:{}".format(id))
        try:
            business_rule_detail=self.db.query("select * from business_rules where id=%s",(id,)).fetchone()
            return business_rule_detail
        except Exception as e:
            app.logger.error(str(e))
            raise e

    def audit_decision(self,data_list):
        try:
            app.logger.info("Inside audit decision for status {}".format(data["status"]))
            for data in data_list:
                if data["status"]=="REJECTED":
                    self.audit.reject_dml(data)
                if data["status"]=="APPROVED":
                    self.audit.approve_dml(data)
                if data["status"]=="REGRESSED":
                    self.audit.regress_dml(data)
            return {"msg":"Successfully changed status for the records"},200
        except Exception as e:
            app.logger.error(str(e))
            return {"msg": str(e)},500
