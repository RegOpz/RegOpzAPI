from app import *
from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.DatabaseOps import DatabaseOps
from Helpers.AuditHelper import AuditHelper
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *
import pandas as pd
import json
from datetime import datetime

class DataChangeController(Resource):
    def __init__(self):
        self.domain_info = autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.audit=AuditHelper('data_change_log',self.tenant_info)
            self.db=DatabaseHelper(self.tenant_info)

    # @authenticate
    def get(self):
        if request.endpoint=="get_data_audit_list":
            table_name=request.args.get("table_name")
            id_list=request.args.get("id_list")
            business_date=request.args.get("business_date")
            if table_name and table_name != 'undefined':
                return self.get_audit_history(id_list,table_name,business_date)
            return self.get_audit_list()
        if request.endpoint=="get_data_record_detail":
            table_name=request.args.get("table_name")
            id=request.args.get("id")
            return self.get_record_detail(table_name,id)

    def post(self):
        data_list=request.get_json(force=True)
        return self.audit_decision(data_list)


    def get_audit_history(self,id_list = None,table_name = None,business_date = None):
        app.logger.info("Getting audit list")
        try:
            sql = "SELECT DISTINCT id,table_name,change_type,change_reference,\
                date_of_change,maker,maker_comment,checker,checker_comment,status,date_of_checking,\
                business_date FROM {} WHERE 1"
            if id_list == "id" or ((id_list is None or id_list == 'undefined') and (table_name is None or table_name=='undefined')):
                sql += " AND status='PENDING'"
            if id_list:
                sql = sql + " and (id,business_date) in (" + id_list + ") "
            elif business_date:
                sql = sql + " and business_date=" + business_date
            if table_name is not None and table_name != 'undefined':
                sql = sql + " and table_name = '" + table_name + "'"
            return self.audit.get_audit_list(sql)
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def get_audit_list(self):
        app.logger.info("Getting audit list")
        try:
            pending_audit_df=pd.DataFrame(self.db.query("select * from data_change_log where status='PENDING'").fetchall())
            pending_audit_df['date_of_change']=pending_audit_df['date_of_change'].astype(dtype=datetime,errors='ignore')
            pending_audit_df['date_of_checking']=pending_audit_df['date_of_checking'].astype(dtype=datetime,errors='ignore')

            audit_list=[]
            for grp in pending_audit_df['group_id'].unique():
                group=[]
                inserts = 0
                deletes = 0
                updates = 0
                df=pending_audit_df.loc[pending_audit_df['group_id']==grp]
                maker=df['maker'].unique()[0]
                business_date=df['business_date'].unique()[0]
                group_tables=df['table_name'].unique()
                group_date_of_change=df['date_of_change'].min()
                non_update_df=df.loc[df['change_type']!='UPDATE']
                update_df=df.loc[df['change_type']=='UPDATE']

                #print(non_update_df,update_df)

                if not non_update_df.empty:
                    inserts = len(non_update_df[non_update_df['change_type']=='INSERT'].index)
                    deletes = len(non_update_df[non_update_df['change_type']=='DELETE'].index)
                    group += non_update_df.to_dict(orient='records')

                if not update_df.empty:
                    update_list = []

                    for idx,ugrp in update_df.groupby(['table_name','id']):
                        update_info = []
                        updates += 1
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
                                            "business_date": int(ugrp['business_date'].unique()[0]),
                                            "update_info": update_info
                                            })

                    group +=update_list

                audit_list.append({'group_id':grp,'maker':maker, 'business_date': int(business_date),
                                    'group':group, 'group_tables': ','.join(group_tables),
                                    'group_date_of_change': group_date_of_change.isoformat(),
                                    'inserts': inserts, 'deletes': deletes, 'updates': updates})
            for lst in audit_list:
                # app.logger.info('Processing index {}'.format(lst))
                for d in lst['group']:
                    for k,v in d.items():
                        if isinstance(v,datetime):
                            d[k] = d[k].isoformat()
                            # app.logger.info("{0} {1}".format(d[k], type(d[k])))
            return audit_list
        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500


    def get_record_detail(self,table_name,id):
        app.logger.info("Getting record detail")
        try:
            record_detail=self.db.query("select * from "+table_name+" where id="+str(id)).fetchone()
            return record_detail
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def audit_decision(self,data_list):
        try:
            approve=0
            reject=0
            regress=0
            for key in data_list.keys():
                for data in data_list[key]:
                    app.logger.info("Inside audit decision for status {}".format(data["status"]))
                    if data["status"]=="REJECTED":
                        self.audit.reject_dml(data)
                        reject+=1
                    if data["status"]=="APPROVED":
                        self.audit.approve_dml(data)
                        approve+=1
                    if data["status"]=="REGRESSED":
                        self.audit.regress_dml(data)
                        regress+=1
            return {"msg":"Successfully processed reviews for the following: " + \
                          "{1} approval, {2} rejection and {3} regress".format(approve,reject,regress)},200
        except Exception as e:
            app.logger.error(str(e))
            return {"msg": str(e)},500
