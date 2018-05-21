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
        self.db_master=DatabaseHelper()
        self.audit=AuditHelper('def_change_log',self.tenant_info)

    #@authenticate
    def get(self):
        if request.endpoint=="get_audit_list":
            table_name=request.args.get("table_name")
            id_list=request.args.get("id_list")
            source_id=request.args.get("source_id")
            if table_name and table_name!='undefined':
                return self.get_audit_history(id_list,table_name,source_id)
            return self.get_audit_list()
        if request.endpoint=="get_record_detail":
            table_name=request.args.get("table_name")
            id=request.args.get("id")
            return self.get_record_detail(table_name,id)



    #@authenticate
    def post(self):
        data_list=request.get_json(force=True)
        return self.audit_decision(data_list)



    def get_audit_list(self):
        app.logger.info("Getting audit list")
        try:
            pending_audit_df=pd.DataFrame(self.db.query("select * from def_change_log where status='PENDING'").fetchall())
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
                maker_tenant_id=df['maker_tenant_id'].unique()[0]
                checker_tenant_id=df['checker_tenant_id'].unique()[0]
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
                                            "maker_tenant_id":ugrp['maker_tenant_id'].unique()[0],
                                            "checker_tenant_id":ugrp['checker_tenant_id'].unique()[0],
                                            "update_info": update_info
                                            })

                    group +=update_list

                audit_list.append({'group_id':grp,'maker':maker,'maker_tenant_id':maker_tenant_id,
                                    'checker_tenant_id':checker_tenant_id,
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

    def get_audit_history(self,id_list=None,table_name=None,source_id=None):
        app.logger.info("Getting audit change history list")
        try:
            sql = "SELECT DISTINCT id,table_name,change_type,change_reference," + \
                    "date_of_change,maker,maker_tenant_id,maker_comment,checker,checker_comment,status,date_of_checking " + \
                    "FROM def_change_log WHERE id IN (" + \
                    (id_list if id_list and id_list!='undefined' else "id") + \
                    ") AND table_name = '" + table_name + "'"

            if table_name=="business_rules":
                sql += " and id in (select id from business_rules where source_id='{0}')".format(source_id,)
            if table_name=="business_rules_master":
                sql += " and id in (select id from business_rules_master where country='{0}')".format(source_id,)

            if '_master' in table_name:
                db=self.db_master
            else:
                db=self.db

            audit_hist = db.query(sql).fetchall()
            # app.logger.info("Audit hist list {}".format(audit_hist,))
            for i,item in enumerate(audit_hist):
                for k,v in item.items():
                    if isinstance(v,datetime):
                        item[k] = item[k].isoformat()
                if item["change_type"]=="UPDATE":
                    values=db.query("select field_name,old_val,new_val from def_change_log where id="+str(item["id"])+
                                         " and table_name='"+str(item["table_name"])+"' and status='"+str(item["status"])+
                                         "' and date_of_change='"+str(item["date_of_change"])+"'").fetchall()
                    update_info_list=[]
                    for val in values:
                        update_info_list.append({"field_name":val["field_name"],"old_val":val["old_val"],"new_val":val["new_val"]})
                    audit_hist[i]["update_info"]=update_info_list

            return audit_hist

        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def get_record_detail(self,table_name,id):
        app.logger.info("Getting record detail")
        try:
            if table_name:
                record = self.db.query("select * from {} where id=%s".format(table_name,),(id,)).fetchone()
                return record
            else:
                return {"msg": "Invalid source object referred, please check."},400

        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500

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
                          "{0} approval, {1} rejection and {2} regress".format(approve,reject,regress)},200
        except Exception as e:
            app.logger.error(str(e))
            return {"msg": str(e)},500
