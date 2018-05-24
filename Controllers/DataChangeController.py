from app import *
from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
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
        app.logger.info("Getting audit list history")
        try:
            sql = "SELECT * FROM data_change_log WHERE 1"
            if id_list == "id" or ((id_list is None or id_list == 'undefined') and (table_name is None or table_name=='undefined')):
                sql += " AND status='PENDING'"
            if id_list:
                sql += " and (origin_id,business_date) in (" + \
                            "select origin_id,business_date from data_change_log " + \
                            "where (id,business_date) in (" + id_list + ")" + \
                            ")"
                # sql = sql + " and (id,business_date) in (" + id_list + ") "
            elif business_date:
                sql = sql + " and business_date=" + business_date
            if table_name is not None and table_name != 'undefined':
                sql = sql + " and table_name = '" + table_name + "'"

            audit_list = self.db.query(sql).fetchall()
            for i,d in enumerate(audit_list):
                # app.logger.info('Processing index {}'.format(i))
                if d["change_type"]=='UPDATE':
                    d['update_info']=json.loads(d['change_summary'])
                for k,v in d.items():
                    if isinstance(v,datetime):
                        d[k] = d[k].isoformat()
                        # app.logger.info("{0} {1}".format(d[k], type(d[k])))
            return audit_list

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

                    for idx,ugrp in update_df.iterrows():
                        update_info = json.loads(ugrp['change_summary'])
                        updates += 1
                        update_list.append({"id": str(ugrp['id']),
                                            "prev_id": str(ugrp['prev_id']),
                                            "origin_id": str(ugrp['origin_id']),
                                            "table_name": ugrp['table_name'],
                                            "change_type": ugrp['change_type'],
                                            "date_of_change": ugrp['date_of_change'],
                                            "maker": ugrp['maker'],
                                            "maker_comment": ugrp['maker_comment'],
                                            "checker": ugrp['checker'],
                                            "checker_comment": ugrp['checker_comment'],
                                            "status": ugrp['status'],
                                            "date_of_checking": ugrp['date_of_checking'],
                                            "change_reference": ugrp['change_reference'],
                                            "group_id": str(ugrp['group_id']),
                                            "business_date": int(ugrp['business_date']),
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

    def update_or_delete_data(self,data,id):
        if data['change_type']=='DELETE':
            return self.delete_data(data,id)
        if data['change_type']=='UPDATE':
            return self.update_data(data,id)

    def create_new_record(self, data):
        try:
            table_name = data['table_name']
            change_type = data['change_type']
            app.logger.info("Inserting record for : {0} Change Event: {1}".format(table_name,change_type))

            update_info = data['update_info']
            # If id value is present set it to null
            update_info['id']=None
            update_info['dml_allowed']='N'
            update_info['in_use']='N'
            update_info_cols = ",".join(update_info.keys())
            update_info_placeholder = ",".join(["%s"] * len(update_info.keys()))
            params_tuple = tuple(update_info.values())

            sql = "insert into {0} ({1}) values ({2})".format(table_name,update_info_cols,update_info_placeholder)
            app.logger.info("Inserting new record: {0} {1}".format(sql,params_tuple))
            id = self.db.transact(sql, params_tuple)

            return id
        except Exception as e:
            app.logger.error(str(e))
            raise(e)

    def insert_data(self, data):
        try:
            app.logger.info("Insert data for source")
            table_name = data['table_name']
            change_type = data['change_type']
            id = self.create_new_record(data)
            self.audit_insert(data,id)
            self.db.commit()

            return {"msg": "Inserted record related version and audit data created for " + \
                            "{0} ID: {1}".format(table_name, id)},200
        except Exception as e:
            app.logger.error(str(e))
            self.db.rollback()
            return {"msg": str(e)},500

    def update_data(self, data, prev_id):
        try:
            app.logger.info("Update data for source table")
            table_name = data['table_name']
            change_type = data['change_type']

            id = self.create_new_record(data)
            self.audit_update(data,id,prev_id)
            self.update_approval_status(table_name=table_name, id=prev_id, dml_allowed='X')
            self.db.commit()

            return {"msg": "Update record related version and audit data created for " + \
                            "{0} ID: {1}".format(table_name, prev_id)},200
        except Exception as e:
            app.logger.error(str(e))
            self.db.rollback()
            return {"msg": str(e)},500

    def delete_data(self, data, prev_id):
        try:
            app.logger.info("Delete data for source table")
            table_name = data['table_name']
            change_type = data['change_type']

            self.audit_delete(data,prev_id)
            self.update_approval_status(table_name=table_name, id=prev_id, dml_allowed='N')
            self.db.commit()

            return {"msg": "Delete record related version and audit data created for " + \
                            "{0} ID: {1}".format(table_name, prev_id)},200
        except Exception as e:
            app.logger.error(str(e))
            self.db.rollback()
            return {"msg": str(e)},500

    def audit_insert(self, data, id):
        try:
            app.logger.info("Data Change controller audit info insert")
            audit_info = data['audit_info']
            sql = "insert into  data_change_log " + \
            "(id,prev_id,origin_id,table_name,change_type,maker_comment,status,change_reference," + \
            "date_of_change,maker,business_date,group_id)" + \
            " values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            res = self.db.transact(sql, (id, id, id, audit_info['table_name'], audit_info['change_type'], \
                    audit_info['comment'],'PENDING', audit_info['change_reference'], datetime.now(), \
                    audit_info['maker'],audit_info['business_date'],audit_info['group_id']))

        except Exception as e:
            app.logger.error(str(e))
            raise(e)

    def audit_update(self, data, id, prev_id):
        try:
            app.logger.info("Data Change controller audit info update")
            audit_info = data['audit_info']
            update_info = data['update_info']
            table_name = data['table_name']

            # Now get the origin id for the record if there is any existing entry in data change log
            sql="select origin_id from data_change_log where table_name='{0}' and id={1}".format(table_name,prev_id)
            origin_id=self.db.query(sql).fetchone()
            # If no previous entry found, that means the record is updated for the very first time, so
            # use the prev (present) id as origin_id to be casted for subsequent changes
            if not origin_id:
                origin_id = prev_id
            else:
                origin_id = origin_id['origin_id']

            change_summary = []
            old_rec=self.db.query("select * from "+ table_name+" where id="+str(prev_id)).fetchone()
            for col in update_info.keys():
                old_val = old_rec[col]
                new_val = update_info[col]
                if old_val != new_val and col not in ('dml_allowed','in_use','id'):
                    change_summary.append({'field_name': col, 'old_val': old_val,'new_val':new_val})

            sql = "insert into  data_change_log " + \
            "(id,prev_id,origin_id,table_name,change_type,maker_comment,status,change_reference," + \
            "date_of_change,maker,business_date,group_id, change_summary)" + \
            " values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            res = self.db.transact(sql, (id, prev_id, origin_id, audit_info['table_name'], audit_info['change_type'], \
                    audit_info['comment'],'PENDING', audit_info['change_reference'], datetime.now(), \
                    audit_info['maker'],audit_info['business_date'],audit_info['group_id'],
                    json.dumps(change_summary)))

        except Exception as e:
            app.logger.error(str(e))
            raise(e)

    def audit_delete(self, data, prev_id):
        try:
            app.logger.info("Data Change controller audit info delete")
            table_name = data['table_name']
            audit_info = data['audit_info']
            # Now get the origin id for the record if there is any existing entry in data change log
            sql="select origin_id from data_change_log where table_name='{0}' and id={1}".format(table_name,prev_id)
            origin_id=self.db.query(sql).fetchone()
            # If no previous entry found, that means the record is updated for the very first time, so
            # use the prev (present) id as origin_id to be casted for subsequent changes
            if not origin_id:
                origin_id = prev_id
            else:
                origin_id = origin_id['origin_id']

            sql = "insert into  data_change_log " + \
            "(id,prev_id,origin_id,table_name,change_type,maker_comment,status,change_reference," + \
            "date_of_change,maker,business_date,group_id)" + \
            " values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            res = self.db.transact(sql, (prev_id, prev_id, origin_id, audit_info['table_name'], audit_info['change_type'], \
                    audit_info['comment'],'PENDING', audit_info['change_reference'], datetime.now(), \
                    audit_info['maker'],audit_info['business_date'],audit_info['group_id']))

        except Exception as e:
            app.logger.error(str(e))
            raise(e)

    def update_approval_status(self,table_name,id,dml_allowed,in_use=None):
        try:
            app.logger.info("Inside update approval status for {0} {1}".format(table_name,id))
            sql = "update " + table_name + " set dml_allowed='"+dml_allowed+"'"
            if in_use is not None:
                sql+=",in_use='"+in_use+"' "
            sql+= "  where id=%s"
            res = self.db.transact(sql, (id,))

        except Exception as e:
            app.logger.error(str(e))
            raise(e)

    def update_audit_record(self,data):
        try:
            app.logger.info("Updateing audit record for data audit decision")
            sql="update data_change_log set status=%s,checker_comment=%s,date_of_checking=%s,checker=%s where table_name=%s and id=%s and status='PENDING'"
            params=(data["status"],data["checker_comment"],datetime.now(),data["checker"],data["table_name"],data["id"])
            self.db.transact(sql,params)

        except Exception as e:
            app.logger.error(str(e))
            raise(e)

    def reject_dml(self,data):
        try:
            app.logger.info("AuditDicision reject dml")
            if data["change_type"]=="INSERT":
                self.update_approval_status(table_name=data["table_name"],id=data["id"],dml_allowed='Y',in_use='N')
            elif data["change_type"]=="UPDATE":
                self.update_approval_status(table_name=data["table_name"], id=data["prev_id"], dml_allowed='Y')
                self.update_approval_status(table_name=data["table_name"], id=data["id"], dml_allowed='X',in_use='X')
            elif data["change_type"]=="DELETE":
                self.update_approval_status(table_name=data["table_name"], id=data["id"], dml_allowed='Y')
            self.update_audit_record(data)
            self.db.commit()

        except Exception as e:
            app.logger.error(str(e))
            raise(e)

    def regress_dml(self,data):
        try:
            app.logger.info("AuditDicision regress dml")
            if data["change_type"]=="INSERT":
                self.update_approval_status(table_name=data["table_name"],id=data["id"],dml_allowed='Y',in_use='N')
            elif data["change_type"]=="UPDATE":
                self.update_approval_status(table_name=data["table_name"], id=data["prev_id"], dml_allowed='Y')
                self.update_approval_status(table_name=data["table_name"], id=data["id"], dml_allowed='X',in_use='X')
            elif data["change_type"]=="DELETE":
                self.update_approval_status(table_name=data["table_name"], id=data["id"], dml_allowed='Y')
            self.update_audit_record(data)
            self.db.commit()

        except Exception as e:
            app.logger.error(str(e))
            raise(e)


    def approve_dml(self,data):
        try:
            app.logger.info("AuditDicision approval dml")
            if data["change_type"]=="INSERT":
                self.update_approval_status(table_name=data["table_name"],id=data["id"],dml_allowed='Y',in_use='Y')
            elif data["change_type"]=="DELETE":
                self.update_approval_status(table_name=data["table_name"], id=data["id"], dml_allowed='N',in_use='N')
            elif data["change_type"] == "UPDATE":
                self.update_approval_status(table_name=data["table_name"], id=data["prev_id"], dml_allowed='X',in_use='X')
                self.update_approval_status(table_name=data["table_name"], id=data["id"], dml_allowed='Y',in_use='Y')

            self.update_audit_record(data)
            self.db.commit()

        except Exception as e:
            app.logger.error(str(e))
            raise(e)


    def audit_decision(self,data_list):
        try:
            approve=0
            reject=0
            regress=0
            for key in data_list.keys():
                for data in data_list[key]:
                    app.logger.info("Inside audit decision for status {}".format(data["status"]))
                    if data["status"]=="REJECTED":
                        self.reject_dml(data)
                        reject+=1
                    if data["status"]=="APPROVED":
                        self.approve_dml(data)
                        approve+=1
                    if data["status"]=="REGRESSED":
                        self.regress_dml(data)
                        regress+=1
            return {"msg":"Successfully processed reviews for the following: " + \
                          "{0} approval, {1} rejection and {2} regress".format(approve,reject,regress)},200
        except Exception as e:
            app.logger.error(str(e))
            return {"msg": str(e)},500
