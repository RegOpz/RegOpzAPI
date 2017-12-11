from app import *
from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.DatabaseOps import DatabaseOps
from Helpers.AuditHelper import AuditHelper

class DataChangeController(Resource):
    def __init__(self):
        self.db=DatabaseHelper()
        self.audit=AuditHelper('data_change_log')

    def get(self):
        if request.endpoint=="get_data_audit_list":
            table_name=request.args.get("table_name")
            id_list=request.args.get("id_list")
            business_date=request.args.get("business_date")
            return self.get_audit_list(id_list,table_name,business_date)
        if request.endpoint=="get_data_record_detail":
            table_name=request.args.get("table_name")
            id=request.args.get("id")
            return self.get_record_detail(table_name,id)

    def post(self):
        data=request.get_json(force=True)
        return self.audit_decision(data)


    def get_audit_list(self,id_list = None,table_name = None,business_date = None):
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

    def get_record_detail(self,table_name,id):
        app.logger.info("Getting record detail")
        try:
            record_detail=self.db.query("select * from "+table_name+" where id="+str(id)).fetchone()
            return record_detail
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def audit_decision(self,data):
        try:
            app.logger.info("Inside audit decision for status {}".format(data["status"]))
            if data["status"]=="REJECTED":
                self.audit.reject_dml(data)
            if data["status"]=="APPROVED":
                self.audit.approve_dml(data)
            if data["status"]=="REGRESSED":
                self.audit.regress_dml(data)
            return {"msg":"Successfully {0} for {1}".format(data["status"],data)},200
        except Exception as e:
            app.logger.error(str(e))
            return {"msg": str(e)},500
