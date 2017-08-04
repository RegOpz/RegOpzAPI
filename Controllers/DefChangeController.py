from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.DatabaseOps import DatabaseOps
from Helpers.AuditHelper import AuditHelper
import csv
import time
from datetime import datetime
from Constants.Status import *
from operator import itemgetter

class DefChangeController(Resource):
    def __init__(self):
        self.db=DatabaseHelper()
        self.audit=AuditHelper('def_change_log')


    def get(self):
        if request.endpoint=="get_audit_list":
            table_name=request.args.get("table_name")
            id_list=request.args.get("id_list")
            return self.get_audit_list(id_list,table_name)
        if request.endpoint=="get_record_detail":
            table_name=request.args.get("table_name")
            id=request.args.get("id")
            return self.get_record_detail(table_name,id)

    def post(self):
        data=request.get_json(force=True)
        return self.audit_decision(data)


    def get_audit_list(self,id_list=None,table_name=None):
        return self.audit.get_audit_list(id_list,table_name)

    def get_record_detail(self,table_name,id):
        record_detail=self.db.query("select * from "+table_name+" where id="+str(id)).fetchone()
        return record_detail

    def audit_decision(self,data):
        if data["status"]=="REJECTED":
            self.audit.reject_dml(data)
        if data["status"]=="APPROVED":
            self.audit.approve_dml(data)
        if data["status"]=="REGRESSED":
            self.audit.regress_dml(data)
        return data
