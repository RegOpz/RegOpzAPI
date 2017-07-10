from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.DatabaseOps import DatabaseOps
import csv
import time
from datetime import datetime
from Constants.Status import *

class DefChangeController(Resource):
    def __init__(self):
        self.db=DatabaseHelper()

    def get(self):
        if request.endpoint=="get_audit_list":
            return self.get_audit_list()
        if request.endpoint=="get_record_detail":
            table_name=request.args.get("table_name")
            id=request.args.get("id")
            return self.get_record_detail(table_name,id)

    def get_audit_list(self):
        pending_aduit_list=self.db.query("select * from def_change_log where status='PENDING'").fetchall()
        return pending_aduit_list

    def get_record_detail(self,table_name,id):
        record_detail=self.db.query("select * from "+table_name+" where id="+str(id)).fetchone()
        return record_detail

