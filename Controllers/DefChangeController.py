from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.DatabaseOps import DatabaseOps
import csv
import time
from datetime import datetime
from Constants.Status import *
from operator import itemgetter

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

    def post(self):
        data=request.get_json(force=True)
        return self.audit_decision(data)


    def get_audit_list(self):
        audit_list=self.db.query("select distinct id,table_name,change_type,\
                                date_of_change,maker,maker_comment,checker,checker_comment,status,date_of_checking\
                                 from def_change_log where status='PENDING'").fetchall()

        for idx,item in enumerate(audit_list):
            if item["change_type"]=="UPDATE":
                values=self.db.query("select field_name,old_val,new_val from def_change_log  where id="+str(item["id"])+
                                     " and table_name='"+str(item["table_name"])+"'").fetchall()
                update_info_list=[]
                for val in values:
                    update_info_list.append({"field_name":val["field_name"],"old_val":val["old_val"],"new_val":val["new_val"]})
                audit_list[idx]["update_info"]=update_info_list

        return audit_list

    def get_record_detail(self,table_name,id):
        record_detail=self.db.query("select * from "+table_name+" where id="+str(id)).fetchone()
        return record_detail

    def audit_decision(self,data):
        if data["status"]=="APPROVED":
            print(data)
        if data["status"]=="REJECTED":
            print(data)
        return data

