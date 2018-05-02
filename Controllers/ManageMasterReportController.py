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
from Helpers.AuditHelper import AuditHelper
from Models.Token import Token

class ManageMasterReportController(Resource):

    def __init__(self):
        self.master_db=DatabaseHelper()
        self.domain_info = autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.tenant_db=DatabaseHelper(self.tenant_info)
            self.tenant_audit=AuditHelper('def_change_log',self.tenant_info)
            self.user_id=Token().authenticate()

    def get(self,country=None):
        return self.report_template_catalog_list(country)

    def copy_template_to_tenant(self,country,report_id,comment,overwrite=False):

        try:
            app.logger.info("Checking if report template already present in tenant space.")
            existing_record=self.tenant_db.query("select * from report_def_catalog where report_id=%s",(report_id,)).fetchone()
            if existing_record:
                components=existing_record['report_components'].split(',')
                template_present=('TEMPLATE'in components)
                if template_present and not overwrite:
                    return {"msg":"Template for report {} alreday exists.".format(report_id)},500

            app.logger.info("Fetching report template from master space.")
            report_template=self.master_db.query("select * from report_def_master where country=%s and report_id=%s",(country,report_id)).fetchall()

            if template_present and overwrite:
               app.logger.info("Erasing existing report template.")
               self.tenant_db.transact("delete from report_def where report_id=%s",(report_id,))
               components.remove('TEMPLATE')
               components_list=",".join(components)
               self.tenant_db.transact("update report_def_catalog set report_components=%s where report_id=%s",(components_list,report_id))

            for rec in report_template:
                id=self.tenant_db.transact("insert into report_def(report_id,sheet_id,cell_id,cell_render_def,cell_calc_ref) values(%s,%s,%s,%s,%s)",
                                           rec["report_id"],rec["sheet_id"],rec["cell_id"],rec["cell_render_def"],rec["cell_calc_ref"])

                audit_info = {
                    "table_name": "report_def",
                    "change_type": "INSERT",
                    "comment": comment,
                    "change_reference": "Copying report template from master database",
                    "maker": self.user_id
                }

                app.logger.info("Inserting audit info")
                self.tenant_audit.audit_insert({"audit_info": audit_info}, id)
                self.tenant_db.commit()

            return {"msg":"Template successfully copied into tenant space."},200

        except Exception as e:
            self.tenant_db.rollback()
            app.logger.error(str(e))
            return {"msg":str(e)},500


    def report_template_catalog_list(self,country='ALL'):

        try:
            data_dict=[]
            where_clause = ''

            sql = "select distinct country from report_def_catalog_master where 1 "
            if country is not None and country !='ALL':
                 where_clause =  " and country = '{}'".format(country.upper())

            country = self.master_db.query(sql + where_clause).fetchall()

            if country:
                for i,c in enumerate(country):
                    sql = "select * from report_def_catalog_master where country = %s"
                    report = self.master_db.query(sql,(c['country'],)).fetchall()
                    data_dict.append({'country': c['country'], 'report': report})

            return data_dict

        except Exception as e:
            self.tenant_db.rollback()
            app.logger.error(str(e))
            return {"msg":str(e)},500
