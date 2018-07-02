from app import *
from flask_restful import Resource,abort
from flask import Flask, request, redirect, url_for
from Helpers.DatabaseHelper import DatabaseHelper
from Controllers.ViewReportController import ViewReportController as report
import Helpers.utils as util
import json
from datetime import datetime
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *

class VarianceAnalysisController(Resource):
    def __init__(self):
        self.domain_info = autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.db=DatabaseHelper(self.tenant_info)

    @authenticate
    def get(self):
        if request.endpoint=='get_variance_country_suggestion_list':
            return self.get_country_suggestion_list()

        if request.endpoint=='get_variance_report_suggestion_list':
            country=request.args.get('country') if request.args.get('country') != None else 'ALL'
            return self.get_report_suggestion_list(country)

        if request.endpoint == 'get_variance_date_suggestion_list':
            report_id=request.args.get('report_id') if request.args.get('report_id') !=None else 'ALL'
            excluded_date=request.args.get('excluded_date')
            return self.get_date_suggestion_list(report_id,excluded_date)
        if request.endpoint == 'get_variance_report':
            report_id=request.args.get('report_id')
            first_reporting_date=request.args.get('first_date')
            subsequent_reporting_date=request.args.get('subsequent_date')
            variance_tolerance=request.args.get('variance_tolerance')
            return self.get_variance_report(report_id,first_reporting_date,subsequent_reporting_date,variance_tolerance)

    def get_country_suggestion_list(self):
        app.logger.info("Getting country suggestion list")

        try:
            country_list=self.db.query("select distinct country from report_def_catalog").fetchall()
            return country_list
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def get_report_suggestion_list(self,country):
        app.logger.info("Getting report suggestion list")
        try:

            where_clause=''
            if country!='ALL':
                where_clause=" where country= '"+country+"'"

            report_list=self.db.query("select distinct report_id from report_def_catalog " + where_clause).fetchall()
            return report_list
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def get_date_suggestion_list(self,report_id,excluded_date=None):
        app.logger.info("Getting date suggestion list")

        try:

            where_clause=" where 1=1 "
            if report_id!="ALL":
                where_clause+=" and report_id='"+report_id+"'"
            if excluded_date:
                where_clause+=" and  reporting_date != '"+excluded_date +"'"

            sql="select as_of_reporting_date,reporting_date,report_create_date from report_catalog " +where_clause

            date_list=self.db.query(sql).fetchall()
            for i,c in enumerate(date_list):
                for k,v in c.items():
                    if isinstance(v,datetime):
                        c[k] = c[k].isoformat()
            return date_list
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def get_variance_report(self,report_id, first_reporting_date,subsequent_reporting_date, variance_tolerance=0):
        app.logger.info("Creating variance report")
        try:
            self.report_id = report_id
            app.logger.info("Creating report for reporting date {}".format(first_reporting_date))
            first_report = report.render_report_json(self,reporting_date=first_reporting_date,cell_format_yn='Y')
            app.logger.info("Creating report for reporting date {}".format(subsequent_reporting_date))
            subsequent_report = report.render_report_json(self,reporting_date=subsequent_reporting_date,cell_format_yn='Y')
            for first_sheet,subsequent_sheet in zip(first_report,subsequent_report):
                for first_cell,subsequent_cell in zip(first_sheet['matrix'],subsequent_sheet['matrix']):
                    first_value = 0
                    subsequent_value = 0
                    if first_cell['origin'] == "DATA" :
                        first_value = first_cell['value']
                        subsequent_value = subsequent_cell['value']
                        first_cell['displayattribute'] = "variance"
                        first_cell['first_value'] = first_value
                        first_cell['subsequent_value'] = subsequent_value
                        first_cell['title'] = "P1: " + str(first_value) + " , P2: " + str(subsequent_value)
                        if first_value == subsequent_value or (first_value ==0 and subsequent_value == 0):
                            first_cell['variance'] = 0
                            first_cell['classname'] = ""
                        elif first_value ==0 and subsequent_value != 0:
                            first_cell['variance'] = "+ inf"
                            first_cell['classname'] = "red fa fa-caret-up"
                        # elif first_value !=0 and subsequent_value == 0:
                        #     first_cell['variance'] = "- ∞"
                        #     first_cell['classname'] = "red fa fa-caret-down"
                        else:
                            variance = util.round_value((subsequent_value/first_value -1)*100,"DECIMAL2")
                            first_cell['variance'] = variance
                            first_cell['classname'] = "red "  if abs(variance) > float(variance_tolerance) else "green "
                            first_cell['classname'] += " fa fa-caret-up" if variance > 0 else " fa fa-caret-down"
                        # else:
                        #     first_cell['variance'] = 0
                        #     first_cell['classname'] = ""

            return first_report
        except Exception as e:
            app.logger.error(e)
            return {"msg": e}, 500
