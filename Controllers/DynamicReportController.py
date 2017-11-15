from app import *
from flask_restful import Resource,abort
import os
from flask import Flask, request, redirect, url_for
from werkzeug.utils import secure_filename
import uuid
from Constants.Status import *
from Helpers.DatabaseHelper import DatabaseHelper
import datetime
import openpyxl as xls
import Helpers.utils as util
from openpyxl.styles import Border, Side, PatternFill, Font, GradientFill, Alignment, Protection
from openpyxl.utils import get_column_letter
import Helpers.utils as util
import json
import ast
from operator import itemgetter
from datetime import datetime

class DynamicReportController(Resource):
    def __init__(self):
        self.db=DatabaseHelper()



    def create_report_detail(self,report_id,business_date_from,business_date_to):
        try:
            reporting_date=business_date_from+business_date_to
            #fetch all report definition for all sections of a report
            sheets=self.db.query("select distinct sheet_id,section_id from report_dyn_def where report_id=%s",(report_id,)).fetchall()
            for sheet in sheets:
                dyn_def=self.db.query("select  * from report_dyn_def where report_id=%s and sheet_id=%s and section_id=%s and in_use='Y' ",
                                       (report_id,sheet['sheet_id'],sheet['section_id'])).fetchall()

                groupby={}
                rules={}
                for dyn in dyn_def:
                    groupby[dyn['source_id']] = dyn['cell_calc_ref'] if dyn['cell_render_def']=="GROUPBY"
                    rules[dyn['source_id']]=dyn['cell_calc_ref'] if dyn['cell_render_def']=="RULES"

                sources=groupby.keys()
                row_num = 0
                unique_records = {}
                data_records = []

                for source in sources:
                    source_table_name=self.db.query("select source_table_name from data_source_information where source_id=%s",
                                                    (source,)).fetchone()['source_table_name']
                    qualified_data=self.db.query("select * from {} where business_date between %s and %s ".format(source_table_name),
                                                 (business_date_from,business_date_to)).fetchall()


                    for qd in qualified_data:
                        if set(rules[source].split(',')).issubset(set(qd['business_rules'].split(','))):
                            key=''
                            for col in groupby[source].split(','):
                                key+=qd[col]
                            if key not in unique_records.keys():
                                row_num+=1
                                unique_records[key]=row_num

                            # data_records.append({'source_id':source,'report_id':report_id,'sheet_id':sheet['sheet_id'],
                            #                      'cell_id': '$' + unique_records[key],'cell_calc_ref':sheet['section_id'],
                            #                      'qualifying_key':qd['qualifying_key'],
                            #                      'buy_currency':qd['buy_currency'],'sell_currency':qd['sell_currnecy'],
                            #                      'mtm_currency':qd['mtm_currency'],'business_date':qd['business_date'],
                            #                      'reporting_date':reporting_date})

                            data_records.append((source, report_id, sheet['sheet_id'],'$' + unique_records[key],
                                                 sheet['section_id'], qd['buy_currency'], qd['sell_currnecy'],qd['mtm_currency'],
                                                 qd['qualifying_key'],  qd['business_date'],reporting_date))

                sql="insert into qualified_data_link(source_id,report_id,sheet_id,cell_id,cell_calc_ref,buy_currency,\
                        sell_currency,mtm_currency,qualifying_key,business_date,reporting_date) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,)"

                res=self.db.transactmany(sql,data_records)
                self.db.commit()

            #fetch all data from qualified data
            #for each entry in report definition assign each qualified data to a particular section and particular row
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500