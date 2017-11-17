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
import pandas as pd

class DynamicReportController(Resource):
    def __init__(self):
        self.db=DatabaseHelper()

    def post(self):
        if request.endpoint=='create_dynamic_report_ep':
            report_info=request.get_json(force=True)
            report_id=report_info['report_id']
            business_date_from=report_info['business_date_from']
            business_date_to=report_info['business_date_to']
            print("Hi")
            self.create_report_detail(report_id,business_date_from,business_date_to)



    def create_report_detail(self,report_id,business_date_from,business_date_to):
        try:
            reporting_date=business_date_from+business_date_to
            #fetch all report definition for all sections of a report
            app.logger.info("Getting list of sections for report {}".format(report_id))
            sheets=self.db.query("select distinct sheet_id,section_id from report_dyn_def where report_id=%s",(report_id,)).fetchall()
            for sheet in sheets:
                dyn_def=self.db.query("select  * from report_dyn_def where report_id=%s and sheet_id=%s and section_id=%s and in_use='Y' ",
                                       (report_id,sheet['sheet_id'],sheet['section_id'])).fetchall()

                groupby={}
                rules={}
                for dyn in dyn_def:
                    if dyn['cell_render_def'] == "GROUPBY":
                        groupby[dyn['source_id']] = dyn['cell_calc_ref']

                    if dyn['cell_render_def'] == "RULES":
                        rules[dyn['source_id']]=dyn['cell_calc_ref']

                sources=groupby.keys()
                #print(list(sources))
                row_num = 0
                unique_records = {}
                data_records = []

                for source in sources:
                    #print(source)
                    source_table_name=self.db.query("select source_table_name from data_source_information where source_id=%s",
                                                    (source,)).fetchone()['source_table_name']
                    key_column=util.get_keycolumn(self.db._cursor(),source_table_name)
                    sql="select a.*,b.* from {0} a,qualified_data b where  a.{1} = b.qualifying_key \
                         and a.business_date=b.business_date and a.business_date between %s and %s \
                         and b.source_id=%s and a.in_use='Y' ".format(source_table_name,key_column)
                    source_data=self.db.query(sql,(business_date_from,business_date_to,source)).fetchall()
                    source_qd_list=[]

                    #print(source_data)

                    for qd in source_data:
                        #print(qd)
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
                            qd.update({'source_id':source,'report_id':report_id,'sheet_id':sheet['sheet_id'],
                                        'cell_id': '$' + str(unique_records[key]),'cell_calc_ref':sheet['section_id']+'$' + str(unique_records[key]),
                                       'reporting_date': reporting_date
                                       })
                            source_qd_list.append(qd)
                            data_records.append((source, report_id, sheet['sheet_id'],'$' + str(unique_records[key]),
                                                 sheet['section_id']+'$' + str(unique_records[key]), qd['buy_currency'], qd['sell_currency'],qd['mtm_currency'],
                                                 qd['qualifying_key'],  qd['business_date'],reporting_date))
                            #print(qd)
                        source_qd_frame=pd.DataFrame(source_qd_list)
                        #print(source_qd_frame)



                sql="insert into report_qualified_data_link(source_id,report_id,sheet_id,cell_id,cell_calc_ref,buy_currency,\
                        sell_currency,mtm_currency,qualifying_key,business_date,reporting_date) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                res=self.db.transactmany(sql,data_records)
                self.db.commit()

            #fetch all data from qualified data
            #for each entry in report definition assign each qualified data to a particular section and particular row
        except Exception as e:
            app.logger.error(e)
            print(e)
            return {"msg":e},500