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
                first_source=True

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
                    for col in source_qd_frame.columns:
                        source_qd_frame[col]=source_qd_frame[col].astype(dtype=float,errors='ignore')
                    #print(source_qd_frame)

                    sql="select * from report_dyn_calc_def where source_id=%s and report_id=%s and sheet_id=%s and section_id=%s and in_use='Y' "
                    dyn_calc_def=self.db.query(sql,(source,report_id,sheet['sheet_id'],sheet['section_id'])).fetchall()
                    summ_by_src=pd.DataFrame(columns=['source_id','report_id','sheet_id','cell_id','cell_calc_ref','cell_summary','reporting_date'])

                    column_list={}
                    for calc_def in dyn_calc_def:
                        column_list.update({calc_def['aggregation_ref']:calc_def['column_id']})


                    summ_by_src_table=pd.DataFrame(columns=list(column_list.values()).append('row'))


                    for idx,grp in source_qd_frame.groupby('cell_calc_ref'):
                        table_data={}
                        for calc_def in dyn_calc_def:
                            expr_str="grp['{0}'].".format(calc_def['aggregation_ref'])
                            if calc_def['aggregation_func']=='distinct':
                                expr_str+="unique()[0]"
                            else:
                                expr_str+=calc_def['aggregation_func']+"()"
                            summary=eval(expr_str)
                            cell_id=grp['cell_id'].unique()[0].replace('$',calc_def['column_id'])
                            cell_calc_ref=grp['cell_calc_ref'].unique()[0].replace('$',calc_def['column_id'])
                            data={'source_id':source,'report_id':report_id,'sheet_id':sheet['sheet_id'],
                                  'cell_id':cell_id,'cell_calc_ref':cell_calc_ref,'cell_summary':summary,'reporting_date':reporting_date}
                            summ_by_src=summ_by_src.append(data,ignore_index=True)
                            table_data.update({column_list[calc_def['aggregation_ref']]:summary})
                        row_num=grp['cell_id'].unique()[0].replace('$','')
                        table_data.update({'row':row_num})
                        summ_by_src_table=summ_by_src_table.append(table_data,ignore_index=True)
                        if first_source :
                            summ_all_src_table=summ_by_src_table
                        else:
                            summ_all_src_table=summ_all_src_table.append(summ_by_src_table)


                    sql="insert into report_summary_by_source(source_id,report_id,sheet_id,cell_id,cell_calc_ref,cell_summary,reporting_date) \
                        values(%s,%s,%s,%s,%s,%s,%s)"
                    res=self.db.transactmany(sql,summ_by_src.to_records(index=False).tolist())

                #print(summ_all_src_table.groupby('row'))

                summ_all_src_table_final=pd.DataFrame(data=None,columns=summ_all_src_table.columns)
                for idx,grp in summ_all_src_table.groupby('row'):
                    #print(grp)
                    table_data = {}
                    for calc_def in dyn_calc_def:
                        expr_str = "grp['{0}'].".format(column_list[calc_def['aggregation_ref']])
                        if calc_def['aggregation_func'] == 'distinct':
                            expr_str += "unique()[0]"
                        else:
                            expr_str += calc_def['aggregation_func'] + "()"
                        summary = eval(expr_str)
                        #print(summary)
                        table_data.update({column_list[calc_def['aggregation_ref']]: summary})
                    row_num = grp['row'].unique()[0]
                    table_data.update({'row': row_num})
                    #print(table_data)
                    summ_all_src_table_final=summ_all_src_table_final.append(table_data,ignore_index=True)
                    #print(summ_all_src_table_final)

                #print(summ_all_src_table_final)
                summ_list=[]
                for row in summ_all_src_table_final.to_dict(orient='records'):
                    for col in summ_all_src_table_final.columns:
                        if col =='row':
                            continue
                        cell_id=col+row['row']
                        summary=row[col]
                        summ_list.append((report_id,sheet['sheet_id'],cell_id,summary,reporting_date))


                sql="insert into report_summary(report_id,sheet_id,cell_id,cell_summary,reporting_date) values(%s,%s,%s,%s,%s)"
                res=self.db.transactmany(sql,summ_list)

                sql="insert into report_qualified_data_link(source_id,report_id,sheet_id,cell_id,cell_calc_ref,buy_currency,\
                        sell_currency,mtm_currency,qualifying_key,business_date,reporting_date) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                res=self.db.transactmany(sql,data_records)
                self.db.commit()


        except Exception as e:
            app.logger.error(e)
            #print(e)
            return {"msg":e},500