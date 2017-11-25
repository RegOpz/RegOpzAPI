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
from Helpers.Tree import tree

class DynamicReportController(Resource):
    def __init__(self):
        self.db=DatabaseHelper()

    def post(self):
        if request.endpoint=='create_dynamic_report_ep':
            report_info=request.get_json(force=True)
            report_id=report_info['report_id']
            business_date_from=report_info['business_date_from']
            business_date_to=report_info['business_date_to']
            return self.create_report_detail(report_id,business_date_from,business_date_to)



    def create_report_detail(self,report_id,business_date_from,business_date_to):
        try:
            reporting_date=business_date_from+business_date_to
            # First clean the tables , if any existing data, before generating report
            util.clean_table(self.db._cursor(), 'report_qualified_data_link', '', reporting_date,'report_id=\''+ report_id + '\'')
            util.clean_table(self.db._cursor(), 'report_summary_by_source', '', reporting_date,'report_id=\''+ report_id + '\'')
            util.clean_table(self.db._cursor(), 'report_summary', '', reporting_date, 'report_id=\''+ report_id + '\'')
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
                        if dyn['source_id'] in rules.keys():
                            rules[dyn['source_id']].append(dyn['cell_calc_ref'])
                        else:
                            rules[dyn['source_id']]=[dyn['cell_calc_ref']]

                sources=groupby.keys()
                #print(list(sources))
                # Build the dyn comp agg def dictionary ref
                dyn_comp_agg_def=self.db.query("select  * from report_dyn_comp_agg_def where report_id=%s and sheet_id=%s and section_id=%s and in_use='Y' ",
                                       (report_id,sheet['sheet_id'],sheet['section_id'])).fetchall()

                agg_summary={}
                for dyn in dyn_comp_agg_def:
                    agg_summary[dyn['column_id']] = {'comp_agg_ref': dyn['comp_agg_ref'],
                                                    'comp_agg_rule': dyn['comp_agg_rule'],
                                                    'reporting_scale': dyn['reporting_scale'],
                                                    'rounding_option': dyn['rounding_option']
                                                    }

                row_num = 0
                unique_records = {}
                data_records = []
                comp_agg_def_cls={}
                first_source=True

                for source in sources:
                    #print(source)
                    source_table_name=self.db.query("select source_table_name from data_source_information where source_id=%s",
                                                    (source,)).fetchone()['source_table_name']
                    key_column=util.get_keycolumn(self.db._cursor(),source_table_name)

                    # Create separate function for this purpose later
                    # to get the column_list and calc_column_ref_list similar to get_list_of_columns_for_dataframe
                    # used in generate static report
                    sql="select * from report_dyn_calc_def where source_id=%s and report_id=%s and sheet_id=%s and section_id=%s and in_use='Y' "
                    dyn_calc_def=self.db.query(sql,(source,report_id,sheet['sheet_id'],sheet['section_id'])).fetchall()

                    column_list={}
                    calc_column_ref_list=''
                    cell_calc_ref_list={}
                    for calc_def in dyn_calc_def:
                        column_list.update({calc_def['aggregation_ref']:calc_def['column_id']})
                        calc_column_ref_list+= '/' + calc_def['aggregation_ref']
                        cell_calc_ref_list[calc_def['column_id']]=calc_def['calc_ref']

                    table_def = self.db.query("describe " + source_table_name).fetchall()
                    table_col_list=''
                    calc_column_ref_list += '/' + groupby[source].replace(',','/')
                    for col in table_def:
                        if col['Field'] in calc_column_ref_list:
                            if table_col_list == '':
                                table_col_list = col['Field']
                            else:
                                table_col_list += ',' + col['Field']
                    table_col_list=(table_col_list,'1 as const')[table_col_list=='']

                    select_column_list = ''
                    select_column_list = key_column + ',' + table_col_list
                    select_column_list = 'a.' + select_column_list.replace(',',',a.')

                    sql="select {2},b.* from {0} a,qualified_data b where  a.{1} = b.qualifying_key \
                         and a.business_date=b.business_date and a.business_date between %s and %s \
                         and b.source_id=%s and a.in_use='Y' ".format(source_table_name,key_column,select_column_list)
                    source_data=self.db.query(sql,(business_date_from,business_date_to,source)).fetchall()
                    source_qd_list=[]

                    #print(source_data)

                    for qd in source_data:
                        #print(qd)
                        for rule in rules[source]:
                            if set(rule.split(',')).issubset(set(qd['business_rules'].split(','))):
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
                                # cell_calc_ref is build assuming the following section_id+'S'+source_id+'$'+record row
                                qd.update({'source_id':source,'report_id':report_id,'sheet_id':sheet['sheet_id'],
                                            'cell_id': '$' + str(unique_records[key]),
                                           'reporting_date': reporting_date
                                           })
                                source_qd_list.append(qd)
                                data_records.append((source, report_id, sheet['sheet_id'],'$' + str(unique_records[key]),
                                                     sheet['section_id']+'$' + str(unique_records[key]), qd['buy_currency'], qd['sell_currency'],qd['mtm_currency'],
                                                     qd['qualifying_key'],  qd['business_date'],reporting_date))
                                #print(qd)
                                # Ensure we are not accounting the same record multiple times
                                # if all rules are applicable for same qd line
                                break
                    # Let's insert qualified link data records
                    # Need to insert as chunks to avoid merroy limit issue
                    sql="insert into report_qualified_data_link(source_id,report_id,sheet_id,cell_id,cell_calc_ref,buy_currency,\
                            sell_currency,mtm_currency,qualifying_key,business_date,reporting_date) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

                    res=self.db.transactmany(sql,data_records)

                    # Release memory of qualified data link data_records
                    data_records=None
                    source_data=None

                    source_qd_frame=pd.DataFrame(source_qd_list)
                    for col in source_qd_frame.columns:
                        source_qd_frame[col]=source_qd_frame[col].astype(dtype=float,errors='ignore')
                    #print(source_qd_frame)

                    summ_by_src=pd.DataFrame(columns=['source_id','report_id','sheet_id','cell_id','cell_calc_ref','cell_summary','reporting_date'])
                    # summ_by_src_table=pd.DataFrame(columns=list(column_list.values()).append(['row'] + [c + '_ref' for c in column_list.values()]))


                    for idx,grp in source_qd_frame.groupby('cell_id'):
                        # table_data={}
                        row_num=grp['cell_id'].unique()[0].replace('$','')
                        for calc_def in dyn_calc_def:
                            expr_str="grp['{0}'].".format(calc_def['aggregation_ref'])
                            if calc_def['aggregation_func']=='distinct':
                                expr_str+="unique()[0]"
                            else:
                                expr_str+=calc_def['aggregation_func']+"()"
                            summary=eval(expr_str)
                            cell_id=calc_def['column_id'] + str(row_num)
                            cell_calc_ref=cell_calc_ref_list[calc_def['column_id']].replace('$',str(row_num))
                            data={'source_id':source,'report_id':report_id,'sheet_id':sheet['sheet_id'],
                                  'cell_id':cell_id,'cell_calc_ref':cell_calc_ref,'cell_summary':summary,'reporting_date':reporting_date}
                            summ_by_src=summ_by_src.append(data,ignore_index=True)
                            comp_agg_def_cls[cell_calc_ref] = {
                                    'formula': summary,
                                    'reporting_scale': 1,
                                    'rounding_option': "NONE",
                                    'type': 'NA',
                                    'report_id':report_id,
                                    'sheet_id':sheet['sheet_id'],
                                    'cell_id': cell_id,
                                    'column_id': calc_def['column_id'],
                                    'contributor': 'SOURCE'
                                    }
                            agg_ref = agg_summary[calc_def['column_id']]['comp_agg_ref']+str(row_num)
                            if agg_ref in comp_agg_def_cls.keys():
                                comp_agg_def_cls[agg_ref]['formula'] = comp_agg_def_cls[agg_ref]['formula'].replace(cell_calc_ref_list[calc_def['column_id']],cell_calc_ref)
                                comp_agg_def_cls[agg_ref]['cell_summary'] = comp_agg_def_cls[agg_ref]['cell_summary'].replace(cell_calc_ref_list[calc_def['column_id']],str(summary))
                            else:
                                comp_agg_def_cls[agg_ref] = {
                                        'formula': agg_summary[calc_def['column_id']]['comp_agg_rule'].replace(cell_calc_ref_list[calc_def['column_id']],cell_calc_ref),
                                        'cell_summary': agg_summary[calc_def['column_id']]['comp_agg_rule'].replace(cell_calc_ref_list[calc_def['column_id']],str(summary)),
                                        'reporting_scale': agg_summary[calc_def['column_id']]['reporting_scale'] if agg_summary[calc_def['column_id']]['reporting_scale'] else 1,
                                        'rounding_option': agg_summary[calc_def['column_id']]['rounding_option'],
                                        'type': 'NUMBER' if agg_summary[calc_def['column_id']]['reporting_scale'] else 'TEXT',
                                        'report_id':report_id,
                                        'sheet_id':sheet['sheet_id'],
                                        'cell_id': cell_id,
                                        'column_id': calc_def['column_id'],
                                        'contributor': 'AGGDEF'
                                }
                        #     table_data.update({column_list[calc_def['aggregation_ref']]:summary,
                        #                         column_list[calc_def['aggregation_ref']]+'_ref':cell_calc_ref})
                        # table_data.update({'row':row_num})
                        # print(table_data)
                        # summ_by_src_table=summ_by_src_table.append(table_data,ignore_index=True)


                    sql="insert into report_summary_by_source(source_id,report_id,sheet_id,cell_id,cell_calc_ref,cell_summary,reporting_date) \
                        values(%s,%s,%s,%s,%s,%s,%s)"
                    res=self.db.transactmany(sql,summ_by_src.to_records(index=False).tolist())

                # if first_source :
                #     summ_all_src_table=summ_by_src_table
                #     first_source=False
                # else:
                #     summ_all_src_table=summ_all_src_table.append(summ_by_src_table)
                # #print(summ_all_src_table.groupby('row'))
                #
                # summ_all_src_table_final=pd.DataFrame(data=None,columns=summ_all_src_table.columns)
                # #Lets build the contributors for the summary final calculation
                # for idx,grp in summ_all_src_table.groupby('row'):
                #     #print(grp)
                #     table_data = {}
                #     for calc_def in dyn_calc_def:
                #         expr_str = "grp['{0}'].".format(column_list[calc_def['aggregation_ref']])
                #         if calc_def['aggregation_func'] == 'distinct':
                #             expr_str += "unique()[0]"
                #         else:
                #             expr_str += calc_def['aggregation_func'] + "()"
                #         summary = eval(expr_str)
                #         # print(expr_str,summary)
                #         table_data.update({column_list[calc_def['aggregation_ref']]: summary})
                #     row_num = grp['row'].unique()[0]
                #     table_data.update({'row': row_num})
                #     #print(table_data)
                #     summ_all_src_table_final=summ_all_src_table_final.append(table_data,ignore_index=True)
                #     #print(summ_all_src_table_final)
                #
                # #print(summ_all_src_table_final)
                # summ_list=[]
                # for row in summ_all_src_table_final.to_dict(orient='records'):
                #     for col in summ_all_src_table_final.columns:
                #         if col =='row':
                #             continue
                #         cell_id=col+row['row']
                #         summary=row[col]
                #         summ_list.append((report_id,sheet['sheet_id'],cell_id,summary,reporting_date))
                #
                #
                # sql="insert into report_summary(report_id,sheet_id,cell_id,cell_summary,reporting_date) values(%s,%s,%s,%s,%s)"
                # res=self.db.transactmany(sql,summ_list)
                summary_set = tree(comp_agg_def_cls, format_flag='Y')
                summary_final=[]
                for key in comp_agg_def_cls.keys():
                    if comp_agg_def_cls[key]['contributor'] == 'AGGDEF':
                        summary_final.append((comp_agg_def_cls[key]['report_id'],
                            comp_agg_def_cls[key]['sheet_id'],
                            comp_agg_def_cls[key]['cell_id'],
                            summary_set[key] if comp_agg_def_cls[key]['type']=='NUMBER' else comp_agg_def_cls[key]['cell_summary'],
                            reporting_date
                            ))
                        # print(comp_agg_def_cls[key]['report_id'],
                        #     comp_agg_def_cls[key]['sheet_id'],
                        #     comp_agg_def_cls[key]['cell_id'],
                        #     summary_set[key] if comp_agg_def_cls[key]['type']=='NUMBER' else comp_agg_def_cls[key]['cell_summary']
                        #     )
                sql="insert into report_summary(report_id,sheet_id,cell_id,cell_summary,reporting_date) values(%s,%s,%s,%s,%s)"
                res=self.db.transactmany(sql,summary_final)


                self.db.commit()
            return {"msg":"Dynamic report generation completed successfully"},200


        except Exception as e:
            app.logger.error(e)
            #print(e)
            return {"msg":e},500
