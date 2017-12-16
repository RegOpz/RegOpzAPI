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
            sheets=self.db.query("select distinct sheet_id from report_dyn_def where report_id=%s",(report_id,)).fetchall()
            for sheet in sheets:
                sheet_id=sheet['sheet_id']
                groupby={}
                rules={}
                agg_summary={}
                # Initialise rownum before scaning any of the dynamic sections
                row_num_counter = 0
                unique_records = {}
                comp_agg_def_cls={}
                # For each sheet there may be more than one dynamic section
                sections=self.db.query("select distinct sheet_id,section_id from report_dyn_def where report_id=%s and sheet_id=%s",(report_id,sheet_id)).fetchall()

                for section in sections:
                    section_id=section['section_id']
                    dyn_def=self.db.query("select  * from report_dyn_def where report_id=%s and sheet_id=%s and section_id=%s and in_use='Y' ",
                                           (report_id,sheet_id,section_id)).fetchall()

                    for dyn in dyn_def:
                        # app.logger.info("Looping in dyn def {}".format(dyn))
                        if dyn['cell_render_def'] == "GROUPBY":
                            if dyn['source_id'] in groupby.keys():
                                groupby[dyn['source_id']].update({section_id: dyn['cell_calc_ref']})
                            else:
                                groupby[dyn['source_id']] = {section_id: dyn['cell_calc_ref']}

                        if dyn['cell_render_def'] == "RULES":
                            if dyn['source_id'] in rules.keys():
                                if section_id in rules[dyn['source_id']].keys():
                                    rules[dyn['source_id']][section_id].append(dyn['cell_calc_ref'])
                                else:
                                    rules[dyn['source_id']][section_id]=[dyn['cell_calc_ref']]
                            else:
                                rules[dyn['source_id']]={section_id:[dyn['cell_calc_ref']]}

                first_source=True
                sources=groupby.keys()
                # app.logger.info(list(sources))

                for source in sources:
                    app.logger.info("Inside source loop..{}".format(source))
                    #print(source)
                    source_table_name=self.db.query("select source_table_name from data_source_information where source_id=%s",
                                                    (source,)).fetchone()['source_table_name']
                    key_column=util.get_keycolumn(self.db._cursor(),source_table_name)

                    # Create separate function for this purpose later
                    # to get the column_list and calc_column_ref_list similar to get_list_of_columns_for_dataframe
                    # used in generate static report
                    sql="select * from report_dyn_calc_def where source_id=%s and report_id=%s and sheet_id=%s and in_use='Y' "
                    dyn_calc_def=self.db.query(sql,(source,report_id,sheet_id,)).fetchall()

                    # column_list={}
                    calc_column_ref_list=''
                    cell_calc_ref_list={}
                    for calc_def in dyn_calc_def:
                        # column_list.update({calc_def['aggregation_ref']:calc_def['column_id']})
                        calc_column_ref_list+= '/' + calc_def['aggregation_ref']
                        if calc_def['section_id'] in cell_calc_ref_list.keys():
                            cell_calc_ref_list[calc_def['section_id']].update({calc_def['column_id']:calc_def['calc_ref']})
                        else:
                            cell_calc_ref_list[calc_def['section_id']]={calc_def['column_id']:calc_def['calc_ref']}

                    app.logger.info("calc column ref list {}".format(cell_calc_ref_list))
                    table_def = self.db.query("describe " + source_table_name).fetchall()
                    table_col_list=''
                    for groupbysec in groupby[source].keys():
                        # app.logger.info("groupbysec {}".format(groupby[source][groupbysec]))
                        calc_column_ref_list += '/' + groupby[source][groupbysec].replace(',','/')
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
                    source_data_cur=self.db.query(sql,(business_date_from,business_date_to,source))
                    source_data=source_data_cur.fetchall()

                    #print(source_data)

                    for rulesection in rules[source].keys():
                        app.logger.info("Inside rulesection loop..{0} {1}".format(rulesection,rules[source]))
                        # Initialise unique record keys for each dynamic section
                        source_qd_list=[]
                        data_records = []

                        agg_summary={}
                        # Build the dyn comp agg def dictionary ref
                        dyn_comp_agg_def=self.db.query("select  * from report_dyn_comp_agg_def where report_id=%s and sheet_id=%s and section_id=%s and in_use='Y' ",
                                               (report_id,sheet_id,rulesection)).fetchall()

                        for dyn in dyn_comp_agg_def:
                            agg_summary[dyn['column_id']]={ 'comp_agg_ref': dyn['comp_agg_ref'],
                                                            'comp_agg_rule': dyn['comp_agg_rule'],
                                                            'reporting_scale': dyn['reporting_scale'],
                                                            'rounding_option': dyn['rounding_option']
                                                            }
                        for qd in source_data:
                            # app.logger.info("Inside qd loop..{}".format(qd))
                            for rule in rules[source][rulesection]:
                                if set(rule.split(',')).issubset(set(qd['business_rules'].split(','))):
                                    key=rulesection
                                    for col in groupby[source][rulesection].split(','):
                                        key+=qd[col]
                                    if key not in unique_records.keys():
                                        row_num_counter+=1
                                        # app.logger.info("section {0} row_num_counter {1}".format(rulesection,row_num_counter))
                                        unique_records[key]=row_num_counter

                                    # data_records.append({'source_id':source,'report_id':report_id,'sheet_id':sheet['sheet_id'],
                                    #                      'cell_id': '$' + unique_records[key],'cell_calc_ref':sheet['section_id'],
                                    #                      'qualifying_key':qd['qualifying_key'],
                                    #                      'buy_currency':qd['buy_currency'],'sell_currency':qd['sell_currnecy'],
                                    #                      'mtm_currency':qd['mtm_currency'],'business_date':qd['business_date'],
                                    #                      'reporting_date':reporting_date})
                                    # cell_calc_ref is build assuming the following section_id+'S'+source_id+'$'+record row
                                    qd.update({'source_id':source,'report_id':report_id,'sheet_id':sheet_id,
                                                'cell_id': '$' + str(unique_records[key]),
                                               'reporting_date': reporting_date
                                               })
                                    source_qd_list.append(qd)
                                    data_records.append((source, report_id, sheet_id,'$' + str(unique_records[key]),
                                                         rulesection+'$' + str(unique_records[key]), qd['buy_currency'], qd['sell_currency'],qd['mtm_currency'],
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
                        data_records=[]

                        if not source_qd_list:
                            qd={}
                            for i in source_data_cur.description:
                                qd[i[0]]=None
                            qd.update({'source_id':source,'report_id':report_id,'sheet_id':sheet_id,
                                        'cell_id': None,
                                       'reporting_date': reporting_date
                                       })
                            # source_qd_list.append()
                        source_qd_frame=pd.DataFrame(source_qd_list)
                        for col in source_qd_frame.columns:
                            source_qd_frame[col]=source_qd_frame[col].astype(dtype=float,errors='ignore')
                        #print(source_qd_frame)

                        summ_by_src=pd.DataFrame(columns=['source_id','report_id','sheet_id','cell_id','cell_calc_ref','cell_summary','reporting_date'])
                        # summ_by_src_table=pd.DataFrame(columns=list(column_list.values()).append(['row'] + [c + '_ref' for c in column_list.values()]))


                        for idx,grp in source_qd_frame.groupby('cell_id'):
                            # table_data={}
                            # app.logger.info("idx,grop loop idx: {0} grp: {1}".format(idx,grp))
                            row_num=grp['cell_id'].unique()[0].replace('$','')
                            sql="select * from report_dyn_calc_def where source_id=%s and report_id=%s and sheet_id=%s and section_id=%s and in_use='Y' "
                            dyn_calc_def=self.db.query(sql,(source,report_id,sheet_id,rulesection)).fetchall()
                            for calc_def in dyn_calc_def:
                                # app.logger.info("calc_def loop calc_def: {0} rulesection: {1}".format(calc_def,rulesection))
                                # app.logger.info("calc_def loop {}".format(calc_def['aggregation_ref']))
                                expr_str="grp['{0}'].".format(calc_def['aggregation_ref'])
                                if calc_def['aggregation_func']=='distinct':
                                    expr_str+="unique()[0]"
                                else:
                                    expr_str+=calc_def['aggregation_func']+"()"
                                summary=eval(expr_str)
                                cell_id=calc_def['column_id'] + str(row_num)
                                cell_calc_ref=cell_calc_ref_list[rulesection][calc_def['column_id']].replace('$',str(row_num))
                                data={'source_id':source,'report_id':report_id,'sheet_id':sheet_id,
                                      'cell_id':cell_id,'cell_calc_ref':cell_calc_ref,'cell_summary':summary,'reporting_date':reporting_date}
                                summ_by_src=summ_by_src.append(data,ignore_index=True)
                                comp_agg_def_cls[cell_calc_ref] = {
                                        'formula': summary,
                                        'reporting_scale': 1,
                                        'rounding_option': "NONE",
                                        'type': 'NA',
                                        'report_id':report_id,
                                        'sheet_id':sheet_id,
                                        'cell_id': cell_id,
                                        'column_id': calc_def['column_id'],
                                        'contributor': 'SOURCE',
                                        'column_id': calc_def['column_id'],
                                        'section': rulesection,
                                        'row':row_num,
                                        'ref':cell_calc_ref_list[rulesection][calc_def['column_id']]
                                        }
                                # app.logger.info("agg_ref {0} {1}".format(rulesection,agg_summary))
                                agg_ref = agg_summary[calc_def['column_id']]['comp_agg_ref']+str(row_num)
                                if agg_ref in comp_agg_def_cls.keys():
                                    comp_agg_def_cls[agg_ref]['formula'] = comp_agg_def_cls[agg_ref]['formula'].replace(cell_calc_ref_list[rulesection][calc_def['column_id']],cell_calc_ref)
                                    comp_agg_def_cls[agg_ref]['cell_summary'] = comp_agg_def_cls[agg_ref]['cell_summary'].replace(cell_calc_ref_list[rulesection][calc_def['column_id']],str(summary))
                                else:
                                    comp_agg_def_cls[agg_ref] = {
                                            'formula': agg_summary[calc_def['column_id']]['comp_agg_rule'].replace(cell_calc_ref_list[rulesection][calc_def['column_id']],cell_calc_ref),
                                            'cell_summary': agg_summary[calc_def['column_id']]['comp_agg_rule'].replace(cell_calc_ref_list[rulesection][calc_def['column_id']],str(summary)) if agg_summary[calc_def['column_id']]['reporting_scale'] else str(summary),
                                            'reporting_scale': agg_summary[calc_def['column_id']]['reporting_scale'] if agg_summary[calc_def['column_id']]['reporting_scale'] else 1,
                                            'rounding_option': agg_summary[calc_def['column_id']]['rounding_option'],
                                            'type': 'NUMBER' if agg_summary[calc_def['column_id']]['reporting_scale'] else 'TEXT',
                                            'report_id':report_id,
                                            'sheet_id':sheet_id,
                                            'cell_id': cell_id,
                                            'column_id': calc_def['column_id'],
                                            'contributor': 'AGGDEF',
                                            'column_id': calc_def['column_id'],
                                            'section': rulesection,
                                            'row':row_num,
                                            'ref':cell_calc_ref_list[rulesection][calc_def['column_id']]
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
            # app.logger.info(comp_agg_def_cls)
            summary_set = tree(comp_agg_def_cls, format_flag='Y')
            # app.logger.info(summary_set)
            summary_final=[]
            summary_final_d=[]
            for key in comp_agg_def_cls.keys():
                # app.logger.info("inside comp agg def key loop, key: {0} ".format(key))
                if comp_agg_def_cls[key]['contributor'] == 'AGGDEF':
                    summary_final_d.append({'report_id':comp_agg_def_cls[key]['report_id'],
                        'sheet_id':comp_agg_def_cls[key]['sheet_id'],
                        'cell_id':comp_agg_def_cls[key]['cell_id'],
                        'summary':summary_set[key] if comp_agg_def_cls[key]['type']=='NUMBER' else comp_agg_def_cls[key]['cell_summary'],
                        'reporting_date':reporting_date,
                        'section':comp_agg_def_cls[key]['section'],
                        'column_id':comp_agg_def_cls[key]['column_id'],
                        'key':comp_agg_def_cls[key]['row']
                        })
                    # if comp_agg_def_cls[key]['section']=='DYN001' and comp_agg_def_cls[key]['cell_id']=='B' and comp_agg_def_cls[key]['row'] in ['142','143','144','145']:
                    # if comp_agg_def_cls[key]['section']=='DYN001' and comp_agg_def_cls[key]['row'] in ['142','143','144','145']:
                    #     app.logger.info("{0} {1}".format(key,comp_agg_def_cls[key]))
                    # print(comp_agg_def_cls[key]['report_id'],
                    #     comp_agg_def_cls[key]['sheet_id'],
                    #     comp_agg_def_cls[key]['cell_id'],
                    #     summary_set[key] if comp_agg_def_cls[key]['type']=='NUMBER' else comp_agg_def_cls[key]['cell_summary']
                    #     )
            summary_frame=pd.DataFrame(summary_final_d)
            summary_row_num = 0
            for gidx,grp in summary_frame.groupby('section'):
                for kidx, kgrp in grp.groupby('cell_id'):
                    summary_row_num+=1
                    for idx,row in kgrp.iterrows():
                        summary_final.append((comp_agg_def_cls[key]['report_id'],
                            row['sheet_id'],
                            row['cell_id'],
                            row['summary'],
                            row['reporting_date'],
                            row['column_id']+str(summary_row_num)
                            ))


            sql="insert into report_summary(report_id,sheet_id,cell_id,cell_summary,reporting_date,render_cell_id) values(%s,%s,%s,%s,%s,%s)"
            res=self.db.transactmany(sql,summary_final)


            self.db.commit()
            return {"msg":"Dynamic report generation completed successfully"},200


        except Exception as e:
            self.db.rollback()
            app.logger.error(str(e))
            raise
            # return {"msg":e},500
