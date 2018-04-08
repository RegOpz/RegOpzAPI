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
        tenant_info = json.loads(request.headers.get('Tenant'))
        self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
        self.db=DatabaseHelper(self.tenant_info)

    def post(self):
        if request.endpoint=='create_dynamic_report_ep':
            report_info=request.get_json(force=True)
            report_id=report_info['report_id']
            business_date_from=report_info['business_date_from']
            business_date_to=report_info['business_date_to']
            return self.create_report_detail(report_id,business_date_from,business_date_to)

    def get_dyn_def_details(self,report_id,sheet_id):
        try:
            app.logger.info("get dynamic def details for Report:{0} Sheet:{1}"\
                            .format(report_id,sheet_id))
            groupby={}
            rules={}
            dyn_def=self.db.query("select  * from report_dyn_def where report_id=%s and sheet_id=%s and in_use='Y' ",
                                   (report_id,sheet_id)).fetchall()

            for dyn in dyn_def:
                # app.logger.info("Looping in dyn def {}".format(dyn))
                source_id = dyn['source_id']
                section_id = dyn['section_id']
                cell_render_def = dyn['cell_render_def']
                cell_calc_ref = dyn['cell_calc_ref']
                if cell_render_def == "GROUPBY":
                    if source_id in groupby.keys():
                        groupby[source_id].update({section_id: cell_calc_ref})
                    else:
                        groupby[source_id] = {section_id: cell_calc_ref}

                if cell_render_def == "RULES":
                    if source_id in rules.keys():
                        if section_id in rules[source_id].keys():
                            rules[source_id][section_id].append(cell_calc_ref)
                        else:
                            rules[source_id].update({section_id:[cell_calc_ref]})
                    else:
                        rules[source_id]={section_id:[cell_calc_ref]}
            return {'groupby':groupby,'rules':rules}
        except Exception as e:
            app.logger.error(e)
            raise

    def get_table_col_list(self,source_id,report_id,sheet_id,table_name,groupby_columns):
        try:
            app.logger.info("Getting table column list for source [{0}] {1}".format(source_id,table_name))
            sql="select * from report_dyn_calc_def where source_id=%s and report_id=%s and sheet_id=%s and in_use='Y' "
            dyn_calc_def=self.db.query(sql,(source_id,report_id,sheet_id,)).fetchall()

            calc_column_ref_list=''
            for calc_def in dyn_calc_def:
                # column_list.update({calc_def['aggregation_ref']:calc_def['column_id']})
                calc_column_ref_list+= '/' + calc_def['aggregation_ref']
            # Now append the list of group by column for the source table
            calc_column_ref_list += groupby_columns
            app.logger.info("calc column ref list {0}".format(calc_column_ref_list))
            table_def = self.db.query("describe " + table_name).fetchall()
            table_col_list=''
            for col in table_def:
                if col['Field'] in calc_column_ref_list:
                    if table_col_list == '':
                        table_col_list = col['Field']
                    else:
                        table_col_list += ',' + col['Field']
            table_col_list=(table_col_list,'1 as const')[table_col_list=='']
            return table_col_list
        except Exception as e:
            app.logger.error(e)
            raise

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
                agg_summary_sec_def={}
                # Initialise rownum before scaning any of the dynamic sections
                row_num_counter = 0
                unique_records = {}
                comp_agg_def_cls={}
                # For each sheet there may be more than one dynamic section
                dyn_def_details = self.get_dyn_def_details(report_id,sheet_id)
                groupby = dyn_def_details['groupby']
                rules = dyn_def_details['rules']
                # app.logger.info("GROUPBY: {0} RULES: {1}".format(groupby,rules))

                sources=groupby.keys()
                # app.logger.info(list(sources))

                for source in sources:
                    app.logger.info("Inside source loop..{}".format(source))
                    #print(source)
                    source_table_name=self.db.query("select source_table_name from data_source_information where source_id=%s",
                                                    (source,)).fetchone()['source_table_name']
                    key_column=util.get_keycolumn(self.db._cursor(),source_table_name)

                    # Let's get the groupby column list for all sections of the source
                    groupby_columns = ''
                    for groupbysec in groupby[source].keys():
                        # app.logger.info("groupbysec {}".format(groupby[source][groupbysec]))
                        groupby_columns += '/' + groupby[source][groupbysec].replace(',','/')

                    table_col_list = self.get_table_col_list(source,report_id,sheet_id,source_table_name,groupby_columns)
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
                        agg_summary_sec_def[rulesection]={}
                        # Build the dyn comp agg def dictionary ref
                        dyn_comp_agg_def=self.db.query("select  * from report_dyn_comp_agg_def where report_id=%s and sheet_id=%s and section_id=%s and in_use='Y' ",
                                               (report_id,sheet_id,rulesection)).fetchall()

                        for dyn in dyn_comp_agg_def:
                            if dyn['agg_type']!='ELEMENT':
                                # All agg def entried other than ELEMENT e.g. HEADER,FOOTER, SUBTOTAL etc
                                if dyn['agg_type'] in agg_summary_sec_def[rulesection].keys():
                                    agg_summary_sec_def[rulesection][dyn['agg_type']].update({dyn['column_id']:dyn})
                                else:
                                    agg_summary_sec_def[rulesection].update({ dyn['agg_type']:{dyn['column_id']:dyn}})
                            else:
                                # This section only deals with ELEMENT for agg_summary
                                agg_summary[dyn['column_id']]=dyn
                        for qd in source_data:
                            # app.logger.info("Inside qd loop..{}".format(qd))
                            for rule in rules[source][rulesection]:
                                if set(rule.split(',')).issubset(set(qd['business_rules'].split(','))):
                                    key=rulesection
                                    for col in groupby[source][rulesection].split(','):
                                        key+=str(qd[col])
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

                        if source_qd_list:
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
                                summ_by_section_row={'row':row_num,'section_id':rulesection}
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
                                    # cell_calc_ref=cell_calc_ref_list[rulesection][calc_def['column_id']].replace('$',str(row_num))
                                    cell_calc_ref=calc_def['calc_ref'].replace('$',str(row_num))
                                    data={'source_id':source,'report_id':report_id,'sheet_id':sheet_id,
                                          'cell_id':cell_id,'cell_calc_ref':cell_calc_ref,'cell_summary':summary,'reporting_date':reporting_date}
                                    summ_by_src=summ_by_src.append(data,ignore_index=True)
                                    # comp_agg_def_cls[cell_calc_ref] = {'source_id':source,'report_id':report_id,'sheet_id':sheet_id,
                                    #       'cell_id':cell_id,'cell_calc_ref':cell_calc_ref,'cell_summary':summary,'reporting_date':reporting_date}
                                    comp_agg_def_cls[cell_calc_ref]={
                                            'formula': summary,
                                            'cell_summary':summary,
                                            'reporting_scale': 1,
                                            'rounding_option': "NONE",
                                            'type': 'NA',
                                            'contributor': 'SOURCE',
                                            'column_id': calc_def['column_id'],
                                            'section': rulesection,
                                            'row':row_num,
                                            'ref':calc_def['calc_ref']
                                            }
                                    # app.logger.info("agg_ref {0} {1}".format(rulesection,agg_summary))
                                    agg_ref = agg_summary[calc_def['column_id']]['comp_agg_ref']+str(row_num)
                                    agg_ref_type = 'NUMBER' if agg_summary[calc_def['column_id']]['reporting_scale'] else 'TEXT'
                                    formula = agg_summary[calc_def['column_id']]['comp_agg_rule']
                                    formula_replace_str=calc_def['calc_ref']
                                    if agg_ref in comp_agg_def_cls.keys():
                                        # comp_agg_def_cls[agg_ref]['formula'] = comp_agg_def_cls[agg_ref]['formula'].replace(cell_calc_ref_list[rulesection][calc_def['column_id']],cell_calc_ref)
                                        # comp_agg_def_cls[agg_ref]['cell_summary'] = comp_agg_def_cls[agg_ref]['cell_summary'].replace(cell_calc_ref_list[rulesection][calc_def['column_id']],str(summary))
                                        formula=comp_agg_def_cls[agg_ref]['formula']
                                        cell_summary = comp_agg_def_cls[agg_ref]['cell_summary']
                                        comp_agg_def_cls[agg_ref].update({
                                                'formula': formula.replace(formula_replace_str,cell_calc_ref),
                                                'cell_summary': cell_summary.replace(formula_replace_str,str(summary)) if agg_ref_type=='NUMBER' else str(summary)
                                                })
                                    else:
                                        # comp_agg_def_cls[agg_ref] = {'source_id':source,'report_id':report_id,'sheet_id':sheet_id,
                                        #       'cell_id':cell_id,'cell_calc_ref':cell_calc_ref,'cell_summary':summary,'reporting_date':reporting_date}
                                        comp_agg_def_cls[agg_ref]={
                                                'formula': formula.replace(formula_replace_str,cell_calc_ref),
                                                'cell_summary': formula.replace(formula_replace_str,str(summary)) if agg_ref_type=='NUMBER' else str(summary),
                                                'reporting_scale': agg_summary[calc_def['column_id']]['reporting_scale'] if agg_ref_type=='NUMBER' else 1,
                                                'rounding_option': agg_summary[calc_def['column_id']]['rounding_option'],
                                                'type': agg_ref_type,
                                                'contributor': 'AGGDEF',
                                                'column_id': calc_def['column_id'],
                                                'section': rulesection,
                                                'row':row_num,
                                                'ref':calc_def['calc_ref']
                                        }
                                        # app.logger.info("Data: {0} Old code: {1} Modified code : {2}".format(data,comp_agg_def_cls[agg_ref],comp_agg_def_cls[cell_calc_ref]))


                            sql="insert into report_summary_by_source(source_id,report_id,sheet_id,cell_id,cell_calc_ref,cell_summary,reporting_date) \
                                values(%s,%s,%s,%s,%s,%s,%s)"
                            res=self.db.transactmany(sql,summ_by_src.to_records(index=False).tolist())

            # app.logger.info(comp_agg_def_cls)
            summary_set = tree(comp_agg_def_cls, format_flag='Y')
            # app.logger.info(summary_set)
            summary_final=[]
            summary_final_d=[]
            for key in comp_agg_def_cls.keys():
                # app.logger.info("inside comp agg def key loop, key: {0} ".format(key))
                if comp_agg_def_cls[key]['contributor'] == 'AGGDEF':
                    agg_element = comp_agg_def_cls[key]
                    if comp_agg_def_cls[key]['type']=='NUMBER':
                        agg_element.update({'cell_summary':summary_set[key]})
                    summary_final_d.append(agg_element)



            if not summary_final_d:
                app.logger.info("No summary data to process.....")
            else:
                summary_frame=pd.DataFrame(summary_final_d)
                summary_row_num = 0
                for gidx,grp in summary_frame.groupby('section'):
                    # Now look for HEADER of the agg rule
                    summary_row_num+=1
                    dyn_section=grp['section'].iloc[0]
                    app.logger.info("Dynamic section header for agg summary {0} {1}".format(dyn_section,agg_summary_sec_def[dyn_section]))
                    for column in agg_summary_sec_def[dyn_section]['HEADER'].keys():
                        summary_final.append((report_id,
                            sheet_id,
                            column+'$',
                            agg_summary_sec_def[dyn_section]['HEADER'][column]['comp_agg_rule'],
                            reporting_date,
                            column+str(summary_row_num)
                            ))
                    summ_by_section=[]
                    # Now transpose the data frame from row to columns. e.g.
                    # groupby={'99':{'column_id':'A','cell_id':'A9','cell_summary':10 .....},
                    #          '99':{'column_id':'B','cell_id':'B9','cell_summary':'Text' .....},
                    #          '99':{'column_id':'C','cell_id':'C9','cell_summary':'Text' .....},
                    #          ..........................}
                    # to
                    # df={{'A':10,'B':'Text','C':'Text','row':'99'},
                    #     {'A':12,'B':'Text1','C':'TextC','row':'100'},
                    #     ......................}
                    for kidx, kgrp in grp.groupby('row'):
                        summ_by_section_row={}
                        for idx,row in kgrp.iterrows():
                            summ_by_section_row.update({row['column_id']:row['cell_summary'],'row':row['row']})
                        summ_by_section.append(summ_by_section_row)
                    df=pd.DataFrame(summ_by_section)
                    app.logger.info("{0}".format(df))
                    if 'ORDER' in agg_summary_sec_def[dyn_section].keys():
                        order_by=[agg_summary_sec_def[dyn_section]['ORDER'].keys()]
                    else:
                        order_by=df.columns.tolist()
                        order_by.remove('row')

                    for sidx, sgrp in df.sort_values(by=order_by).iterrows():
                    # if dyn_section == 'DYN001':
                    #     for sidx, sgrp in df.nlargest(10,'D').iterrows():
                        summary_row_num+=1
                        for key in sgrp.keys():
                            if key not in ['row']:
                                summary_final.append((report_id,
                                    sheet_id,
                                    key+str(sgrp['row']),
                                    sgrp[key],
                                    reporting_date,
                                    key+str(summary_row_num)
                                    ))
                    app.logger.info("Dynamic section subtotal for agg summary {0}".format(dyn_section))
                    summary_row_num+=1
                    for column in agg_summary_sec_def[dyn_section]['SUBTOTAL'].keys():
                        if agg_summary_sec_def[dyn_section]['SUBTOTAL'][column]['rounding_option']=='TEXT':
                            subtotal = agg_summary_sec_def[dyn_section]['SUBTOTAL'][column]['comp_agg_rule']
                        else:
                            subtotal_column=agg_summary_sec_def[dyn_section]['SUBTOTAL'][column]['comp_agg_rule']
                            if len(grp.index)>0:
                                subtotal=grp.loc[grp['column_id']==subtotal_column,'cell_summary'].sum()
                            else:
                                subtotal=0
                        summary_final.append((report_id,
                            sheet_id,
                            column+'$',
                            subtotal,
                            reporting_date,
                            column+str(summary_row_num)
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
