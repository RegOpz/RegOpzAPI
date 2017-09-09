from flask import Flask, jsonify, request
from flask_restful import Resource
import openpyxl as xls
import time
from multiprocessing import Pool,cpu_count
from functools import partial
import csv
import re
import Helpers.utils as util
from collections import defaultdict
import sys
import ast
import mysql.connector as mysql
import pandas as pd
from Helpers.DatabaseHelper import DatabaseHelper
from Constants.Status import *
from operator import itemgetter
from datetime import datetime
from numpy import where
from Helpers.Tree import tree

class GenerateReportController(Resource):

    def get(self):
        if(request.endpoint=='get_report_list_ep'):
            country=request.args.get('country') if request.args.get('country') != None else 'ALL'
            return self.get_report_list(country)
        if (request.endpoint == 'get_country_list_ep'):
            return self.get_country_list()

    def post(self):
        if (request.endpoint=='create_report_ep'):
            report_info=request.get_json(force=True)
            report_id=report_info['report_id']
            reporting_date=report_info['reporting_date']
            as_of_reporting_date=report_info['as_of_reporting_date']
            report_create_date=report_info['report_create_date']
            report_create_status=report_info['report_create_status']

            report_parameters = "'business_date_from':'" + report_info["business_date_from"] + "'," + \
                                "'business_date_to':'" + report_info["business_date_to"] + "'," + \
                                "'reporting_currency':'" + report_info["reporting_currency"] + "'," + \
                                "'ref_date_rate':'" + report_info["ref_date_rate"] + "'," + \
                                "'rate_type':'" + report_info["rate_type"] +"'"
            if(report_info["report_parameters"]):
                report_parameters=report_parameters+","+report_info["report_parameters"]
            print(report_parameters)
            report_kwargs=eval("{"+"'report_id':'" + report_id + "',"+ report_parameters + "}")
            print(report_kwargs)

            self.create_report_catalog(report_id,reporting_date,report_create_date,
                                       report_parameters,report_create_status,as_of_reporting_date)
            self.update_report_catalog(status='RUNNING', report_id=report_id, reporting_date=reporting_date)
            self.create_report_detail(**report_kwargs)
            print("create_report_summary_by_source")
            self.create_report_summary_by_source(**report_kwargs)
            print("create_report_summary_final")
            self.create_report_summary_final(**report_kwargs)
            self.update_report_catalog(status='SUCCESS', report_id=report_id, reporting_date=reporting_date)



        if(request.endpoint == 'generate_report_ep'):
            report_info = request.get_json(force=True)
            report_id = report_info['report_id']
            report_parameters = report_info['report_parameters']
            reporting_date = report_info['reporting_date']
            report_kwargs = eval("{'report_id':'" + report_id + "' ," + report_parameters + "}")
            #report_kwargs = {'report_id': 'MAS1003', 'business_date_from': '20160930', 'reporting_currency': 'SGD', 'ref_date_rate': 'B', 'business_date_to': '20160930', 'rate_type': 'MAS'}
            print(report_kwargs)
            db=DatabaseHelper()
            #try:
            self.update_report_catalog(status='RUNNING',report_id=report_id,reporting_date=reporting_date)
            self.create_report_detail(**report_kwargs)
            print("create_report_summary_by_source")
            self.create_report_summary_by_source(**report_kwargs)
            print("create_report_summary_final")
            self.create_report_summary_final(**report_kwargs)
            db.commit()
            self.update_report_catalog(status='SUCCESS',report_id=report_id,reporting_date=reporting_date)
            #except Exception as e:
                #print("Error ... : " + str(e))
                #db.rollback()
            #finally:
                #return report_kwargs

    def get_report_list(self,country='ALL'):
        db=DatabaseHelper()
        report_list=db.query("select distinct report_id from report_def_catalog where country='"+country+"'").fetchall()
        return report_list

    def get_country_list(self):
        db=DatabaseHelper()
        country_list=db.query("select distinct country from report_def_catalog").fetchall()
        return country_list

    def create_report_catalog(self,report_id,reporting_date,report_create_date,
                              report_parameters,report_create_status,as_of_reporting_date):
        db=DatabaseHelper()
        sql="insert into report_catalog(report_id,reporting_date,report_create_date,\
            report_parameters,report_create_status,as_of_reporting_date) values(%s,%s,%s,%s,%s,%s)"
        db.transact(sql,(report_id,reporting_date,report_create_date,report_parameters,report_create_status,as_of_reporting_date))
        db.commit()

    def update_report_catalog(self,status,report_id,reporting_date):
        db=DatabaseHelper()
        db.transact("update report_catalog set report_create_status=%s \
                    where report_id=%s and reporting_date=%s",(status,report_id,reporting_date))
        db.commit()


    def map_data_to_cells(self,list_business_rules,exch_rt_dict,reporting_currency,qualified_data):

        result_set=[]
        for qd in qualified_data:
            trd_rules_list=qd["business_rules"].split(',')
            for rl in list_business_rules:
                br_rules_list=rl["cell_business_rules"].split(',')
                if set(br_rules_list).issubset(set(trd_rules_list)):
                    d_r = {"buy_reporting_rate": None, "sell_reporting_rate": None, "mtm_reporting_rate": None}
                    d_u = {"buy_usd_rate": None, "sell_usd_rate": None, "mtm_usd_rate": None}
                    # To check which date to be considered for exchange rate calculation
                    # B - business_date exchange rate
                    # R - Reporting date exchange rate
                    erd = lambda rr: qd["business_date"] if rr == 'B' else qd["business_date_to"]
                    ref_date = str(erd(qd["ref_date_rate"]))

                    for k in d_r.keys():
                        key1 = qd[k[:k.find('_')] + "_currency"]
                        if key1 != '' and (ref_date+reporting_currency in exch_rt_dict[ref_date+key1]):
                            d_r[k] = exch_rt_dict[ref_date+key1][ref_date+reporting_currency]

                    for k in d_u.keys():
                        key1=qd[k[:k.find('_')] + "_currency"]
                        if key1 != '' and (ref_date+'USD' in exch_rt_dict[ref_date+key1]):
                            d_u[k] = exch_rt_dict[ref_date+key1][ref_date+'USD']

                    # print(d_r["buy_reporting_rate"],d_r["sell_reporting_rate"],d_r["mtm_reporting_rate"],d_u["buy_usd_rate"],\
                    #       d_u["sell_usd_rate"],d_u["mtm_usd_rate"])

                    result_set.append((rl["report_id"], rl["sheet_id"],rl["cell_id"],\
                    rl["cell_calc_ref"], qd["source_id"],qd["qualifying_key"],qd["buy_currency"],qd["sell_currency"],\
                    qd["mtm_currency"],qd["business_date"],qd["reporting_date"],d_r["buy_reporting_rate"],\
                    d_r["sell_reporting_rate"],d_r["mtm_reporting_rate"],d_u["buy_usd_rate"],d_u["sell_usd_rate"],\
                    d_u["mtm_usd_rate"]))

        return result_set



    def create_report_detail(self,**kwargs):

        print(kwargs)

        parameter_list=['report_id','reporting_currency','business_date_from','business_date_to','ref_date_rate','rate_type']
        if set(parameter_list).issubset(set(kwargs.keys())):
            report_id=kwargs["report_id"]
            reporting_date=kwargs["business_date_from"]+kwargs["business_date_to"]
            reporting_currency=kwargs["reporting_currency"]
            business_date_from=kwargs["business_date_from"]
            business_date_to=kwargs["business_date_to"]
            ref_date_rate=kwargs["ref_date_rate"]
            rate_type=kwargs["rate_type"]
        else:
            print("Please supply parameters: "+str(parameter_list))

        db=DatabaseHelper()

        all_business_rules=db.query('select report_id,sheet_id,cell_id,cell_calc_ref,cell_business_rules \
                    from report_calc_def where report_id=%s and in_use=\'Y\'',(report_id,)).fetchall()


        start = time.time()
        #report_parameter={'_TODAY':'20160930','_YESDAY':'20160929'}
        report_parameter={}
        for k,v in kwargs.items():
            if k.startswith('_'):
                report_parameter[k]=v

        for i,rl in enumerate(all_business_rules):
            #check for possible report parameter token replacement
            for key, value in report_parameter.items():
                all_business_rules[i]['cell_business_rules'] = all_business_rules[i]['cell_business_rules'].replace(key, key + ':' + value)

        #print('All business rules after report parameter', all_business_rules)
        print('Time taken for converting to dictionary all_business_rules ' + str((time.time() - start) * 1000))

        #Changes required for incoporating exchange rates

        if ref_date_rate=='B':
            exch_rt=db.query('select business_date,from_currency,to_currency,rate from exchange_rate where business_date between %s and %s\
                    and rate_type=%s',(business_date_from,business_date_to,rate_type)).fetchall()
        else:
            exch_rt=db.query('select business_date,from_currency,to_currency,rate from exchange_rate where business_date=%s and rate_type=%s',\
                    (business_date_to,rate_type)).fetchall()

        exch_rt_dict=defaultdict(dict)

        for er in exch_rt:
            exch_rt_dict[str(er["business_date"])+er["from_currency"]][str(er["business_date"])+er["to_currency"]]=er["rate"]

        #Clean the link table before populating for same reporting date
        print('Before clean_table report_qualified_data_link')
        start = time.time()
        util.clean_table(db._cursor(), 'report_qualified_data_link', '', reporting_date,'report_id=\''+ report_id + '\'')
        print('Time taken for clean_table report_qualified_data_link ' + str((time.time() - start) * 1000))

        dbqd=DatabaseHelper()
        curdata =dbqd.query('select source_id,qualifying_key,business_rules,buy_currency,sell_currency,mtm_currency,\
                    %s as reporting_currency,business_date,%s as reporting_date,%s as business_date_to, \
                    %s as ref_date_rate \
                     from qualified_data where business_date between %s and %s',\
                    (reporting_currency,reporting_date,business_date_to,ref_date_rate,business_date_from,business_date_to))
        startcur = time.time()
        while True:
            all_qualified_trade=curdata.fetchmany(50000)
            if not all_qualified_trade:
                break

            print('Before converting to dictionary')
            start = time.time()
            all_qual_trd_dict_split = util.split(all_qualified_trade, 1000)
            print('Time taken for converting to dictionary and spliting all_qual_trd '+str((time.time() - start)*1000))

            #print(exch_rt_dict)
            start=time.time()

            #mp=partial(map_data_to_cells,all_bus_rl_dict,exch_rt_dict,reporting_currency)
            mp = partial(self.map_data_to_cells, all_business_rules, exch_rt_dict, reporting_currency)

            print('CPU Count: ' + str(cpu_count()))
            if cpu_count()>1 :
                pool=Pool(cpu_count()-1)
            else:
                print('No of CPU is only 1, ... Inside else....')
                pool=Pool(1)
            result_set=pool.map(mp,all_qual_trd_dict_split)
            pool.close()
            pool.join()

            print('Time taken by pool processes '+str((time.time() - start)*1000))

            start=time.time()
            result_set_flat=util.flatten(result_set)
            start=time.time()
            #for result_set_flat in rs:
            print('Database inserts for 50000 .....')
            db.transactmany('insert into report_qualified_data_link \
                            (report_id,sheet_id ,cell_id ,cell_calc_ref,source_id ,qualifying_key,\
                             buy_currency,sell_currency,mtm_currency,business_date,reporting_date,\
                             buy_reporting_rate,sell_reporting_rate,mtm_reporting_rate,\
                             buy_usd_rate,sell_usd_rate,mtm_usd_rate)\
                              values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',result_set_flat)

            print('Time taken by database inserts '+ str((time.time() - start) * 1000))
            db.commit()
            #return
        print('Time taken by complete loop of qualified data '+ str((time.time() - startcur) * 1000))


    def apply_formula_to_frame(self, df, excel_formula,new_field_name):
        tokens_queue = re.split('(\W)', excel_formula)

        # from this expression:if(sell_currency='SGD',sell_currency,buy_currency*buy_reporting_rate)
        # to this expression:df['reporting_value'] = np.where(df['Sell_Currency'] == 'SGD', df['Amount_Sell'],df['Amount_Buy'] * df['buy_reporting_rate'])
        num_token = 0
        pandas_code = ''
        pandas_float=''
        while num_token < len(tokens_queue):

            if tokens_queue[num_token] == 'if':
                pandas_code += 'where'
                num_token += 1

            #Added other operators as well, +, -, /, *,>,<
            elif tokens_queue[num_token] in ['(', ')', ',','+','-','/','*','>','<']:
                pandas_code += tokens_queue[num_token]
                num_token += 1

            elif tokens_queue[num_token] == '=':
                #check if its part of >=,<=, else set it ==
                #num_token -2 since the previous token would be '' and the previous to previous one would be < or >
                if tokens_queue[num_token-2] in ['>','<']:
                    pandas_code += '='
                else:
                    pandas_code += '=='
                num_token += 1

            elif tokens_queue[num_token] in ["'", '"']:
                open_quote = tokens_queue[num_token]
                num_token += 1

                str_lit = tokens_queue[num_token]
                num_token += 1

                close_quote = tokens_queue[num_token]
                num_token += 1

                pandas_code += open_quote + str_lit + close_quote

            #if the list element in token is a column of the data frame then convert it to the data frame syntax
            elif tokens_queue[num_token] in df.columns:
                pandas_code += "df['TOKEN']".replace('TOKEN', tokens_queue[num_token])
                num_token += 1

            #This is to check whether any constant is part of the expression
            #constants can be int e.g. 1, 2345, 67 or fractions e.g. .1,0.9,1.99 etc
            #re.split('(\W)') splits int as an element in the list, however a fraction 0.99 would be split as follows:
            # '0','.','99', so we need to add all these to the result.
            elif re.match('\w', tokens_queue[num_token]) or tokens_queue[num_token] in ['.']:
                pandas_code += tokens_queue[num_token]
                num_token += 1

            else:
                num_token += 1

            #make sure two operands of an arithmatic operation are float
            if tokens_queue[num_token-1] in ['*']:
                operand1,operand2=tokens_queue[num_token-2],tokens_queue[num_token]

                # df['Amount_Sell']= df['Amount_Sell'].map(float)
                #before doing float operation, check whether operand1 & 2 are  df columns
                if operand1 in df.columns:
                    pandas_float += 'df[\''+operand1+'\']=df[\'' + operand1 + '\'].map(float)\n'

                if operand2 in df.columns:
                    pandas_float += 'df[\'' + operand2 + '\']=df[\'' + operand2 + '\'].map(float)\n'

                #print(pandas_float)

        pandas_code = 'df[\''+new_field_name+'\']=' + pandas_code
        #print(list(df))
        #print(pandas_code)
        exec(pandas_float)
        exec(pandas_code)

        #print(tokens_queue)
        #print(pandas_code)

        return df

    def get_list_of_columns_for_dataframe(self,agg_df,table_name):
        db=DatabaseHelper()
        table_def = db.query("describe " + table_name).fetchall()

        #Now build the agg column list
        df_agg_column_list=''
        for agg_ref in agg_df['aggregation_ref'].unique():
            df_agg_column_list += '/' + agg_ref

        #check for column in the agg column list search_string
        #if present add to the table column list
        table_col_list=''
        for col in table_def:
            if col['Field'] in df_agg_column_list:
                if table_col_list == '':
                    table_col_list = col['Field']
                else:
                    table_col_list += ',' + col['Field']

        #Iftable col list is blank, that means we have to select all the columns of the table
        table_col_list=(table_col_list,'1 as const')[table_col_list=='']
        print(table_col_list)
        return table_col_list



    def create_report_summary_by_source(self,**kwargs):

        parameter_list = ['report_id', 'business_date_from', 'business_date_to']

        if set(parameter_list).issubset(set(kwargs.keys())):
            report_id = kwargs["report_id"]
            business_date_from = kwargs["business_date_from"]
            business_date_to = kwargs["business_date_to"]
            reporting_date = business_date_from+business_date_to

        else:
            print("Please supply parameters: " + str(parameter_list))

        db=DatabaseHelper()

        # Fetch all aggregate clauses into a dataframe
        sql = "select a.report_id,a.sheet_id,a.cell_id,b.source_id,\
                        b.source_table_name,a.aggregation_ref,a.cell_calc_ref,a.aggregation_func\
                        from report_calc_def a,data_source_information b\
                        where  a.source_id=b.source_id and a.in_use='Y' \
                        and a.report_id='REPORT_ID' order by a.source_id".replace(
            'REPORT_ID', report_id)
        all_agg_cls = pd.read_sql(sql, db.connection())
        #Convert to float where possible to reduce memory usage
        for col_to_convert in all_agg_cls.columns:
            all_agg_cls[[col_to_convert]]=all_agg_cls[[col_to_convert]].astype(dtype=float,errors='ignore')

        print(all_agg_cls.dtypes)
        all_agg_cls_grp=all_agg_cls.groupby('source_id')
        sources=all_agg_cls['source_id'].unique()

        #Now get the required column list for data frames
        col_list = ''
        col_list = self.get_list_of_columns_for_dataframe(all_agg_cls,'report_qualified_data_link')
        if col_list != '1 as const':
            col_list = 'a.' + col_list.replace(',',',a.')

        sql= "select a.sheet_id, a.cell_id, a.cell_calc_ref,a.source_id,a.qualifying_key,\
              a.business_date,COLUMN_LIST,b.source_table_name from report_qualified_data_link a , data_source_information b\
              where business_date between 'DATE_FROM' and 'DATE_TO' and \
               a.source_id=b.source_id and report_id='REPORT_ID' and \
               reporting_date='REPORT_DATE'".replace('COLUMN_LIST',col_list).replace('REPORT_ID',report_id)\
                .replace('DATE_FROM',business_date_from).replace('DATE_TO',business_date_to)\
                .replace('REPORT_DATE',reporting_date)

        print(sql)
        report_qualified_data_link=pd.read_sql(sql,db.connection())
        #Convert to float where possible to reduce memory usage
        for col_to_convert in report_qualified_data_link.columns:
            report_qualified_data_link[[col_to_convert]]=report_qualified_data_link[[col_to_convert]].astype(dtype=float,errors='ignore')

        print(report_qualified_data_link.dtypes)
        report_qualified_data_link.info(memory_usage='deep')

        # break report_qualified_data_link into groups according to source_id
        grouped = report_qualified_data_link.groupby('source_id')

        merge_grouped={}
        for idx,grp in grouped:
            source_table=grp['source_table_name'].unique()[0]
            key_column = util.get_keycolumn(db._cursor(), source_table)
            print('key column ['+key_column+']')

            col_list = ''
            col_list = self.get_list_of_columns_for_dataframe(all_agg_cls,source_table)
            if key_column not in col_list:
                col_list = key_column + ',' + col_list
            sql = "select COLUMN_LIST from TBL where business_date between 'DATE_FROM' and 'DATE_TO'".replace('COLUMN_LIST',col_list)\
                .replace('TBL', source_table) \
                .replace('DATE_FROM', business_date_from).replace('DATE_TO', business_date_to)

            data_frms = pd.read_sql(sql, db.connection(), chunksize=50000)
            df_group_list = []
            for frm in data_frms:
                #print(frm.columns)
                #print(frm.dtypes)
                #Convert to float where possible to reduce memory usage
                for col_to_convert in frm.columns:
                    frm[[col_to_convert]]=frm[[col_to_convert]].astype(dtype=float,errors='ignore')
                print(frm.dtypes)
                col_to_use = frm.columns.difference(grp.columns)
                frm.info(memory_usage='deep')
                #print(col_to_use)
                df_group_list.append(pd.merge(grp, frm[col_to_use], left_on='qualifying_key', right_on=key_column))

            #print("Df group list count: " + str(len(df_group_list)))
            merge_grouped[idx]=pd.concat(df_group_list)


        # clean summary table before populating it for reporting_date
        # util.clean_table(cur, 'report_summary', '', reporting_date)
        util.clean_table(db._cursor(), 'report_summary_by_source', '', reporting_date,'report_id=\''+ report_id + '\'')

        for src in sources:
            print('Processing data frame for source id [' + str(src) + '].')
            agg_cls_grp = all_agg_cls_grp.get_group(src)
            result_set = []
            # if the data frame is empty for a source id, then the source id would not be there as one of the keys
            # of the merged group of data frames, so do nothing for empty data frames
            if src not in merge_grouped.keys():
                print('Empty data frame for source id [' + str(src) + '], so no action required.')
            else:
                mrg_src_grp = merge_grouped[src].groupby(['sheet_id', 'cell_id', 'cell_calc_ref'])
                #print(mrg_src_grp.groups.keys())

                for idx, row in agg_cls_grp.iterrows():
                    key = (row['sheet_id'], row['cell_id'], row['cell_calc_ref'])
                    #print(key)

                    if key in mrg_src_grp.groups.keys():
                        #print("Inside if..", key)
                        mrg_src = mrg_src_grp.get_group(key)
                        mrg_src = self.apply_formula_to_frame(mrg_src, row['aggregation_ref'], 'reporting_value')
                        # print(mrg_src['reporting_value'])
                        mrg_src['reporting_value'] = mrg_src['reporting_value'].map(float)
                        summary = eval('mrg_src[\'reporting_value\'].' + row["aggregation_func"] + '()')
                        result_set.append((row['report_id'], row['sheet_id'], row['cell_id'], \
                                           row['source_id'], row['cell_calc_ref'], float(summary), reporting_date))

                db.transactmany('insert into report_summary_by_source(report_id,sheet_id,cell_id,\
                                    source_id,cell_calc_ref,cell_summary,reporting_date)\
                                    values(%s,%s,%s,%s,%s,%s,%s)', result_set)


        db.commit()
        #return

    def create_report_summary_final(self,**kwargs):
        parameter_list = ['report_id', 'business_date_from', 'business_date_to']

        if set(parameter_list).issubset(set(kwargs.keys())):
            report_id = kwargs["report_id"]
            business_date_from = kwargs["business_date_from"]
            business_date_to = kwargs["business_date_to"]
            reporting_date = business_date_from + business_date_to
        else:
            print("Please supply parameters: " + str(parameter_list))

        # con = mysql.connect(**db_config)
        # cur = con.cursor(dictionary=True)
        #
        db = DatabaseHelper()

        # clean summary table before populating it for reporting_date
        # util.clean_table(cur, 'report_summary', '', reporting_date)
        util.clean_table(db._cursor(), 'report_summary', '', reporting_date, 'report_id=\''+ report_id + '\'')


        # formula='(RCDMAS1003ID001G19+RCDMAS1003ID001H19)/RCDMAS1003ID001K19+RCDMAS1003ID001G19*0.5'
        # variables = list(set([node.id for node in ast.walk(ast.parse(formula)) if isinstance(node, ast.Name)]))
        # Another example of formula can be as follows using ternary operators (if_test_is_false, if_test_is_true)[test]
        # if we want to implement a condition like R1+if(R2>0,R2,0) can be implemented as follows:
        # formula='R1 + (0,R2)[R2>0]'

        sql = "SELECT * FROM report_summary_by_source WHERE reporting_date='{0}' AND report_id='{1}'"\
            .format(reporting_date, report_id)

        summ_by_src = pd.read_sql(sql, db.connection())
        summ_by_src.set_index(['cell_calc_ref'],inplace=True)

        comp_agg_cls = db.query("SELECT * FROM report_comp_agg_def WHERE report_id=%s AND in_use='Y'",\
        (report_id,)).fetchall()

        formula_set = {}
        for cls in comp_agg_cls:
            ref = cls['comp_agg_ref']
            formula = cls['comp_agg_rule']

            if ref != formula:
                formula_set[ref] = formula
            else:
                try:
                    summary_val = summ_by_src.loc[ref]['cell_summary']
                    formula_set[ref] = summary_val
                except KeyError:
                    formula_set[ref] = 0.0 # S2S27

        summary_set = tree(formula_set)
        print(summary_set)

            # variables = list(set([node.id for node in ast.walk(ast.parse(formula)) if isinstance(node, ast.Name)]))
            #
            # summ_by_src_vals=summ_by_src[summ_by_src['cell_calc_ref'].isin(variables)][['cell_calc_ref','cell_summary']]
            # summ_by_src_vals.set_index(['cell_calc_ref'],inplace=True)
            #
            # #print(summ_by_src_vals)
            #
            # for var in variables:
            #
            #     if var in summ_by_src_vals.index.values:
            #         val=summ_by_src_vals.loc[var]['cell_summary']
            #     else:
            #         val=0
            #
            #     formula=formula.replace(var,str(val))
            #
            # # for idx,row in summ_by_src_vals.iterrows():
            # #     formula=formula.replace(row['cell_calc_ref'],row['cell_summary'])
            # # print(formula)
            # summary=eval(formula)

        result_set = []
        for cls in comp_agg_cls:
            result_set.append((cls['report_id'], cls['sheet_id'], cls['cell_id'],\
            summary_set[cls['comp_agg_ref']], reporting_date))

        # print(result_set)

        try:
            rowId = db.transactmany("INSERT INTO report_summary(report_id,sheet_id,cell_id,cell_summary,reporting_date)\
                            VALUES(%s,%s,%s,%s,%s)", result_set)
            db.commit()
            return rowId
        except Exception as e:
            print("Transaction Failed:", e)
