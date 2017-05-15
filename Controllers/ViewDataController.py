from flask import Flask, jsonify, request
from flask_restful import Resource
import openpyxl as xls
import time
from multiprocessing import Pool
from functools import partial
import csv
import Helpers.utils as util
from openpyxl.styles import Border, Side, PatternFill, Font, GradientFill, Alignment, Protection
from openpyxl.utils import get_column_letter
from collections import defaultdict
import sys
import ast
import mysql.connector as mysql
import pandas as pd
from Helpers.DatabaseHelper import DatabaseHelper
from operator import itemgetter
from datetime import datetime
class ViewDataController(Resource):
    def get(self):
        if(request.endpoint == 'get_date_heads_ep'):
            startDate = request.args.get('start_date') if request.args.get('start_date') != None else '19000101'
            endDate = request.args.get('end_date') if request.args.get('end_date') != None else '39991231'
            table_name = request.args.get('table_name')
            return self.render_data_load_dates(startDate, endDate, table_name)
        if(request.endpoint == 'report_ep'):
            source_id = request.args['source_id']
            business_date = request.args['business_date']
            page = request.args['page']
            return self.getDataSource(source_id=source_id,business_date=business_date,page=page)
        if(request.endpoint == 'get_source_ep'):
            business_date = request.args['business_date']
            return self.render_data_source_list(business_date=business_date)
        if(request.endpoint == 'report_linkage_ep'):
            source_id = request.args.get("source_id")
            qualifying_key = request.args.get("qualifying_key")
            business_date = request.args.get("business_date")
            return self.list_reports_for_data(source_id=source_id,qualifying_key=qualifying_key,business_date=business_date)
        if (request.endpoint == 'report_export_csv_ep'):
            tableName = request.args.get("table_name")
            businessDate = request.args.get("business_date")
            return self.export_to_csv(tableName,businessDate)
        if(request.endpoint == 'apply_rules_ep'):
            source_id = request.args['source_id']
            business_date = request.args['business_date']
            business_or_validation = request.args['business_or_validation']
            return self.run_rules_engine(source_id=source_id,business_date=business_date,business_or_validation=business_or_validation)

    def put(self, id=None):
        data = request.get_json(force=True)
        res = self.update_data(data, id)
        return res

    def post(self):
        data = request.get_json(force=True)
        res = self.insert_data(data)
        return res

    def delete(self, id=None):
        if id == None:
            return DATA_NOT_FOUND
        tableName = request.args.get("table_name")
        businessDate = request.args.get("business_date")
        res = self.delete_data(businessDate,tableName,id)
        return res

    def delete_data(self,business_date,table_name,id):
        db=DatabaseHelper()
        sql="delete from "+table_name +" where business_date = %s and id=%s"
        print(sql)

        params=(business_date,id,)
        print(params)
        res=db.transact(sql,params)

        return res


    def insert_data(self,data):

        db = DatabaseHelper()

        table_name = data['table_name']
        update_info = data['update_info']
        update_info_cols = update_info.keys()
        business_date=data['business_date']

        sql="insert into "+table_name + "("
        placeholders="("
        params=[]

        for col in update_info_cols:
            sql+=col+","
            placeholders+="%s,"
            if col=='business_date':
                params.append(business_date)
            else:
                params.append(update_info[col])

        placeholders=placeholders[:len(placeholders)-1]
        placeholders+=")"
        sql=sql[:len(sql)-1]
        sql+=") values "+ placeholders

        params_tuple=tuple(params)
        #print(sql)
        #print(params_tuple)
        res=db.transact(sql,params_tuple)

        return self.ret_source_data_by_id(table_name,business_date,res)

    def update_data(self,data,id):
        db=DatabaseHelper()

        table_name=data['table_name']
        update_info=data['update_info']
        update_info_cols=update_info.keys()
        business_date=data['business_date']

        sql= 'update '+table_name+ ' set '
        params=[]
        for col in update_info_cols:
            sql+=col +'=%s,'
            params.append(update_info[col])

        sql=sql[:len(sql)-1]
        sql+=" where business_date = '"+business_date+"' and id='"+id+"'"
        params_tuple=tuple(params)

        print(sql)
        print(params_tuple)

        res=db.transact(sql,params_tuple)

        if res==0:
            return self.ret_source_data_by_id(table_name,business_date,id)

        return UPDATE_ERROR

    def ret_source_data_by_id(self, table_name,business_date,id):
        db = DatabaseHelper()
        query = 'select * from ' + table_name + ' where business_date = %s and id = %s'
        cur = db.query(query, (business_date,id, ))
        data = cur.fetchone()
        if data:
            return data
        return NO_BUSINESS_RULE_FOUND

    def getDataSource(self,**kwargs):
        parameter_list = ['source_id', 'business_date', 'page']

        if set(parameter_list).issubset(set(kwargs.keys())):
            source_id = kwargs['source_id']
            business_date = kwargs['business_date']
            page = kwargs['page']
        else:
            print("Please supply parameters: " + str(parameter_list))
        db = DatabaseHelper()
        cur = db.query(
            "select source_table_name from data_source_information where source_id='" + str(source_id) + "'")
        table = cur.fetchone()

        startPage = int(page) * 100
        data_dict = {}
        cur = db.query("select * from " + table['source_table_name'] +
                       " where business_date='" + business_date + "' limit " + str(startPage) + ", 100")
        data = cur.fetchall()
        cols = [i[0] for i in cur.description]
        count = db.query('select count(*) as count from ' +
                         table['source_table_name'] +
                         ' where business_date=\'' + business_date + '\'').fetchone()
        data_dict['cols'] = cols
        data_dict['rows'] = data
        data_dict['count'] = count['count']
        data_dict['table_name'] = table['source_table_name']

        # print(data_dict)
        return data_dict

    def render_data_load_dates(self,start_business_date='19000101',end_business_date='39991231',catalog_table='data_catalog'):

        month_lookup={ '01': 'January',
                       '02':'February',
                       '03':'March',
                       '04':'April',
                       '05':'May',
                       '06':'June',
                       '07':'July',
                       '08':'August',
                       '09':'Sepember',
                       '10':'October',
                       '11':'November',
                       '12':'December'
                       }
        db = DatabaseHelper()
        if catalog_table == 'data_catalog':
            sqlQuery = "select distinct business_date from data_catalog where business_date between "+ start_business_date + " and " + end_business_date + " order by business_date"
        if catalog_table == 'report_catalog':
            sqlQuery = "select distinct reporting_date as business_date from report_catalog where reporting_date between "+ start_business_date + " and " + end_business_date + " order by reporting_date"

        #print(catalog_table,date_column)

        catalog=db.query(sqlQuery).fetchall()

        catalog_list=[]

        for cat in catalog:
            year=cat['business_date'][:4]
            month_num=cat['business_date'][4:6]
            bus_date=cat['business_date'][6:]
            month=month_lookup[month_num]

            #print(year,month,bus_date)
            #print(list(map(itemgetter('year'),catalog_list)))

            idx=list(map(itemgetter('year'),catalog_list)).index(year)\
                if year in map(itemgetter('year'),catalog_list) else None
            #print(list(map(itemgetter('year'), catalog_list)))
            if idx==None:
                d={'year':year,'month':{month:[bus_date]}}
                catalog_list.append(d)
                #print(catalog_list)

            else:
                if month in catalog_list[idx]['month'].keys():
                    catalog_list[idx]['month'][month].append(bus_date)
                else:
                    catalog_list[idx]['month'][month]=[bus_date]


        return (catalog_list)
    def render_data_source_list(self,**kwargs):

        parameter_list = ['business_date']

        if set(parameter_list).issubset(set(kwargs.keys())):
            business_date = kwargs['business_date']
        else:
            print("Please supply parameters: " + str(parameter_list))

        db=DatabaseHelper()
        data_sources = db.query("select *  from data_catalog where business_date='"+business_date+"'").fetchall()

        #print(data_sources)
        return (data_sources)
    def list_reports_for_data(self,**kwargs):
        parameter_list = ['source_id','qualifying_key','business_date']
        if set(parameter_list).issubset(set(kwargs.keys())):
            source_id=kwargs["source_id"]
            qualifying_key = kwargs['qualifying_key']
            business_date=kwargs['business_date']

        else:
            print("Please supply parameters: " + str(parameter_list))

        db=DatabaseHelper()
        sql="select * from report_qualified_data_link where qualifying_key='"+qualifying_key+\
        "' and business_date='"+business_date+"'"

        cur=db.query(sql)
        report_list=cur.fetchall()

        data_qual = db.query(
        "select * from qualified_data where qualifying_key='" + qualifying_key + "' and business_date='" + business_date + "'").fetchone()

        result_set = []
        for data in report_list:
            cell_rule=db.query("select * from report_calc_def where cell_calc_ref='"+data['cell_calc_ref']+"'").fetchone()

            data["cell_business_rules"]=cell_rule["cell_business_rules"]
            data["data_qualifying_rules"]=data_qual["business_rules"]
            result_set.append(data)

        return result_set
    def export_to_csv(self,table_name,business_date):
        db = DatabaseHelper()

        sql = "select * from "+table_name +" where business_date='"+business_date+"'"

        cur = db.query(sql)

        data = cur.fetchall()
        keys = [i[0] for i in cur.description]
        filename=table_name+business_date+".csv"

        with open('./static/'+filename, 'wt') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)
        return { "file_name": filename }

    def run_rules_engine(self,source_id,business_date,business_or_validation='ALL'):

        db=DatabaseHelper()

        sql_str = 'Select source_id,source_table_name from data_source_information'
        if source_id!='ALL':
            sql_str+=' where source_id =' + str(source_id)

        tables=db.query(sql_str).fetchall()

        for src in tables:
            # Select the data in the rule ececution order to facilitate derived rules definitions in the rule
            data=db.query('select * from business_rules where source_id=%s order by rule_execution_order asc',(src["source_id"],)).fetchall()

            code = 'if business_or_validation in [\'ALL\',\'BUSINESSRULES\']:\n'
            code += '\tdb.transact("delete from qualified_data where source_id='+src["source_id"]+' and business_date=%s",(business_date,))\n'
            code += 'if business_or_validation in [\'ALL\',\'VALIDATION\']:\n'
            code += '\tdb.transact("delete from invalid_data where source_id=' + src["source_id"] + ' and business_date=%s",(business_date,))\n'
            code += 'curdata=db.query("SELECT * FROM  '+src["source_table_name"]+' where business_date=%s",(business_date,))\n'
            code += 'while True:\n'
            code += '\tdata=curdata.fetchmany(100000)\n'
            code += '\tif not data:\n'
            code += '\t\tbreak\n'
            code +='\tqualified_data=[]\n'
            code +='\tinvalid_data=[]\n'
            code += '\tfor row in data:\n'
            code += '\t\tbusiness_rule=\'\'\n'
            code += '\t\tvalidation_rule=\'\'\n'
            qualifying_key='\'\''
            buy_currency='\'\''
            sell_currency='\'\''
            mtm_currency='\'\''
            for row in data:
                if row["python_implementation"].strip():
                     fields=row["data_fields_list"].split(',')
                     final_str=row["python_implementation"]
                     for field in fields:
                         new_str="row[\""+field+"\"]"
                         #fields names in the python_implementation should be within the tag <fld>field</fld>
                         #no space allowed between tags and the fields name
                         #final_str=final_str.replace("<fld>" + field + "</fld>",new_str)
                         final_str=final_str.replace("["+field+"]",new_str)
                     ##################################################################################
                     # Some specific literals to be used while defining rules
                     #  rule_type    |      Description
                     #  KEYCOLUMN    | The column name of the source data table which is unique.
                     #               | e.g. source_key, order_number etc.
                     #  BUYCURRENCY  | The column name of the buy currency for notional/book value.
                     #               | e.g. buy_currency, balance_currency, CHF etc.
                     #  SELLCURRENCY | The column name of the sell currency for a transaction.
                     #               | e.g. sell_currency, currency, SGD or can be null as well.
                     #  MTMCURRENCY  | The column name of the MTM currency for a transaction.
                     #               | e.g. mtm_currency, currency, CHF, SGD or can be null as well.
                     #  USEDATA      | Create the tag using data of the list of columns.
                     #               | e.g. buy_currency/sell_currency => USD/SGD
                     #               | business_date => 20170323 etc.
                     #  DERIVED      | Refer to the business_rule tag of an earlier rule during the ordered execution.
                     #               | e.g. 'IRS' not in DERIVED
                     #               | ',NRPT,' in DERIVED etc
                     ##################################################################################
                     if row["rule_type"]=='DERIVED':
                         final_str=final_str.replace("DERIVED","business_rule")
                     if row["rule_type"]=='KEYCOLUMN':
                         qualifying_key=final_str
                     elif row["rule_type"]=='BUYCURRENCY':
                         buy_currency=final_str
                     elif row["rule_type"]=='SELLCURRENCY':
                         sell_currency=final_str
                     elif row["rule_type"] == 'MTMCURRENCY':
                         mtm_currency = final_str
                     elif row["rule_type"]=='USEDATA':
                         code += '\t\tbusiness_rule+='+final_str+'+\',\'\n'
                     else:
                         code += '\t\tif '+final_str+':\n'
                         if row["business_or_validation"] == 'VALIDATION':
                             code += '\t\t\tvalidation_rule+=\'' + row["business_rule"].strip() + ',\'\n'
                         else:
                             code += '\t\t\tbusiness_rule+=\''+row["business_rule"].strip()+',\'\n'

            code += '\t\tif business_rule!=\'\' and validation_rule==\'\' and business_or_validation in [\'ALL\',\'BUSINESSRULES\']:\n'
            code += '\t\t\tqualified_data.append((source_id,business_date,'+qualifying_key+',business_rule,'+buy_currency+','+sell_currency+','+mtm_currency+'))\n'


            code += '\t\tif validation_rule!=\'\' and business_or_validation in [\'ALL\',\'VALIDATION\']:\n'
            code += '\t\t\tinvalid_data.append((source_id,business_date,' + qualifying_key + ',validation_rule))\n'

            code += '\tdb.transactmany("insert into invalid_data(source_id,business_date,qualifying_key,business_rules)\\\n \
                      values(%s,%s,%s,%s)",invalid_data)\n'
            code += '\tdb.transactmany("insert into qualified_data(source_id,business_date,qualifying_key,business_rules,buy_currency,sell_currency,mtm_currency)\\\n \
                      values(%s,%s,%s,%s,%s,%s,%s)",qualified_data)\n'
            #code += 'db.commit()\n'

            try:
                data_sources = db.query("select *  from data_catalog where business_date='"+business_date+"' and source_id="+str(source_id)).fetchone()
                exec(code)
                data_sources["file_load_status"] = "SUCCESS"
            except Exception as e:
                data_sources["file_load_status"] = "FAILED"
            finally:
                return (data_sources)
