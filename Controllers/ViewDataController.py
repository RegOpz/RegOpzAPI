from app import *
from flask import Flask, jsonify, request
from flask_restful import Resource
import time
import csv
from Helpers.DatabaseHelper import DatabaseHelper
from Constants.Status import *
from operator import itemgetter
from datetime import datetime
import json
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *
import pandas as pd
from Controllers.DataChangeController import DataChangeController
from Controllers.DocumentController import DocumentController

class ViewDataController(Resource):
    def __init__(self):
        self.domain_info=autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.dcc=DataChangeController()
            self.db=DatabaseHelper(self.tenant_info)
        self.doc = DocumentController()

    @authenticate
    def get(self):
        if(request.endpoint == 'get_date_heads_ep'):
            start_date = request.args.get('start_date') if request.args.get('start_date') != None else '19000101'
            end_date = request.args.get('end_date') if request.args.get('end_date') != None else '39991231'
            table_name = request.args.get('table_name')
            return self.render_data_load_dates(start_date, end_date, table_name)
        if(request.endpoint == 'report_ep'):
            source_id = request.args['source_id']
            business_date = request.args['business_date']
            page = request.args['page']
            filter = request.args['filter']
            return self.get_data_source(source_id=source_id,business_date=business_date,page=page,filter=filter)
        if(request.endpoint == 'table_data_ep'):
            table = request.args['table']
            filter = request.args['filter']
            page = request.args['page']
            return self.get_table_data(table=table,filter=filter,page=page)
        if(request.endpoint == 'get_source_ep'):
            start_date = request.args.get('startDate') if request.args.get('startDate') != None else '19000101'
            end_date = request.args.get('endDate') if request.args.get('endDate') != None else '39991231'
            return self.render_data_source_list(start_business_date=start_date, end_business_date=end_date)
        if(request.endpoint == 'report_linkage_ep'):
            source_id = request.args.get("source_id")
            qualifying_key = request.args.get("qualifying_key")
            business_date = request.args.get("business_date")
            return self.list_reports_for_data(source_id=source_id,qualifying_key=qualifying_key,business_date=business_date)
        if (request.endpoint == 'report_export_csv_ep'):
            table_name = request.args.get("table_name")
            business_ref = request.args.get("business_ref")
            export_reference = request.args.get("exportReference")
            sql = request.args.get("sql")
            return self.export_to_csv(table_name,business_ref,sql,export_reference=export_reference)

    def put(self, id=None):
        data = request.get_json(force=True)
        if id == None:
            return DATA_NOT_FOUND
        return self.update_or_delete_data(data, id)

    def post(self):
        if(request.endpoint == 'report_ep'):
            data = request.get_json(force=True)
            return self.insert_data(data)
        if(request.endpoint == 'apply_rules_ep'):
            source_info = request.get_json(force=True)
            source_id = source_info['source_id']
            business_date = source_info['business_date']
            business_or_validation = source_info['business_or_validation']
            return self.run_rules_engine(source_id=source_id,business_date=business_date,business_or_validation=business_or_validation)


    def delete(self, id=None):
        if id == None:
            return DATA_NOT_FOUND
        tableName = request.args.get("table_name")
        businessDate = request.args.get("business_date")
        res = self.delete_data(businessDate,tableName,id)
        return res

    def delete_data(self,business_date,table_name,id):
        app.logger.info("Deleting data")
        try:
            sql="delete from {} where business_date = %s and id=%s".format(table_name)
            #print(sql)

            params=(business_date,id,)
            #print(params)
            res=self.db.transact(sql,params)
            self.db.commit()

            return res
        except Exception as e:
            self.db.rollback()
            app.logger.error(e)
            return {"msg":e},500


    def insert_data(self,data):

        app.logger.info("Inseting data")
        try:
            res = self.dcc.insert_data(data)
            return res
        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500

    def update_or_delete_data(self,data,id):
        app.logger.info("Updating or Deleting data")
        try:
            res = self.dcc.update_or_delete_data(data, id)
            return res
        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500

    def ret_source_data_by_id(self, table_name,business_date,id):
        app.logger.info("Getting source data by id")
        try:
            query = 'select * from {} where business_date = %s and id = %s'.format(table_name)
            cur = self.db.query(query, (business_date,id, ))
            data = cur.fetchone()
            if data:
                return data
            return NO_BUSINESS_RULE_FOUND
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def get_data_source(self,source_id,business_date,page,filter=None):

        #db = DatabaseHelper()
        app.logger.info("Getting data source")
        try:
            app.logger.info("Getting source table name ")
            filter_maps = {
                "starts":{"operator": "like", "start_wild_char":"", "end_wild_char":"%"},
                "notstarts":{"operator": "not like", "start_wild_char":"", "end_wild_char":"%"},
                "ends":{"operator": "like", "start_wild_char":"%", "end_wild_char":""},
                "notends":{"operator": "not like", "start_wild_char":"%", "end_wild_char":""},
                "includes":{"operator": "like", "start_wild_char":"%", "end_wild_char":"%"},
                "excludes":{"operator": "not like", "start_wild_char":"%", "end_wild_char":"%"},
                "equals":{"operator": "=", "start_wild_char":"", "end_wild_char":""},
                "notequals":{"operator": "!=", "start_wild_char":"", "end_wild_char":""},
            }
            cur =self.db.query(
                "select source_table_name from data_source_information where source_id={}".format(source_id))
            table = cur.fetchone()

            start_page = int(page) * 100
            data_dict = {}
            app.logger.info("Getting data")
            filter_sql = ''
            if filter and filter != 'undefined':
                filter=json.loads(filter)
                for col in filter:
                    col_filter_sql =''
                    conditions = col['value'].split(",")
                    for ss in conditions:
                        if ss != '':
                            ss = ss.lstrip().split(":")
                            hint_list = ss[0].split(" ") if len(ss) > 1 else ["and","includes"]
                            app.logger.info("hint_list values {}".format(hint_list,))
                            hint_list = ["and"] + hint_list if len(hint_list)==1 else hint_list
                            app.logger.info("hint_list values 2nd list {}".format(hint_list,))
                            hint_join = hint_list[0]
                            hint = hint_list[1] if hint_list[1] in filter_maps.keys() else "includes"
                            c = ss[1] if len(ss) > 1 else ss[0]
                            fm=filter_maps[hint]
                            app.logger.info("fm values {}".format(fm,))
                            col_filter_sql += ' {0} '.format(hint_join,) if len(col_filter_sql) > 0 else ''
                            col_filter_sql += (col['id'] + ' {0} \'{1}' + c.replace("'","\'") + '{2}\'') \
                                          .format(fm["operator"],fm["start_wild_char"],fm["end_wild_char"])
                    filter_sql +="and ({0}) ".format(col_filter_sql)

            limit_sql = ' limit {0},100'.format(start_page)
            sqlqry = "select {0} from  {1} where business_date='{2}' {3} {4}"
            app.logger.info(sqlqry.format( '*', table['source_table_name'] ,business_date, filter_sql, limit_sql))
            cur = self.db.query(sqlqry.format( '*', table['source_table_name'] ,business_date, filter_sql, limit_sql))
            data = cur.fetchall()
            cols = [i[0] for i in cur.description]
            app.logger.info(sqlqry.format( 'count(*)', table['source_table_name'] ,business_date, filter_sql, ''))
            count = self.db.query(sqlqry.format( 'count(*) as count', table['source_table_name'] ,business_date, filter_sql, '')).fetchone()
            sql = sqlqry.format( '*', table['source_table_name'] ,business_date, filter_sql, '')
            data_dict['cols'] = cols
            data_dict['rows'] = data
            data_dict['count'] = count['count']
            data_dict['table_name'] = table['source_table_name']
            data_dict['sql'] = sql

            # print(data_dict)
            return data_dict
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def get_table_data(self,table,filter,page):
        app.logger.info("Getting table data")

        try:
            if page is None:
                page = 0
            start_page = int(page) * 100
            if filter is None :
                filter = '1'

            data_dict = {}
            sql = "select * from {0} where 1 and {1} limit {2},100".format(table ,filter ,start_page)

            app.logger.info("Getting 100 rows from table")
            cur = self.db.query(sql)
            data=cur.fetchall()

            for i,d in enumerate(data):
                for k,v in d.items():
                    if isinstance(v,datetime):
                        d[k] = d[k].isoformat()


            cols = [i[0] for i in cur.description]
            app.logger.info("Getting count from table")
            count = self.db.query(sql.replace('*','count(*) as count ')).fetchone()

            data_dict['cols'] = cols
            data_dict['rows'] = data
            data_dict['count'] = count['count']
            data_dict['table_name'] = table
            data_dict['sql'] = sql

            # print(data_dict)
            return data_dict
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def render_data_load_dates(self,start_business_date='19000101',end_business_date='39991231',catalog_table='data_catalog'):
        app.logger.info("Getting data load dates")
        try:

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
            #db = DatabaseHelper()
            if catalog_table == 'data_catalog':
                sql = "select distinct business_date from data_catalog where business_date between "+ start_business_date + " and " + end_business_date + " order by business_date"
            if catalog_table == 'report_catalog':
                sql = "select distinct as_of_reporting_date as business_date from report_catalog where as_of_reporting_date between "+ start_business_date + " and " + end_business_date + " order by as_of_reporting_date"

            app.logger.info("Getting data load dates from catalog")
            catalog=self.db.query(sql).fetchall()

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
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500


    def render_data_source_list(self,start_business_date='19000101',end_business_date='39991231'):
        app.logger.info("Getting data source list")

        try:
            data_sources={}
            data_sources['start_date']=start_business_date
            data_sources['end_date']=end_business_date
            sql="select dc.*,dsi.source_description, dsi.country from data_catalog dc, data_source_information dsi " + \
                " where dc.source_id=dsi.source_id and dc.business_date between '{0}' and '{1}' " + \
                " order by dc.business_date "
            sql=sql.format(start_business_date ,end_business_date )
            data_feeds = self.db.query(sql).fetchall()

            #print(data_sources)
            for idx,src in enumerate(data_feeds):
                sql = "select business_date, source_id, version, br_version from qualified_data_vers" + \
                    " where source_id={0} and business_date={1}".format(src["source_id"],src["business_date"])
                versions = self.db.query(sql).fetchall()
                data_feeds[idx]['versions']=versions

            data_sources['data_sources']=data_feeds
            return (data_sources)
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def list_reports_for_data(self,source_id,qualifying_key,business_date):
        app.logger.info("Getting list of reports for data")
        try:
            data_key_list = eval("["+qualifying_key+"]")
            app.logger.info("data key list {}".format(data_key_list,))
            self.db.transact("create temporary table tmp_report_link_key_list(key_source_id int,key_id bigint,key_date int)")
            self.db.transact("truncate table tmp_report_link_key_list")
            self.db.transactmany("insert into tmp_report_link_key_list(key_source_id,key_id,key_date) values (%s,%s,%s)",data_key_list)
            sql="select rqdl.source_id,rqdl.report_id,rqdl.sheet_id,rqdl.cell_id,rqdl.cell_calc_ref," + \
            " rqdl.reporting_date,rqdl.version,k.key_id as qualifying_key,k.key_date as business_date " + \
            " from report_qualified_data_link rqdl, tmp_report_link_key_list k where " + \
            " rqdl.source_id= k.key_source_id " + \
            " and instr(concat(',',rqdl.id_list,','),concat(',',k.key_id,','))"

            app.logger.info("Getting report list for particular data")
            report_list=self.db.query(sql).fetchall()

            result_set = []
            for data in report_list:
                app.logger.info("Getting business rules for data {}".format(data,))
                data_qual = self.db.query(
                    "select * from qualified_data where source_id = %s and qualifying_key = %s and business_date=%s",
                    (data['source_id'],data['qualifying_key'],data['business_date'])).fetchall()

                app.logger.info("Getting cell business rules for data {}".format(data_qual[0],))
                cell_rule=self.db.query("select * from report_calc_def where cell_calc_ref=%s and report_id = %s\
                            and sheet_id = %s and cell_id = %s",(data['cell_calc_ref'],data["report_id"],
                                                                 data["sheet_id"],data["cell_id"])).fetchone()

                if cell_rule:
                    data["cell_business_rules"]=cell_rule["cell_business_rules"]
                    data["data_qualifying_rules"]=data_qual[0]["business_rules"]
                    result_set.append(data)

            return result_set
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def export_to_csv(self,table_name,business_ref,sql,export_reference):
        app.logger.info("Exporting to CSV")
        try:
            app.logger.info("Getting data from table")
            if sql and sql !='undefined' and sql !='null':
                app.logger.info(sql)
                cur = self.db.query(sql)
            if export_reference and export_reference != 'undefined' and export_reference != 'null':
                app.logger.info(export_reference)
                export_reference = json.loads(export_reference)
                export_reference['export_to_csv']='Y'
                cur = self.doc.cell_drill_down_data(**export_reference)

            data = cur.fetchall()
            keys = [i[0] for i in cur.description]
            filename=table_name+business_ref+str(time.time())+".csv"

            with open('./static/'+filename, 'wt') as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                dict_writer.writeheader()
                dict_writer.writerows(data)
            return { "file_name": filename }
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500


    def run_rules_engine(self,source_id,business_date,business_or_validation='ALL'):

        #db to insert qualified/invalid_data
        db=DatabaseHelper(self.tenant_info)
        #dbsf to query data and create source data cursor
        dbsf=DatabaseHelper(self.tenant_info)


        sql_str = 'Select source_id,source_table_name from data_source_information'
        if source_id!='ALL':
            sql_str+=' where source_id =' + str(source_id)

        tables=db.query(sql_str).fetchall()

        for src in tables:
            code = ''
            qdf = pd.DataFrame(columns=['source_id', 'business_date', 'qualifying_key', 'business_rules', 'buy_currency',
                         'sell_currency', 'mtm_currency'])
            # idf=pd.DataFrame()
            # Select the data in the rule ececution order to facilitate derived rules definitions in the rule
            #Stamp the business_rule version
            br_version= db.query("select version,id_list from business_rules_vers where source_id=%s and version=(select\
                        max(version) version from business_rules_vers where source_id=%s)",(src['source_id'],src['source_id'])).fetchone()
            brdf = pd.DataFrame(db.query('select id,rule_execution_order,business_rule,source_id,'+\
                            ' data_fields_list,python_implementation, business_or_validation,rule_type' +\
                            ' from business_rules where source_id=%s and in_use=\'Y\' ' + \
                            ' order by rule_execution_order asc',\
                            (src["source_id"],)).fetchall())
            brdf[['id', 'source_id', 'rule_execution_order']] = brdf[['id', 'source_id', 'rule_execution_order']].astype(dtype='int64', errors='ignore')
            br_id_list = list(map(int,brdf['id'].tolist()))
            br_id_list.sort()
            br_id_list_str = ",".join(map(str, br_id_list))

            if not br_version:
                br_version_no=1
            else:
               old_id_list=map(int,br_version['id_list'].split(','))
               #print(set(br_id_list),set(old_id_list))
               br_version_no=br_version['version']+1 if set(br_id_list)!= set(old_id_list) else br_version['version']

            if not br_version or br_version_no != br_version['version']:
               db.transact("insert into business_rules_vers(source_id,version,id_list) values(%s,%s,%s)",(src['source_id'],br_version_no,br_id_list_str))

            # code += 'if business_or_validation in [\'ALL\',\'BUSINESSRULES\']:\n'
            # code += '\tdb.transact("delete from qualified_data where source_id='+str(src["source_id"])+' and business_date=%s",(business_date,))\n'
            code += 'if business_or_validation in [\'ALL\',\'VALIDATION\']:\n'
            code += '\tdb.transact("delete from invalid_data where source_id=' + str(src["source_id"]) + ' and business_date=%s",(business_date,))\n'
            code += 'curdata=dbsf.query("SELECT * FROM  '+src["source_table_name"]+' where business_date=%s and in_use=\'Y\'",(business_date,))\n'
            code += 'start_process=time.time()\n'
            code += 'while True:\n'
            code += '\tdata=curdata.fetchmany(50000)\n'
            code += '\tif not data:\n'
            code += '\t\tbreak\n'
            code +='\tqualified_data=[]\n'
            code +='\tinvalid_data=[]\n'
            code += '\tstart=time.time()\n'
            code += '\tfor row in data:\n'
            code += '\t\tbusiness_rule=\'\'\n'
            code += '\t\tvalidation_rule=\'\'\n'
            qualifying_key='\'row[\"id\"]\''
            buy_currency='\'\''
            sell_currency='\'\''
            mtm_currency='\'\''
            for idx,row in brdf.iterrows():
                if row["python_implementation"].strip():
                     fields=row["data_fields_list"].split(',')
                     #Replace "," of the fields list as " is not None and " to avoid NoneType error
                     #Also included [] to enclose fields for replacement in the loop
                     NoneType_chk_str="["+row["data_fields_list"].replace(",","] is not None and [")
                     #Now to check the last field in the fields list for NoneType error
                     NoneType_chk_str+="] is not None "
                     final_str=row["python_implementation"]
                     for field in fields:
                         new_str="row[\""+field+"\"]"
                         #fields names in the python_implementation should be within the tag <fld>field</fld>
                         #no space allowed between tags and the fields name
                         #final_str=final_str.replace("<fld>" + field + "</fld>",new_str)
                         final_str=final_str.replace("["+field+"]",new_str).replace(new_str,'str('+ new_str +')')
                         NoneType_chk_str=NoneType_chk_str.replace("["+field+"]",new_str)
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
                         qualifying_key='row[\"id\"]' #final_str
                     elif row["rule_type"]=='BUYCURRENCY':
                         buy_currency=final_str
                     elif row["rule_type"]=='SELLCURRENCY':
                         sell_currency=final_str
                     elif row["rule_type"] == 'MTMCURRENCY':
                         mtm_currency = final_str
                     elif row["rule_type"]=='USEDATA':
                         #Now check each element for None and set it to '' if None else use the value
                         for field in fields:
                             final_str=final_str.replace("row[\""+field+"\"]","(row[\""+field+"\"],\'\')[row[\""+field+"\"] is None]")

                         code += '\t\tbusiness_rule+=str('+final_str+')+\',\'\n'
                     else:
                         code += '\t\tif ('+NoneType_chk_str+') and ('+final_str+'):\n'
                         if row["business_or_validation"] == 'VALIDATION':
                             code += '\t\t\tif \'' + row["business_rule"].strip() + '\' not in validation_rule.split(\',\'):\n'
                             code += '\t\t\t\tvalidation_rule+=\'' + row["business_rule"].strip() + ',\'\n'
                         else:
                             code += '\t\t\tif \'' + row["business_rule"].strip() + '\' not in business_rule.split(\',\'):\n'
                             code += '\t\t\t\tbusiness_rule+=\''+row["business_rule"].strip()+',\'\n'

            code += '\t\tif business_rule!=\'\' and validation_rule==\'\' and business_or_validation in [\'ALL\',\'BUSINESSRULES\']:\n'
            code += '\t\t\tqualified_data.append((source_id,business_date,'+qualifying_key+',business_rule,'+buy_currency+','+sell_currency+','+mtm_currency+'))\n'
            # code += '\t\t\tqdf=qdf.append({\'source_id\':int(source_id),\'business_date\':int(business_date),\'qualifying_key\':int(' + qualifying_key + '),\'business_rules\':business_rule,\'buy_currency\':' + buy_currency + ',\'sell_currency\':' + sell_currency + ',\'mtm_currency\':' + mtm_currency + '},ignore_index=True)\n'
            #code += '\t\tprint(qdf)\n'

            code += '\t\tif validation_rule!=\'\' and business_or_validation in [\'ALL\',\'VALIDATION\']:\n'
            code += '\t\t\tinvalid_data.append((source_id,business_date,' + qualifying_key + ',validation_rule))\n'

            code += '\t\tif validation_rule==\'\' and business_rule==\'\' and business_or_validation in [\'ALL\',\'VALIDATION\']:\n'
            code += '\t\t\tinvalid_data.append((source_id,business_date,' + qualifying_key + ',\'No rule applicable!!\'))\n'


            code += '\tprint("Time taken for data loop : " + str((time.time()-start)*1000))\n'
            code += '\tstart=time.time()\n'
            code += '\tdb.transactmany("insert into invalid_data(source_id,business_date,qualifying_key,business_rules)\\\n \
                      values(%s,%s,%s,%s)",invalid_data)\n'
            # code += '\tdb.transactmany("insert into qualified_data(source_id,business_date,qualifying_key,business_rules,buy_currency,sell_currency,mtm_currency)\\\n \
            #           values(%s,%s,%s,%s,%s,%s,%s)",qualified_data)\n'
            code += '\tif len(qualified_data) > 0 :\n'
            code += '\t\tqdf=qdf.append(pd.DataFrame(qualified_data,columns=[\'source_id\',\'business_date\',\'qualifying_key\',\'business_rules\',\'buy_currency\',\'sell_currency\',\'mtm_currency\']),ignore_index=True)\n'
            code += '\telse:\n'
            code += '\t\tqdf=pd.DataFrame(columns=[\'source_id\',\'business_date\',\'qualifying_key\',\'business_rules\',\'buy_currency\',\'sell_currency\',\'mtm_currency\'])\n'
            code += '\tprint("Time taken for data inserts : "+ str((time.time()-start)*1000))\n'
            code += 'print("Total Time taken for data processing : "+ str((time.time()-start_process)*1000))\n'
            data_sources = db.query("select *  from data_catalog where business_date='"+business_date+"' and source_id="+str(source_id)).fetchone()
            try:
                print("Before exec...")

                self.update_data_catalog(status='RUNNING',source_id=source_id,business_date=business_date)
                ldict=locals()
                exec(code,globals(),ldict)
                existing_qdf=pd.DataFrame(db.query("select * from qualified_data where source_id=%s and business_date=%s",(source_id,business_date)).fetchall())
                ldict['qdf'][['source_id','business_date','qualifying_key']] = ldict['qdf'][['source_id','business_date','qualifying_key']].astype(dtype='int32', errors='ignore')
                #ldict['qdf'][['source_id','business_date','qualifying_key']].astype(dtype=int, errors='ignore')
                #existing_qdf[['source_id', 'business_date', 'qualifying_key']].astype(dtype=int, errors='ignore')
                #print(existing_qdf.dtypes)
                #print(ldict['qdf'].dtypes)
                if existing_qdf.empty:
                    qdf=ldict['qdf']
                    qdf['id']= qdf.index + 1
                    qdf['id']=qdf['id'].astype(dtype='int32',errors='ignore')
                    id_list=qdf['id'].tolist()
                else:
                    qdf = pd.merge(ldict['qdf'], existing_qdf, how='left', on=list(ldict['qdf'].columns),suffixes=('', '_old'))
                    qdf['id'].fillna(0,inplace=True)
                    qdf_old=qdf.loc[qdf['id']!=0]
                    qdf_old['id'] = qdf_old['id'].astype(dtype='int32', errors='ignore')
                    old_id_list=qdf_old['id'].tolist()
                    qdf_new=qdf[qdf['id']==0]
                    qdf_new=qdf_new.reset_index(drop=True)
                    qdf_new['id'] = qdf_new['id'].astype(dtype='int32', errors='ignore')
                    max_id=existing_qdf['id'].max()
                    new_id_list=[max_id+idx+1 for idx,rec in qdf_new.iterrows() ]
                    id_list=old_id_list+new_id_list
                    id_df=pd.DataFrame({'id':new_id_list})
                    qdf_new.update(id_df)
                    qdf=qdf_new

                # print(qdf)
                # print(max_id)
                # print(id_list)

                # for col in ['source_id','business_date','qualifying_key','id']:
                #     qdf[col]=qdf[col].astype(dtype='int32',errors='ignore')
                if not qdf.empty:
                    qdf[['source_id','business_date','qualifying_key','id']]=qdf[['source_id','business_date','qualifying_key','id']].astype(dtype='int32',errors='ignore')
                    qdf_records=list(qdf.itertuples(index=False, name=None))
                    placeholder= ",".join(['%s']*len(qdf.columns))
                    columns = ",".join(qdf.columns)
                    id_list_str=",".join(map(str, id_list))
                    max_version=db.query("select max(version) version from qualified_data_vers where business_date=%s and source_id=%s",\
                                         (business_date,source_id)).fetchone()
                    version=max_version['version']+1 if max_version['version'] else 1
                    db.transactmany("insert into qualified_data({0}) values({1})".format(columns,placeholder),qdf_records)
                    db.transact("insert into qualified_data_vers(business_date,source_id,version,br_version,id_list) values (%s,%s,%s,%s,%s)",\
                                (business_date,source_id,version,br_version_no,id_list_str))
                db.commit()
                data_sources["file_load_status"] = "SUCCESS"
                #print(code)
                print("End of try....")
                self.update_data_catalog(status=data_sources["file_load_status"],source_id=source_id,business_date=business_date)
                return {"msg": "Apply rule SUCCESSFULLY COMPLETED for source ["+str(source_id)+"] Business date ["+str(business_date)+"]."}, 200
            except Exception as e:
                print("In except..." + str(e))
                db.rollback()
                #print(code)
                data_sources["file_load_status"] = "FAILED"
                self.update_data_catalog(status=data_sources["file_load_status"],source_id=source_id,business_date=business_date)
                return {"msg": str(e) + " Apply rule FAILED for source ["+ str(source_id) +"] Business date ["+str(business_date)+"]"}, 400
            # finally:
            #     print("In finally")
            #     self.update_data_catalog(status=data_sources["file_load_status"],source_id=source_id,business_date=business_date)
            #     # return data_sources


    def update_data_catalog(self,status,source_id,business_date):
        app.logger.info("Updating data catalog")
        try:
            self.db.transact("update data_catalog set file_load_status=%s \
                        where source_id=%s and business_date=%s",(status,source_id,business_date))
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            app.logger.error(e)
            return {"msg":e},500
