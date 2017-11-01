from app import *
from flask import Flask, jsonify, request
from flask_restful import Resource
import time
import csv
from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.DatabaseOps import DatabaseOps
from Constants.Status import *
from operator import itemgetter
from datetime import datetime
class ViewDataController(Resource):
    def __init__(self):
        self.dbOps = DatabaseOps('data_change_log')
        self.db=DatabaseHelper()
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
            return self.get_data_source(source_id=source_id,business_date=business_date,page=page)
        if(request.endpoint == 'table_data_ep'):
            table = request.args['table']
            filter = request.args['filter']
            page = request.args['page']
            return self.get_table_data(table=table,filter=filter,page=page)
        if(request.endpoint == 'get_source_ep'):
            start_date = request.args.get('startDate') if request.args.get('startDate') != None else '19000101'
            end_date = request.args.get('endDate') if request.args.get('endDate') != None else '39991231'
            catalog_type = request.args.get('catalog_type')
            return self.render_data_source_list(start_business_date=start_date, end_business_date=end_date, catalog_type=catalog_type)
        if(request.endpoint == 'report_linkage_ep'):
            source_id = request.args.get("source_id")
            qualifying_key = request.args.get("qualifying_key")
            business_date = request.args.get("business_date")
            return self.list_reports_for_data(source_id=source_id,qualifying_key=qualifying_key,business_date=business_date)
        if (request.endpoint == 'report_export_csv_ep'):
            table_name = request.args.get("table_name")
            business_ref = request.args.get("business_ref")
            sql = request.args.get("sql")
            return self.export_to_csv(table_name,business_ref,sql)

    def put(self, id=None):
        data = request.get_json(force=True)
        if id == None:
            return DATA_NOT_FOUND
        res = self.dbOps.update_or_delete_data(data, id)
        return res

    def post(self):
        if(request.endpoint == 'report_ep'):
            data = request.get_json(force=True)
            res = self.dbOps.insert_data(data)
            return res
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

            return res
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500


    def insert_data(self,data):

        app.logger.info("Inseting data")
        try:
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
                if col=='id':
                    params.append(None)
                else:
                    params.append(update_info[col])

            placeholders=placeholders[:len(placeholders)-1]
            placeholders+=")"
            sql=sql[:len(sql)-1]
            sql+=") values "+ placeholders

            params_tuple=tuple(params)
            #print(sql)
            #print(params_tuple)
            res=self.db.transact(sql,params_tuple)
            self.db.commit()

            return self.ret_source_data_by_id(table_name,business_date,res)
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def update_data(self,data,id):
        app.logger.info("Updating data")
        try:
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
            sql+=" where business_date =%s and id=%s"
            params.append(business_date)
            params.append(id)
            params_tuple=tuple(params)

            #print(sql)
            #print(params_tuple)

            res=self.db.transact(sql,params_tuple)

            if res==0:
                self.db.commit()
                return self.ret_source_data_by_id(table_name,business_date,id)

            self.db.rollback()
            return UPDATE_ERROR
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

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

    def get_data_source(self,source_id,business_date,page):

        #db = DatabaseHelper()
        app.logger.info("Getting data source")
        try:
            app.logger.info("Getting source table name ")
            cur =self.db.query(
                "select source_table_name from data_source_information where source_id={}".format(source_id))
            table = cur.fetchone()

            start_page = int(page) * 100
            data_dict = {}
            app.logger.info("Getting data")
            cur = self.db.query("select * from  {0} where business_date='{1}' limit {2}, 100".format( table['source_table_name'] ,business_date,start_page))
            data = cur.fetchall()
            cols = [i[0] for i in cur.description]
            count = self.db.query("select count(*) as count from {0} where business_date='{1}'".format( table['source_table_name'],business_date)).fetchone()
            sql = "select * from {0} where business_date='{1}'".format( table['source_table_name'] ,business_date )
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
        app.logger.error("Getting data source list")

        try:
            data_sources={}
            data_sources['start_date']=start_business_date
            data_sources['end_date']=end_business_date
            data_feeds = self.db.query("select *  from data_catalog  where business_date between '{0}' and '{1}' order by business_date ".format(start_business_date ,end_business_date )).fetchall()

            #print(data_sources)
            data_sources['data_sources']=data_feeds
            return (data_sources)
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def list_reports_for_data(self,source_id,qualifying_key,business_date):
        app.logger.info("Getting list of reports for data")
        try:
            sql="select * from report_qualified_data_link where source_id=%s and qualifying_key=%s and business_date=%s"

            app.logger.info("Getting report list for particular data")
            report_list=self.db.query(sql,(source_id,qualifying_key,business_date)).fetchall()

            result_set = []
            for data in report_list:
                app.logger.info("Getting business rules for data")
                data_qual = self.db.query(
                    "select * from qualified_data where source_id = %s and qualifying_key = %s and business_date=%s",
                    (data['source_id'],data['qualifying_key'],data['business_date'])).fetchone()

                app.logger.info("Getting cell business rules for data")
                cell_rule=self.db.query("select * from report_calc_def where cell_calc_ref=%s and report_id = %s\
                            and sheet_id = %s and cell_id = %s",(data['cell_calc_ref'],data["report_id"],
                                                                 data["sheet_id"],data["cell_id"])).fetchone()

                data["cell_business_rules"]=cell_rule["cell_business_rules"]
                data["data_qualifying_rules"]=data_qual["business_rules"]
                result_set.append(data)

            return result_set
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500

    def export_to_csv(self,table_name,business_ref,sql):
        app.logger.info("Exporting to CSV")
        try:
            app.logger.info("Getting data from table")
            cur = self.db.query(sql)

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
        db=DatabaseHelper()
        #dbsf to query data and create source data cursor
        dbsf=DatabaseHelper()

        sql_str = 'Select source_id,source_table_name from data_source_information'
        if source_id!='ALL':
            sql_str+=' where source_id =' + str(source_id)

        tables=db.query(sql_str).fetchall()

        for src in tables:
            # Select the data in the rule ececution order to facilitate derived rules definitions in the rule
            data=db.query('select * from business_rules where source_id=%s and in_use=\'Y\' order by rule_execution_order asc',(src["source_id"],)).fetchall()

            code = 'if business_or_validation in [\'ALL\',\'BUSINESSRULES\']:\n'
            code += '\tdb.transact("delete from qualified_data where source_id='+str(src["source_id"])+' and business_date=%s",(business_date,))\n'
            code += 'if business_or_validation in [\'ALL\',\'VALIDATION\']:\n'
            code += '\tdb.transact("delete from invalid_data where source_id=' + str(src["source_id"]) + ' and business_date=%s",(business_date,))\n'
            code += 'curdata=dbsf.query("SELECT * FROM  '+src["source_table_name"]+' where business_date=%s",(business_date,))\n'
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
            qualifying_key='\'\''
            buy_currency='\'\''
            sell_currency='\'\''
            mtm_currency='\'\''
            for row in data:
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
                         qualifying_key=final_str
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
                             code += '\t\t\tvalidation_rule+=\'' + row["business_rule"].strip() + ',\'\n'
                         else:
                             code += '\t\t\tbusiness_rule+=\''+row["business_rule"].strip()+',\'\n'

            code += '\t\tif business_rule!=\'\' and validation_rule==\'\' and business_or_validation in [\'ALL\',\'BUSINESSRULES\']:\n'
            code += '\t\t\tqualified_data.append((source_id,business_date,'+qualifying_key+',business_rule,'+buy_currency+','+sell_currency+','+mtm_currency+'))\n'


            code += '\t\tif validation_rule!=\'\' and business_or_validation in [\'ALL\',\'VALIDATION\']:\n'
            code += '\t\t\tinvalid_data.append((source_id,business_date,' + qualifying_key + ',validation_rule))\n'

            code += '\t\tif validation_rule==\'\' and business_rule==\'\' and business_or_validation in [\'ALL\',\'VALIDATION\']:\n'
            code += '\t\t\tinvalid_data.append((source_id,business_date,' + qualifying_key + ',\'No rule applicable!!\'))\n'


            code += '\tprint("Time taken for data loop : " + str((time.time()-start)*1000))\n'
            code += '\tstart=time.time()\n'
            code += '\tdb.transactmany("insert into invalid_data(source_id,business_date,qualifying_key,business_rules)\\\n \
                      values(%s,%s,%s,%s)",invalid_data)\n'
            code += '\tdb.transactmany("insert into qualified_data(source_id,business_date,qualifying_key,business_rules,buy_currency,sell_currency,mtm_currency)\\\n \
                      values(%s,%s,%s,%s,%s,%s,%s)",qualified_data)\n'
            code += '\tprint("Time taken for data inserts : "+ str((time.time()-start)*1000))\n'
            code += 'print("Total Time taken for data processing : "+ str((time.time()-start_process)*1000))\n'
            #code += 'db.commit()\n'
            data_sources = db.query("select *  from data_catalog where business_date='"+business_date+"' and source_id="+str(source_id)).fetchone()
            try:
                print("Before exec...")

                self.update_data_catalog(status='RUNNING',source_id=source_id,business_date=business_date)

                exec(code)
                db.commit()
                data_sources["file_load_status"] = "SUCCESS"
                #print(code)
                print("End of try....")
            except Exception as e:
                print("In except..." + str(e))
                db.rollback()
                #print(code)
                data_sources["file_load_status"] = "FAILED"
            finally:
                print("In finally")
                self.update_data_catalog(status=data_sources["file_load_status"],source_id=source_id,business_date=business_date)
                return data_sources

    def update_data_catalog(self,status,source_id,business_date):
        app.logger.info("Updating data catalog")
        try:
            self.db.transact("update data_catalog set file_load_status=%s \
                        where source_id=%s and business_date=%s",(status,source_id,business_date))
            self.db.commit()
        except Exception as e:
            app.logger.error(e)
            return {"msg":e},500
