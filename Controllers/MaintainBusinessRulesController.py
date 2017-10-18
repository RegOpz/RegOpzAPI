from app import *
from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.DatabaseOps import DatabaseOps
import csv
import time
from datetime import datetime
import traceback as tb
from Constants.Status import *


class MaintainBusinessRulesController(Resource):
    def __init__(self):
        self.dbOps=DatabaseOps('def_change_log')
        self.db=DatabaseHelper()

    def get(self, id=None, source_id=None, page=0, col_name=None,business_rule=None):
        app.logger.info(request.endpoint)
        if request.endpoint == "business_rule_export_to_csv_ep":
            source = request.args.get('source_id')
            return self.export_to_csv(source_id=source)
        if request.endpoint == "business_rule_linkage_ep":
            return self.list_reports_for_rule(business_rule=business_rule)
        if request.endpoint == "business_rule_linkage_multiple_ep":
            source = request.args.get('source_id')
            rules = request.args.get('rules')
            #app.logger.info(source,rules)
            return self.list_reports_for_rule_list(source_id=source,business_rule_list=rules)
        if request.endpoint == "business_rule_drill_down_rules_ep":
            app.logger.info('Inside rules drilldown ep')
            source = request.args.get('source_id')
            rules = request.args.get('rules')
            page = request.args.get('page')
            return self.get_business_rules_list_by_source(rules=rules,source=source,page=page)
        if request.endpoint == 'get_br_source_suggestion_list_ep':
            source_id = request.args.get('source_id')
            return self.get_source_suggestion_list(source_id=source_id)
        if request.endpoint == 'get_br_source_column_suggestion_list_ep':
            table_name = request.args.get('table_name')
            return self.get_source_column_suggestion_list(table_name=table_name)
        if id:
            return self.render_business_rule_json(id)
        elif page and source_id==None and col_name == None:
            return self.render_business_rules_json(page)
        elif source_id and page and col_name == None:
            return self.render_business_rules_json(page,source_id)
        elif col_name:
            direction = request.args.get('direction')
            return self.render_business_rules_json(page, "source_id",(col_name,direction))

    def post(self,page=None):

        if request.endpoint == "business_rules_ep_filtered":
            filter_conditions = request.get_json(force=True)
            return self.get_business_rules_filtered(filter_conditions)

        if request.endpoint == "validate_python_expr_ep":
            expr_obj=request.get_json(force=True)
            return self.validate_python_expression(expr_obj)

        br = request.get_json(force=True)
        res = self.dbOps.insert_data(br)
        return res

    def put(self, id=None):
        if id == None:
            return BUSINESS_RULE_EMPTY
        data = request.get_json(force=True)
        res = self.dbOps.update_or_delete_data(data, id)
        return res

    # def delete(self, id=None):
    #       if id == None:
    #               return BUSINESS_RULE_EMPTY
    #       res = self.delete_business_rules(id)
    #       return res
    def render_business_rules_json(self, page=0, source_id="source_id", order=None):
        startPage =  int(page)*100
        business_rules_dict = {}
        business_rules_list = []
        if order:
            cur = self.db.query('select * from business_rules where source_id=' + str(source_id) + ' order by ' + order[0] + ' ' + order[1] + ' limit ' + str(startPage) + ', 100')
        else:
			# cur = db.query('select * from business_rules where source_id=' + str(source_id) + ' limit ' + str(startPage) + ', 100')
            cur = db.query('select * from business_rules where source_id=' + str(source_id))
        business_rules = cur.fetchall()
        cols = [i[0] for i in cur.description]
        business_rules_dict['cols'] = cols

        for br in business_rules:
            business_rules_list.append(br)
        business_rules_dict['rows'] = business_rules_list
        count = self.db.query('select count(*) as count from business_rules where source_id=' + str(source_id)).fetchone()
        business_rules_dict['count'] = count['count']
        json_dump = business_rules_dict
        return json_dump

    def get_business_rules_filtered(self,filter_conditions):
        filter_string = ""
        for filter_condition in filter_conditions:
            filter_string = filter_string + " AND " + filter_condition['field_name'] + "='" + filter_condition['value'] + "'"
        query = 'select * from business_rules where 1' + filter_string
        cur = self.db.query(query)
        data = cur.fetchall()
        if data:
            return data
        return NO_BUSINESS_RULE_FOUND

    def get_business_rules_list_by_source(self,rules,source,page):

        if page is None:
            page = 0
        startPage = int(page) * 100
        data_dict = {}
        where_clause = ''
        sql = 'select * from business_rules where 1 '
        if source is not None:
            where_clause += ' and source_id =' + source
        if rules is not None:
            where_clause += ' and business_rule in (\''+rules.replace(',','\',\'')+'\')'
        cur = self.db.query(sql + where_clause + " limit " + str(startPage) + ", 100")
        data = cur.fetchall()
        cols = [i[0] for i in cur.description]
        count = self.db.query(sql.replace('*','count(*) as count ') + where_clause).fetchone()
        data_dict['cols'] = cols
        data_dict['rows'] = data
        data_dict['count'] = count['count']
        data_dict['table_name'] = 'business_rules'
        data_dict['sql'] = sql

        return data_dict

    def render_business_rule_json(self, id):
        query = 'select * from business_rules where id = %s'
        cur = self.db.query(query, (id, ))
        data = cur.fetchone()
        if data:
            return data
        return NO_BUSINESS_RULE_FOUND

    def ret_source_data_by_id(self, table_name,id):
        query = 'select * from ' + table_name + ' where id = %s'
        cur = self.db.query(query, (id, ))
        data = cur.fetchone()
        for k,v in data.items():
            if isinstance(v,datetime):
                data[k] = data[k].isoformat()
                app.logger.info(data[k], type(data[k]))
        if data:
            return data
        return NO_BUSINESS_RULE_FOUND

    def insert_business_rules(self, data):

        table_name = data['table_name']
        update_info = data['update_info']
        update_info_cols = update_info.keys()

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
        app.logger.info(sql)
        app.logger.info(params_tuple)
        try:
            res=self.db.transact(sql,params_tuple)
            self.db.commit()
        except Exception as e:
            app.logger.error(e)
            return { "msg": e },500

        return self.ret_source_data_by_id(table_name,res)

    def update_business_rules(self, data, id):

        table_name=data['table_name']
        update_info=data['update_info']
        update_info_cols=update_info.keys()

        sql= 'update '+table_name+ ' set '
        params=[]
        for col in update_info_cols:
            sql+=col +'=%s,'
            params.append(update_info[col])

        sql=sql[:len(sql)-1]
        sql+=" where id=%s"
        params.append(id)
        params_tuple=tuple(params)

        app.logger.info(sql)
        app.logger.info(params_tuple)
        try:
            res=self.db.transact(sql,params_tuple)

            if res==0:
                self.db.commit()
                return self.ret_source_data_by_id(table_name,id)

            self.db.rollback()
            return UPDATE_ERROR
        except Exception as e:
            app.logger.error(e)
            return { "msg": e },500

    def delete_business_rules(self, id):
        sql = 'delete from business_rules where id=%s'
        params = (id, )
        try:
            res = self.db.transact(sql, params)
            if res == 0:
                self.db.commit()
                return res

            self.db.rollback()
            return DATABASE_ERROR
        except Exception as e:
            app.logger.error(e)
            return { "msg": e },500

    def export_to_csv(self, source_id='ALL'):

        sql = 'select * from business_rules where 1=1'

        if source_id != 'ALL':
            sql += " and source_id='" + str(source_id) + "'"

        cur = self.db.query(sql)

        business_rules = cur.fetchall()

         # keys=business_rules[0].keys()

        keys = [i[0] for i in cur.description]
        filename="business_rules_source_" + str(source_id) + "_" + str(time.time())+ ".csv"

        with open('./static/'+filename, 'wt') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(business_rules)
        return { "file_name": filename }

    def list_reports_for_rule(self,**kwargs):

        parameter_list = ['business_rule']

        if set(parameter_list).issubset(set(kwargs.keys())):
            business_rule = kwargs['business_rule']

        else:
            return BUSINESS_RULE_EMPTY

        sql = "select distinct report_id,sheet_id,cell_id from report_calc_def, in_use \
        where cell_business_rules like '%," + business_rule + ",%'"
        cur=self.db.query(sql)
        report_list = cur.fetchall()

        return [(rpt['report_id'], rpt['sheet_id'], rpt['cell_id']) for rpt in report_list]

    def list_reports_for_rule_list(self,**kwargs):

        parameter_list = ['source_id', 'business_rule_list']

        if set(parameter_list).issubset(set(kwargs.keys())):
            source_id = kwargs['source_id']
            business_rule_list = kwargs['business_rule_list'].split(',')

        else:
            return BUSINESS_RULE_EMPTY

        sql = "select report_id,sheet_id,cell_id,cell_business_rules,in_use from report_calc_def where source_id=" + str(
                source_id)
        cur=self.db.query(sql)
        data_list = cur.fetchall()

        result_set = []
        for data in data_list:
            # app.logger.info(data['cell_business_rules'])
            cell_rule_list = data['cell_business_rules'].split(',')
            # app.logger.info(type(cell_rule_list))
            if set(business_rule_list).issubset(set(cell_rule_list)):
                # app.logger.info(data)
                result_set.append(data)

        return result_set

    def get_source_suggestion_list(self,source_id='ALL'):

        data_dict={}
        where_clause = ''

        sql = "select source_id, source_table_name " + \
                " from data_source_information " + \
                        " where 1 "
        country_suggestion = self.db.query(sql).fetchall()
        if source_id is not None and source_id !='ALL':
            where_clause =  " and source_id = " + source_id

        source = self.db.query(sql + where_clause).fetchall()


        data_dict['source_suggestion'] = source

        if not data_dict:
            return {"msg":"No report matched found"},404
        else:
            return data_dict


    def get_source_column_suggestion_list(self,table_name):
        data_dict = self.db.query("describe " + table_name).fetchall()

        #Now build the agg column list
        return data_dict

    def validate_python_expression(self,expr_obj):
        py_expr_val=expr_obj['expr']
        py_attr=expr_obj['attr']
        py_sample=expr_obj['sample']
        app.logger.info(py_attr)

        if not py_attr and not py_sample:
            status='INVALID'
            msg="Expression can't be validated"
        else:
            # Sampling option can also be extended for testing
            #input would be a business_date and no of records for Sampling
            #We need to loop through the set and publish the result array for
            #the sampling set e.g. [{attr:{<id:,business_date:,..>},msg:,status:}]
            #on error no need to continue with the sample, just break and display result
            py_items=[]
            if py_attr:
                py_items.append({"attr":py_attr,"msg":"","status":""})
            if py_sample:
                table_name = py_sample['table_name']
                business_date = py_sample['business_date']
                columns = py_sample['columns']
                sample_size = py_sample['sample_size']
                self.db=DatabaseHelper()
                sample_data = self.db.query("select " + columns + " from " + table_name + " where business_date=" + str(business_date) + " and " + columns.replace(","," is not null and ")+ " is not null LIMIT 0," + str(sample_size)).fetchall()
                for data in sample_data:
                    py_items.append({"attr":data,"msg":"","status":""})

            for py_item in py_items:
                app.logger.info(py_item)
                py_expr = py_expr_val
                for attr in py_item['attr'].keys():
                    py_expr=py_expr.replace("["+attr+"]","'" + str(py_item['attr'][attr]) + "'")
                try:
                    msg=[]
                    status='VALID'
                    app.logger.info(py_expr)
                    val=eval(py_expr.replace("DERIVED","'DERIVED'"))
                    msg.append('Executed expression gives a value of '+str(val))
                except:
                    status='INVALID'
                    msg=tb.format_exc().splitlines()[-3:]

                py_item["msg"] = msg
                py_item["status"] = status

        #app.logger.info(msg[-3:])

        return py_items
