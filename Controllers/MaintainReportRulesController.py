from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
import csv
import time
from datetime import datetime
from Constants.Status import *


class MaintainReportRulesController(Resource):
	def get(self):
		print(request.endpoint)
		if request.endpoint == 'get_business_rules_suggestion_list_ep':
			source_id = request.args.get('source_id')
			return self.get_business_rules_suggestion_list(source_id=source_id)
		if request.endpoint == 'get_source_suggestion_list_ep':
			source_id = request.args.get('source_id')
			return self.get_source_suggestion_list(source_id=source_id)
		if request.endpoint == 'get_cell_calc_ref_suggestion_list_ep':
			report_id = request.args.get('report_id')
			return self.get_cell_calc_ref_suggestion_list(report_id=report_id)
		if request.endpoint == 'get_agg_function_column_suggestion_list_ep':
			table_name = request.args.get('table_name')
			return self.get_agg_function_column_suggestion_list(table_name=table_name)


	def post(self):
		data = request.get_json(force=True)
		res = self.insert_data(data)
		return res

	def put(self, id=None):
		if id == None:
			return BUSINESS_RULE_EMPTY
		data = request.get_json(force=True)
		if request.endpoint == "report_rule_ep":
			res = self.update_data(data, id)
			return res

	def delete(self, id=None):
		if id == None:
			return BUSINESS_RULE_EMPTY
		if request.endpoint == "report_rule_ep":
			tableName = request.args.get("table_name")
			res = self.delete_data(tableName,id)
			return res

	def delete_data(self,table_name,id):
		db=DatabaseHelper()
		sql="delete from "+table_name +" where id=%s"
		print(sql)

		params=(id,)
		print(params)
		res=db.transact(sql,params)
		db.commit()

		return res


	def insert_data(self,data):

	    db = DatabaseHelper()

	    table_name = data['table_name']
	    update_info = data['update_info']
	    update_info_cols = update_info.keys()

	    sql="insert into "+table_name + "("
	    placeholders="("
	    params=[]

	    for col in update_info_cols:
	        sql+=col+","
	        placeholders+="%s,"
	        params.append(update_info[col])

	    placeholders=placeholders[:len(placeholders)-1]
	    placeholders+=")"
	    sql=sql[:len(sql)-1]
	    sql+=") values "+ placeholders

	    params_tuple=tuple(params)
	    print(sql)
	    print(params_tuple)
	    res=db.transact(sql,params_tuple)
	    db.commit()

	    return self.ret_source_data_by_id(table_name,res)

	def update_data(self,data,id):
	    db=DatabaseHelper()

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

	    print(sql)
	    print(params_tuple)

	    res=db.transact(sql,params_tuple)

	    if res==0:
	        db.commit()
	        return self.ret_source_data_by_id(table_name,id)

	    db.rollback()
	    return UPDATE_ERROR

	def ret_source_data_by_id(self, table_name,id):
	    db = DatabaseHelper()
	    query = 'select * from ' + table_name + ' where id = %s'
	    cur = db.query(query, (id, ))
	    data = cur.fetchone()
	    for k,v in data.items():
	    	if isinstance(v,datetime):
	    		data[k] = data[k].isoformat()
	    		print(data[k], type(data[k]))
	    if data:
	        return data
	    return NO_BUSINESS_RULE_FOUND

	def get_source_suggestion_list(self,source_id='ALL'):

		db=DatabaseHelper()
		data_dict={}
		where_clause = ''

		sql = "select source_id, source_table_name " + \
		        " from data_source_information " + \
				" where 1 "
		if source_id is not None and source_id !='ALL':
		     where_clause =  " and source_id = " + source_id

		source = db.query(sql + where_clause).fetchall()


		data_dict['source_suggestion'] = source

		if not data_dict:
		    return {"msg":"No report matched found"},404
		else:
		    return data_dict

	def get_business_rules_suggestion_list(self,source_id='ALL'):

		db=DatabaseHelper()
		data_dict={}
		where_clause = ''

		sql = "select source_id, source_table_name " + \
		        " from data_source_information " + \
				" where 1 "
		country_suggestion = db.query(sql).fetchall()
		if source_id is not None and source_id !='ALL':
		     where_clause =  " and source_id = " + str(source_id)

		source = db.query(sql + where_clause).fetchall()


		data_dict['source_suggestion'] = source
		for i,s in enumerate(data_dict['source_suggestion']):
		    sql = "select * from business_rules where source_id = '" + str(s['source_id']) + "'"
		    rules_suggestion = db.query(sql).fetchall()
		    data_dict['source_suggestion'][i]['rules_suggestion'] = rules_suggestion

		if not data_dict:
		    return {"msg":"No report matched found"},404
		else:
		    return data_dict

	def get_agg_function_column_suggestion_list(self,table_name):
		db=DatabaseHelper()
		data_dict_report_link = db.query("describe report_qualified_data_link").fetchall()
		data_dict = db.query("describe " + table_name).fetchall()

		#Now build the agg column list
		return data_dict + data_dict_report_link

	def get_cell_calc_ref_suggestion_list(self,report_id):

		db=DatabaseHelper()
		data_dict={}
		where_clause = ''

		sql = "select * " + \
		        " from report_calc_def " + \
				" where report_id = '" + report_id + "'"
		cell_calc_ref_list = db.query(sql).fetchall()
		data_dict['cell_calc_ref'] = cell_calc_ref_list

		if not data_dict:
		    return {"msg":"No report matched found"},404
		else:
		    return data_dict
