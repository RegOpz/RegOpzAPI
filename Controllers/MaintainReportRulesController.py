from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.DatabaseOps import DatabaseOps
import csv
import time
from datetime import datetime
from Constants.Status import *


class MaintainReportRulesController(Resource):
	def __init__(self):
		self.dbOps=DatabaseOps('def_change_log')

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
		res = self.dbOps.insert_data(data)
		return res

	def put(self, id=None):
		if id == None:
			return BUSINESS_RULE_EMPTY
		data = request.get_json(force=True)
		if request.endpoint == "report_rule_ep":
			res = self.dbOps.update_or_delete_data(data, id)
			return res

	# def delete(self, id=None):
	# 	if id == None:
	# 		return BUSINESS_RULE_EMPTY
	# 	if request.endpoint == "report_rule_ep":
	# 		table_name = request.args.get("table_name")
	# 		data=request.get_json(force=True)
	# 		#comment=request.headers["comment"]
	# 		print("++++++++++++++")
	# 		print(data)
	# 		print("++++++++++++++")
    #
	# 		res = self.dbOps.delete_data(table_name,id)
	# 		return res

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
