from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
import csv
import time
from Constants.Status import *


class MaintainBusinessRulesController(Resource):
	def get(self, id=None, page=0, col_name=None,business_rule=None):
		print(request.endpoint)
		if request.endpoint == "business_rule_export_to_csv_ep":
			return self.export_to_csv()
		if request.endpoint == "business_rule_linkage_ep":
			return self.list_reports_for_rule(business_rule=business_rule)
		if request.endpoint == "business_rule_drill_down_rules_ep":
			print('Inside rules drilldown ep')
			source = request.args.get('source_id')
			rules = request.args.get('rules')
			page = request.args.get('page')
			return self.get_business_rules_list_by_source(rules=rules,source=source,page=page)
		if id:
			return self.render_business_rule_json(id)
		elif page and col_name == None:
			return self.render_business_rules_json(page)
		elif col_name:
			direction = request.args.get('direction')
			return self.render_business_rules_json(page, (col_name,direction))
	def post(self,page=None):

		if request.endpoint == "business_rule_linkage_multiple_ep":
			params=request.get_json(force=True)
			print(params)
			return self.list_reports_for_rule_list(source_id=params['source_id'],business_rule_list=params['business_rule_list'])

		if request.endpoint == "business_rules_ep_filtered":
			filter_conditions = request.get_json(force=True)
			return self.get_business_rules_filtered(filter_conditions)
		br = request.get_json(force=True)
		res = self.insert_business_rules(br)
		return res

	def put(self, id=None):
		if id == None:
			return BUSINESS_RULE_EMPTY
		br = request.get_json(force=True)
		res = self.update_business_rules(br, id)
		return res

	def delete(self, id=None):
		if id == None:
			return BUSINESS_RULE_EMPTY
		res = self.delete_business_rules(id)
		return res
	def render_business_rules_json(self, page=0, order=None):
		db = DatabaseHelper()
		startPage =  int(page)*100
		business_rules_dict = {}
		business_rules_list = []
		if order:
			cur = db.query('select * from business_rules ' + 'order by ' + order[0] + ' ' + order[1] + ' limit ' + str(startPage) + ', 100')
		else:
			cur = db.query('select * from business_rules limit ' + str(startPage) + ', 100')
		business_rules = cur.fetchall()
		cols = [i[0] for i in cur.description]
		business_rules_dict['cols'] = cols

		for br in business_rules:
			business_rules_list.append(br)
		business_rules_dict['rows'] = business_rules_list
		count = db.query('select count(*) as count from business_rules').fetchone()
		business_rules_dict['count'] = count['count']
		json_dump = business_rules_dict
		return json_dump
	def get_business_rules_filtered(self,filter_conditions):
		filter_string = ""
		for filter_condition in filter_conditions:
			filter_string = filter_string + " AND " + filter_condition['field_name'] + "='" + filter_condition['value'] + "'"
		db = DatabaseHelper()
		query = 'select * from business_rules where 1' + filter_string
		cur = db.query(query)
		data = cur.fetchall()
		if data:
			return data
		return NO_BUSINESS_RULE_FOUND
	def get_business_rules_list_by_source(self,rules,source,page):
		db = DatabaseHelper()

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
		cur = db.query(sql + where_clause + " limit " + str(startPage) + ", 100")
		data = cur.fetchall()
		cols = [i[0] for i in cur.description]
		count = db.query(sql.replace('*','count(*) as count ')).fetchone()
		data_dict['cols'] = cols
		data_dict['rows'] = data
		data_dict['count'] = count['count']
		data_dict['table_name'] = 'business_rules'
		data_dict['sql'] = sql

		return data_dict

	def render_business_rule_json(self, id):
		db = DatabaseHelper()
		query = 'select * from business_rules where id = %s'
		cur = db.query(query, (id, ))
		data = cur.fetchone()
		if data:
			return data
		return NO_BUSINESS_RULE_FOUND
	def render_business_rule_json_by_id(self, id):
		db = DatabaseHelper()
		query = 'select * from business_rules where id = %s'
		cur = db.query(query, (id, ))
		data = cur.fetchone()
		if data:
			return data
		return NO_BUSINESS_RULE_FOUND

	def insert_business_rules(self, br):
		db = DatabaseHelper()
		sql = \
			"insert into business_rules(rule_execution_order,business_rule,source_id,rule_description,\
						logical_condition,data_fields_list,python_implementation,business_or_validation,\
					rule_type,valid_from,valid_to) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

		if bool(br):
			params = tuple((
				br['rule_execution_order'],
				br['business_rule'],
				br['source_id'],
				br['rule_description'],
				br['logical_condition'],
				br['data_fields_list'],
				br['python_implementation'],
				br['business_or_validation'],
				br['rule_type'],
				br['valid_from'],
				br['valid_to'],
				))
		else:
			params = tuple((
				0,
				'null',
				'null',
				'null',
				'null',
				'null',
				'null',
				'null',
				'null',
				'null',
				'null',
				))

		res = db.transact(sql, params)
		if res:
			db.commit()
			return self.render_business_rule_json_by_id(res)

		db.rollback()
		return UPDATE_ERROR

	def update_business_rules(self, br, id):
		db = DatabaseHelper()
		sql = \
			'Update business_rules set rule_execution_order=%s,              business_rule=%s,             source_id=%s,             rule_description=%s,             logical_condition=%s,             data_fields_list=%s,             python_implementation=%s,             business_or_validation=%s,             rule_type=%s,             valid_from=%s,             valid_to=%s where id=%s'

		params = (
			br['rule_execution_order'],
			br['business_rule'],
			br['source_id'],
			br['rule_description'],
			br['logical_condition'],
			br['data_fields_list'],
			br['python_implementation'],
			br['business_or_validation'],
			br['rule_type'],
			br['valid_from'],
			br['valid_to'],
			id,
			)

		res = db.transact(sql, params)
		if res == 0:
			db.commit()
			return {"id":res}

		db.rollback()
		return UPDATE_ERROR

	def delete_business_rules(self, id):
		db = DatabaseHelper()
		sql = 'delete from business_rules where id=%s'
		params = (id, )
		res = db.transact(sql, params)
		if res == 0:
			db.commit()
			return res

		db.rollback()
		return DATABASE_ERROR

	def export_to_csv(self, source_id='ALL'):
		db = DatabaseHelper()

		sql = 'select * from business_rules where 1=1'

		if source_id != 'ALL':
			sql += " and source_id='" + str(source_id) + "'"

		cur = db.query(sql)

		business_rules = cur.fetchall()

		 # keys=business_rules[0].keys()

		keys = [i[0] for i in cur.description]
		filename="business_rules"+str(time.time())+".csv"

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

		db = DatabaseHelper()
		sql = "select distinct report_id,sheet_id,cell_id from report_calc_def \
		where cell_business_rules like '%," + business_rule + ",%'"
		cur=db.query(sql)
		report_list = cur.fetchall()

		return [(rpt['report_id'], rpt['sheet_id'], rpt['cell_id']) for rpt in report_list]

	def list_reports_for_rule_list(self,**kwargs):

		parameter_list = ['source_id', 'business_rule_list']

		if set(parameter_list).issubset(set(kwargs.keys())):
			source_id = kwargs['source_id']
			business_rule_list = kwargs['business_rule_list']

		else:
			return BUSINESS_RULE_EMPTY

		db=DatabaseHelper()

		sql = "select report_id,sheet_id,cell_id,cell_business_rules from report_calc_def where source_id=" + str(
			source_id)
		cur=db.query(sql)
		data_list = cur.fetchall()

		result_set = []
		for data in data_list:
			# print(data['cell_business_rules'])
			cell_rule_list = data['cell_business_rules'].split(',')
			# print(type(cell_rule_list))
			if set(business_rule_list).issubset(set(cell_rule_list)):
				# print(data)
				result_set.append(data)

		return result_set
