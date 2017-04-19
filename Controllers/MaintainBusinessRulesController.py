from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
import csv
from Constants.Status import *


class MaintainBusinessRulesController(Resource):

	def get(self, id=None, page=0, business_rule=None):
		if request.endpoint == "business_rule_linkage_ep":
			return self.list_reports_for_rule(business_rule=business_rule)
		if id:
			return self.render_business_rule_json(id)
		elif page:
			return self.render_business_rules_json(page)

	def post(self,page=None):
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

	def render_business_rules_json(self, page):
		db = DatabaseHelper()
		startPage =  int(page)*100
		business_rules_dict = {}
		business_rules_list = []
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
			return self.render_business_rule_json_by_id(res)
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
			return {"id":res}
		return UPDATE_ERROR

	def delete_business_rules(self, id):
		db = DatabaseHelper()
		sql = 'delete from business_rules where id=%s'
		params = (id, )
		res = db.transact(sql, params)
		return res

	def export_to_csv(self, source_id='ALL'):
		db = DatabaseHelper()

		sql = 'select * from business_rules where 1=1'

		if source_id != 'ALL':
			sql += " and source_id='" + str(source_id) + "'"

		cur = db.query(sql)

		business_rules = cur.fetchall()

		 # keys=business_rules[0].keys()

		keys = [i[0] for i in cur.description]

		with open('business_rules.csv', 'wt') as output_file:
			dict_writer = csv.DictWriter(output_file, keys)
			dict_writer.writeheader()
			dict_writer.writerows(business_rules)
	def list_reports_for_rule(self,**kwargs):

		parameter_list = ['business_rule']

		if set(parameter_list).issubset(set(kwargs.keys())):
			business_rule = kwargs['business_rule']

		else:
			return BUSINESS_RULE_EMPTY

		db = DatabaseHelper()
		sql = "select distinct report_id,sheet_id,cell_id from report_calc_def \
		where cell_business_rules like '%" + business_rule + "%'"
		cur=db.query(sql)
		report_list = cur.fetchall()

		return [(rpt['report_id'], rpt['sheet_id'], rpt['cell_id']) for rpt in report_list]