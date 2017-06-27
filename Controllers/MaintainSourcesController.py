from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
import csv
import time
from Constants.Status import *


class MaintainSourcesController(Resource):
	def get(self):
		print(request.endpoint)
		if request.endpoint == 'get_source_feed_suggestion_list_ep':
			source_table_name = request.args.get('source_table_name')
			country = request.args.get('country')
			return self.get_source_feed_suggestion_list(source_table_name=source_table_name,country=country)
		if request.endpoint == 'get_sourcetable_column_suggestion_list_ep':
			table_name = request.args.get('table_name')
			return self.get_sourcetable_column_suggestion_list(table_name=table_name)


	def post(self):
		data = request.get_json(force=True)
		res = self.insert_data(data)
		return res

	def put(self, id=None):
		if id == None:
			return BUSINESS_RULE_EMPTY
		data = request.get_json(force=True)
		res = self.update_data(data, id)
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
	    res=db.transact(sql,params_tuple)
	    db.commit()
	    data['update_info']=self.ret_source_data_by_id(table_name,res)
	    data['update_info']['source_id'] = data['update_info']['id']

	    return self.update_data(data, res)

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
	    if data:
	        return data
	    return NO_BUSINESS_RULE_FOUND

	def get_source_feed_suggestion_list(self,source_table_name='ALL',country='ALL'):

		db=DatabaseHelper()
		data_dict={}
		where_clause = ''

		sql = "select distinct country from data_source_information where 1 "
		country_suggestion = db.query(sql).fetchall()
		if country is not None and country !='ALL':
			 where_clause =  " and instr('" + country.upper() + "', upper(country)) > 0"
		if source_table_name is not None and source_table_name !='ALL':
			 where_clause +=  " and instr('" + source_table_name.upper() + "', upper(source_table_name)) > 0"

		country = db.query(sql + where_clause).fetchall()
		data_dict['country'] = country

		sql = "select distinct source_table_name from data_source_information"
		source_suggestion = db.query(sql).fetchall()
		data_dict['country'] = country
		for i,c in enumerate(data_dict['country']):
			sql = "select * from data_source_information where country = '" + c['country'] + "'"
			if source_table_name is not None and source_table_name !='ALL':
				 where_clause =  " and instr('" + source_table_name.upper() + "', upper(source_table_name)) > 0"
			source = db.query(sql + where_clause).fetchall()
			print(data_dict['country'][i])
			data_dict['country'][i]['source'] = source
			print(data_dict)
		data_dict['source_suggestion'] = source_suggestion
		data_dict['country_suggestion'] = country_suggestion

		if not data_dict:
			return {"msg":"No report matched found"},404
		else:
			return data_dict


	def get_sourcetable_column_suggestion_list(self,table_name):
		db=DatabaseHelper()
		data_dict = [{"Field":"","Type":"","Null":"","Key":"","Default":"","Extra":""}]
		if table_name is None or table_name == 'undefined':
			return data_dict
		try:
			#to handle non existance table_name
			data_dict = db.query("describe " + table_name).fetchall()

		finally:
			#Now build the column list
			return data_dict
