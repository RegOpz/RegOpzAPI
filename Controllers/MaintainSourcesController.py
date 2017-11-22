from app import *
from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
import csv
import time
from Constants.Status import *
import mysql.connector
from mysql.connector import errorcode


class MaintainSourcesController(Resource):
	def __init__(self):
		self.db=DatabaseHelper()

	def get(self):
		app.logger.info(request.endpoint)
		if request.endpoint == 'get_source_feed_suggestion_list_ep':
			source_table_name = request.args.get('source_table_name')
			country = request.args.get('country')
			return self.get_source_feed_suggestion_list(source_table_name=source_table_name,country=country)
		if request.endpoint == 'get_sourcetable_column_suggestion_list_ep':
			table_name = request.args.get('table_name')
			return self.get_sourcetable_column_suggestion_list(table_name=table_name)


	def post(self):
		data = request.get_json(force=True)
		app.logger.info("Data in post call {}".format(data,))
		res = self.insert_data(data)
		return res

	def put(self, id=None):
		if id == None:
			return BUSINESS_RULE_EMPTY
		data = request.get_json(force=True)
		app.logger.info("Data in put call {}".format(data,))
		res = self.update_data(data, id)
		return res

	def insert_data(self,data):
		app.logger.info("Inserting data")
		try:
			create_table_name=data['update_info']['source_table_name']
			columns=data['added_fields']
			self.create_table(create_table_name,columns)
			self.insert_columns(create_table_name,columns)
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
			app.logger.info("Insering data into table")
			res=self.db.transact(sql,params_tuple)
			self.db.commit()
			data['update_info']=self.ret_source_data_by_id(table_name,res)
			data['update_info']['source_id'] = data['update_info']['id']

			return self.update_data(data, res)
		except Exception as e:
			app.logger.error(e)
			return {"msg":e},500

	def update_data(self,data,id):
		app.logger.info("Updating data")

		try:
			alter_table_name=data['update_info']['source_table_name']
			columns=data['added_fields']
			self.modify_table(alter_table_name,columns)
			self.insert_columns(alter_table_name,columns)
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

			app.logger.info("Updating data on table")
			res=self.db.transact(sql,params_tuple)

			if res==0:
				app.logger.info("Comitting updated data")
				self.db.commit()
				return self.ret_source_data_by_id(table_name,id)

			app.logger.info("Rolling back updated data")
			self.db.rollback()
			return UPDATE_ERROR
		except Exception as e:
			app.logger.error(e)
			return {"msg":e}, 500

	def ret_source_data_by_id(self,table_name,id):

		app.logger.info("Getting source data by id")

		try:
			query = "select * from {} where id = %s".format(table_name)
			app.logger.info("Gettind data from table {0} for id {1}".format(table_name, id))
			data = self.db.query(query, (id,)).fetchone()
			if data:
				return data
			return NO_BUSINESS_RULE_FOUND
		except Exception as e:
			app.logger.error(e)
			return {"msg": e}, 500


	def get_source_feed_suggestion_list(self,source_table_name='ALL',country='ALL'):
		app.logger.info("Getting source feed suggestion list")
		try:
			data_dict = {}
			where_clause = ''

			sql = "select distinct country from data_source_information where 1 "
			app.logger.info("Getting country suggestion list")
			country_suggestion = self.db.query(sql).fetchall()
			if country is not None and country != 'ALL':
				where_clause = " and instr('" + country.upper() + "', upper(country)) > 0"
			if source_table_name is not None and source_table_name != 'ALL':
				where_clause += " and instr('" + source_table_name.upper() + "', upper(source_table_name)) > 0"

			app.logger.info("Getting country list")
			country = self.db.query(sql + where_clause).fetchall()
			data_dict['country'] = country

			sql = "select distinct source_table_name from data_source_information"
			app.logger.info("Getting source suggestion list")
			source_suggestion = self.db.query(sql).fetchall()

			for i, c in enumerate(data_dict['country']):
				sql = "select * from data_source_information where country = '" + c['country'] + "'"
				if source_table_name is not None and source_table_name != 'ALL':
					where_clause = " and instr('" + source_table_name.upper() + "', upper(source_table_name)) > 0"
				source = self.db.query(sql + where_clause).fetchall()
				# print(data_dict['country'][i])
				data_dict['country'][i]['source'] = source
			# print(data_dict)
			data_dict['source_suggestion'] = source_suggestion
			data_dict['country_suggestion'] = country_suggestion

			if not data_dict:
				return {"msg": "No report matched found"}, 404
			else:
				return data_dict

		except Exception as e:
			app.logger.error(e)
			return {"msg": e}, 500

	def get_sourcetable_column_suggestion_list(self,table_name):
		app.logger.info("Getting source table column suggestion list")

		try:
			data_dict = [{"Field":"","Type":"","Null":"","Key":"","Default":"","Extra":""}]
			if table_name is None or table_name == 'undefined' or table_name == 'null':
				return data_dict
			app.logger.info("Describing table {}".format(table_name))
			sql="select column_name as Field,column_datatype as 'Type',column_default_value as 'Default',\
			column_is_nullable as 'Null',column_key as 'Key',column_display_name as 'Extra' \
			from data_source_cols where source_table_name= %s"
			data_dict = self.db.query(sql,(table_name,)).fetchall()
			data_dict_database = self.db.query("describe " + table_name).fetchall()
			return data_dict
		except mysql.connector.Error as err:
			if err.errno == errorcode.ER_BAD_TABLE_ERROR:
				app.logger.error(err)
				return data_dict
			else:
				app.logger.error(err)
				return {"msg": err}, 500
		except Exception as e:
			app.logger.error(e)
			return {"msg":e},500


	def create_table(self,table_name,columns):
		app.logger.info("Create table for new source set up")

		sql = 'create table ' + table_name + '( '
		for col in columns:
			not_null='not null' if col['Null']=='NO' else ''
			sql += col['Field'] + ' ' + col['Type'] +' '+not_null+ ','
		sql = sql[:-1] + ' )'
		app.logger.info(sql)
		self.db.transact(sql)

	def modify_table(self,table_name,columns):
		app.logger.info("Modify table for existing source ")

		sql='alter table '+table_name + ' add( '
		for col in columns:
			not_null = 'not null' if col['Null'] == 'NO' else ''
			sql+=col['Field'] +' ' + col['Type']+' '+not_null+','
		sql=sql[:-1]+' )'
		self.db.transact(sql)

	def insert_columns(self,table_name,columns):
		app.logger.info("Insert columns for source")

		for col in columns:
			sql='insert into data_source_cols(source_table_name,column_name,column_datatype,column_default_value,\
			column_is_nullable,column_key,column_display_name) values(%s,%s,%s,%s,%s,%s,%s)'
			params=(table_name,col['Field'],col['Type'],col['Default'],col['Null'],col['Key'],'')

			self.db.transact(sql,params)

		self.db.commit()
