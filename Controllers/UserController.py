from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.authenticate import *
from Constants.Status import *
from Models.RegOpzUser import RegOpzUser
import json

TokenKey = 'HTTP_AUTHORIZATION'

class UserController(Resource):
	def __init__(self):
		tenant_info=json.loads(request.headers.get('Tenant'))
		self.tenant_conn_details=json.loads(tenant_info['tenant_conn_details'])
		# print(self.tenant_conn_details)
		if self.tenant_conn_details:
			self.regopzUser=RegOpzUser(self.tenant_conn_details)

	@authenticate
	def get(self, userId = None):
		if TokenKey in request.environ:
			auth = request.environ[TokenKey]
			# authenticate
			return self.regopzUser.get(userId)
		return self.regopzUser.getUserList(userId)

	def post(self):
		auth = request.authorization
		if auth:
			return self.regopzUser.login(auth['username'], auth['password'])
		user = request.get_json(force=True)
		regOpzUser = RegOpzUser(self.tenant_conn_details,user)
		res = regOpzUser.save()
		return res

	def put(self):
		data = request.get_json(force=True)
		return self.regopzUser.update(data)

	def delete(self, userId):
		return self.regopzUser.changeStatus(userId)
