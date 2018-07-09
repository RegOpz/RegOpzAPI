from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.authenticate import *
from Constants.Status import *
from Models.RegOpzUser import RegOpzUser
import json

TokenKey = 'HTTP_AUTHORIZATION'

class UserController(Resource):
	def __init__(self):
		self.regopzUser=RegOpzUser()

	@authenticate
	def get(self, userId = None):
		userCheck = request.args.get('userCheck')
		labellist = request.args.get('labelList')
		if not userCheck and TokenKey in request.environ:
			# auth = request.environ[TokenKey]
			# authenticate
			return self.regopzUser.get(userId,labellist)
		return self.regopzUser.getUserList(userId)

	def post(self):
		auth = request.authorization
		if auth:
			return self.regopzUser.login(auth['username'], auth['password'])
		user = request.get_json(force=True)
		regOpzUser = RegOpzUser(user)
		res = regOpzUser.save()
		return res

	def put(self):
		data = request.get_json(force=True)
		return self.regopzUser.update(data)

	def delete(self, userId):
		return self.regopzUser.changeStatus(userId)
