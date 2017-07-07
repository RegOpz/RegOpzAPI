from Helpers.DatabaseHelper import DatabaseHelper
# from Models.UserPermission import UserPermission
import uuid
from datetime import datetime, timedelta
from flask import request
from Constants.Status import *

class Token(object):
	def __init__(self):
		pass

	def create(self, user_id):
	# def create(self, userId, name)
		self.tokenId = str(uuid.uuid4())
		self.lease_start = datetime.now()
		self.lease_end = datetime.now() + timedelta(hours=24)
		self.ip = request.remote_addr
		self.user_agent = request.headers.get('User-Agent')
		self.user_id = user_id
		dbhelper = DatabaseHelper()
		queryString = "INSERT INTO token(id,token,lease_start,lease_end,ip,user_agent,user_id) VALUES (%s,%s,%s,%s,%s,%s,%s)"
		values = (None,self.tokenId,self.lease_start,self.lease_end,self.ip,self.user_agent,self.user_id)
		try:
			rowid = dbhelper.transact(queryString,values)
			self.get(rowid)
			# Get role and permission
			# user_permission = UserPermission().get(userId)
			# user['tokenId'] = self.tokenId
			# user['name'] = name
			# user['role'] = user_permission['role']
			# user['permission'] = user_permission['permission']
			# Encrypt before sending
			return self.tokenId
		except Exception as e:
			return NO_USER_FOUND

	def get(self, tokenId):
		dbhelper = DatabaseHelper()
		cur = dbhelper.query("SELECT * FROM token WHERE id=%s",(tokenId, ))
		data = cur.fetchone()
		self.token = data['token']
		self.lease_start = data['lease_start']
		self.lease_end = data['lease_end']
		self.ip = data['ip']
		self.user_agent = data['user_agent']
		self.user_id = data['user_id']

	def authenticate(self, token):
		extracted_token = token.replace("Bearer ","")
		# Decrypt here
		dbhelper = DatabaseHelper()
		cur = dbhelper.query("SELECT * FROM token WHERE token=%s AND lease_end > %s",(extracted_token,datetime.now()))
		data = cur.fetchone()
		if data:
			return data['user_id']
		raise ValueError('Invalid Credentials')
