from Helpers.DatabaseHelper import DatabaseHelper
from Models.UserPermission import UserPermission
import uuid
from datetime import datetime, timedelta
from jwt import JWT, jwk_from_dict, jwk_from_pem
from flask import request
from Constants.Status import *

class Token(object):
	def __init__(self):
		pass

	def create(self, user_id):
		try:
			self.tokenId = self.get(user_id)
		except ValueError:
			self.tokenId = str(uuid.uuid4())
			self.lease_start = datetime.now()
			self.lease_end = datetime.now() + timedelta(hours=24)
			self.ip = request.remote_addr
			self.user_agent = request.headers.get('User-Agent')
			self.user_id = user_id
			queryString = "INSERT INTO token(token, lease_start, lease_end, ip, user_agent, user_id) VALUES (%s,%s,%s,%s,%s,%s)"
			values = (self.tokenId, self.lease_start, self.lease_end, self.ip, self.user_agent, self.user_id, )
			tokenId = DatabaseHelper().transact(queryString, values)
		user_permission = UserPermission().get(user_id)
		user = {
			'tokenId': tokenId,
			'role': user_permission['role'],
			'permission': user_permission['permission']
		}
		with open('private_key', 'rb') as fh:
			salt = jwk_from_pem(fh.read())
		jwt = JWT().encode(user, salt, 'RS512')
		return jwt

	def get(self, userId):
		queryString = "SELECT token FROM token WHERE user_id=%s and lease_end > %s"
		queryParams = (userId, datetime.now(), )
		cur = DatabaseHelper().query(queryString, queryParams)
		data = cur.fetchone()
		if data:
			return data['token']
		else:
			raise ValueError("User not logged in!")

	def authenticate(self, token):
		extracted_token = token.replace("Bearer ","")
		with open('public_key', 'r') as fh:
			salt = jwk_from_pem(fh.read())
		token_decode = JWT().decode(extracted_token, salt)
		queryString = "SELECT * FROM token WHERE token=%s AND lease_end > %s"
		queryParams = (token_decode.tokenId, datetime.now(), )
		cur = DatabaseHelper().query(queryString, queryParams)
		data = cur.fetchone()
		if data:
			return data['user_id']
		raise ValueError('Invalid Credentials')
