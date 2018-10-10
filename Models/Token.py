from Helpers.DatabaseHelper import DatabaseHelper
from Models.UserPermission import UserPermission
import uuid
from datetime import datetime, timedelta
from jwt import JWT, jwk_from_pem
from flask import request
from Constants.Status import *
import json
from Helpers.utils import autheticateTenant

TokenKey = 'HTTP_AUTHORIZATION'

class Token(object):
	def __init__(self):
		self.domain_info=autheticateTenant()
		if self.domain_info:
			tenant_info = json.loads(self.domain_info)
			self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
			self.dbhelper = DatabaseHelper(self.tenant_info)
		if TokenKey in request.environ:
			self.token = request.environ[TokenKey]

	def create(self, user):
		user_id = user['name']
		firstname = user['first_name']
		role = user['role']
		tenant_id = user['tenant_id']
		try:
			self.tokenId = self.get(user_id)
		except ValueError:
			self.tokenId = str(uuid.uuid4())
			self.lease_start = datetime.now()
			self.lease_end = self.lease_start + timedelta(hours=24)
			self.ip = request.remote_addr
			self.user_agent = request.headers.get('User-Agent')
			self.user_id = user_id
			queryString = "INSERT INTO token(token, lease_start, lease_end, ip, user_agent, user_id) VALUES (%s,%s,%s,%s,%s,%s)"
			values = (self.tokenId, self.lease_start, self.lease_end, self.ip, self.user_agent, self.user_id, )
			try:
				rowid = self.dbhelper.transact(queryString, values)
				self.dbhelper.commit()
			except Exception:
				return NO_USER_FOUND
		user_permission = UserPermission(self.tenant_info).get(roleId=role,inUseCheck='Y',tenant_id=tenant_id, getDetails=False)
		if user_permission:
			user = {
				'tokenId': self.tokenId,
				'userId': user_id,
				'name': firstname,
				'role': user_permission['role'],
				'permission': user_permission['components'],
				'source': user_permission['sources'],
				'report': user_permission['reports'],
				'domainInfo': self.domain_info
			}
			jwtObject = JWT()
			with open('private_key', 'rb') as fh:
				salt = jwk_from_pem(fh.read())
			jwt = jwtObject.encode(user, salt, 'RS256')
			return jwt
		else:
			return { "msg": "Permission Denied" },401

	def get(self, userId):
		queryString = "SELECT token FROM token WHERE user_id=%s and lease_end > %s"
		queryParams = (userId, datetime.now(), )
		cur = self.dbhelper.query(queryString, queryParams)
		data = cur.fetchone()
		if data:
			return data['token']
		else:
			raise ValueError("User not logged in!")

	def authenticate(self):
		token = self.token
		if token:
			extracted_token = token.replace("Bearer ", "")
			with open('public_key', 'r') as fh:
				salt = jwk_from_pem(fh.read().encode())
			try:
				token_decode = JWT().decode(extracted_token, salt)
				# print(token_decode)
			except Exception:
				raise TypeError("Invalid Token Recieved for Authentication")
			# Check whether tokenId, UserId as well as domainInfo exists, then check for logged in user
			# Authentication else check whether only domainInfo present (as may be after domain validation,
			# but before login, during signup of new user request etc)
			if 'tokenId' in token_decode.keys() and 'userId' in token_decode.keys():
				queryString = "SELECT user_id FROM token WHERE token=%s AND user_id=%s AND lease_end > %s"
				queryParams = (token_decode['tokenId'], token_decode['userId'], datetime.now(), )
				cur = self.dbhelper.query(queryString, queryParams)
				data = cur.fetchone()
				if data:
					return data['user_id']
				else:
					raise ValueError("Invalid Credentials Recieved for Authentication")
			elif [*token_decode.keys()] == ['domainInfo']:
				return 'domainInfo'
			else:
				raise ValueError("Invalid Credentials Recieved for Authentication")
		else:
			raise ValueError("Token Not Found for Authentication")
