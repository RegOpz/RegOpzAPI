from app import *
from Helpers.DatabaseHelper import DatabaseHelper
from Models.UserPermission import UserPermission
import uuid
from datetime import datetime, timedelta
from jwt import JWT, jwk_from_pem
from flask import request
from Constants.Status import *

TokenKey = 'HTTP_AUTHORIZATION'

class Token(object):
	def __init__(self):
		self.dbhelper = DatabaseHelper()
		if TokenKey in request.environ:
			self.token = request.environ[TokenKey]

	def create(self, user):
		user_id = user['name']
		firstname = user['first_name']
		role = user['role']
		try:
			app.logger.info("I: Models: Token: Create: Getting existing Token for the User")
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
				app.logger.info("I: Models: Token: Create: Generating new Token for the User")
				rowid = self.dbhelper.transact(queryString, values)
			except Exception as e:
				app.logger.error("E: Models: Token: Create:", e)
				return NO_USER_FOUND
		app.logger.info("I: Models: Token: Create: Generating Permissions for the User")
		user_permission = UserPermission().get(role)
		if user_permission:
			user = {
				'tokenId': self.tokenId,
				'userId': user_id,
				'name': firstname,
				'role': user_permission['role'],
				'permission': user_permission['components']
			}
			jwtObject = JWT()
			with open('private_key', 'rb') as fh:
				salt = jwk_from_pem(fh.read())
			jwt = jwtObject.encode(user, salt, 'RS256')
			return jwt
		else:
			return { "msg": "Permission Denied" },301

	def get(self, userId):
		queryString = "SELECT token FROM token WHERE user_id=%s and lease_end > %s"
		queryParams = (userId, datetime.now(), )
		app.logger.info("I: Models: Token: Get: Querying Token for the User in DB")
		cur = self.dbhelper.query(queryString, queryParams)
		data = cur.fetchone()
		if data:
			return data['token']
		else:
			raise ValueError("User not logged in!")

	def authenticate(self, auth=None):
		token = auth if auth else self.token
		if token:
			extracted_token = token.replace("Bearer ", "")
			with open('public_key', 'r') as fh:
				salt = jwk_from_pem(fh.read().encode())
			try:
				app.logger.info("I: Models: Token: Authenticate: Decoding Token for the User")
				token_decode = JWT().decode(extracted_token, salt)
			except Exception as e:
				app.logger.error("E: Models: Token: Authenticate:", e)
				raise TypeError("Invalid Token Recieved for Authentication")
			app.logger.info("I: Models: Token: Authenticate: Querying and Validating Token for the User")
			queryString = "SELECT user_id FROM token WHERE token=%s AND user_id=%s AND lease_end > %s"
			queryParams = (token_decode['tokenId'], token_decode['userId'], datetime.now(), )
			cur = self.dbhelper.query(queryString, queryParams)
			data = cur.fetchone()
			if data:
				app.logger.info("I: Models: Token: New login from user", data['user_id'])
				return data['user_id']
			else:
				err = "Invalid Credentials Recieved for Authentication"
				app.logger.error("E: Models: Token: Authenticate:", err)
				return { "msg": err },301
		else:
			err = "Token Not Found for Authentication"
			app.logger.error("E: Models: Token: Authenticate:", err)
			return { "msg": err },301
