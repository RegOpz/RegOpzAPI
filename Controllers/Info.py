from flask_restful import Resource
from flask import Flask, request
from Configs import APIConfig
import platform
from Helpers.authenticate import *
class Info(Resource):
	@authenticate
	def get(self):
		return {
			'context':"RegOpz REST API",
			'author': "RegOpz Team",
			'organization': "RegOpz Pty Ltd",
			'os': platform.linux_distribution(),
			'kernel': platform.system() + " " + platform.release(),
			'version': APIConfig.API['version'],
			'ip_address': request.remote_addr
	}
