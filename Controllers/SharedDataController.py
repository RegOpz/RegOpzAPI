from app import *
from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *
import json

class SharedDataController(Resource):

    def __init__(self):
        self.master_db=DatabaseHelper()
        self.domain_info = autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.tenant_db=DatabaseHelper(self.tenant_info)



    def get(self):
        if request.endpoint == 'get_countries_ep':
            country = request.args.get('country')
            return self.fetch_countries(country)

    def fetch_countries(self,country=None):
        app.logger.info("Fetching countrie(s) for country {}".format((country if country else "ALL"),))
        try:
            sql = "select * from country"
            sqlparams=""
            if country:
                sql += " where country=%s"
                sqlparams = (country,)

            countries=self.master_db.query(sql,sqlparams).fetchall()
            return countries
        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500
