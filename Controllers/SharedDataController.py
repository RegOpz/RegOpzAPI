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
        if request.endpoint == 'get_components_ep':
            component = request.args.get('component')
            return self.fetch_components(component)
    def post(self):
        if request.endpoint == 'test_connection_ep':
            connection = request.get_json(force=True)
            return self.test_connection(connection);

    def fetch_countries(self,country=None):
        app.logger.info("Fetching countrie(s) {}".format((country if country else "ALL"),))
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

    def fetch_components(self,component=None):
        app.logger.info("Fetching component(s) {}".format((component if component else "ALL"),))
        try:
            sql = "select * from components"
            sqlparams=""
            if component:
                sql += " where component=%s"
                sqlparams = (component,)

            components=self.master_db.query(sql,sqlparams).fetchall()
            return components
        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500

    def test_connection(self,connection):
        app.logger.info("Tesing connection for {}".format((connection if connection else "No connection details!!!"),))
        try:
            if not connection:
                return {"msg": "No connection details provided, Unable test connection!!!"}, 400
            connection_db=DatabaseHelper(connection)
            sql = "select 1"
            res =connection_db.query(sql).fetchall()
            if res:
                return {"msg": "Successfully tested connection setup."}, 200
            else:
                return {"msg": "Connection test failure! Please check!!!"}, 400
        except Exception as e:
            app.logger.error(str(e))
            return {"msg":"Connection test failure! Please check!!!" + str(e)},500
