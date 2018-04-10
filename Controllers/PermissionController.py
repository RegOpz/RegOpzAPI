from Helpers.DatabaseHelper import DatabaseHelper
from flask_restful import Resource,request
import json
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *

class PermissionController(Resource):
    def __init__(self):
        self.domain_info = autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.dbhelper=DatabaseHelper(self.tenant_info)

    @authenticate
    def get(self):
        permissions_list = []
        queryString = "SELECT * FROM components"
        components = self.dbhelper.query(queryString).fetchall()
        for component in components:
            queryString = "SELECT id AS permission_id, permission FROM permission_def \
                     WHERE component_id = %s"
            queryParams = (component['id'],)
            permissions = self.dbhelper.query(queryString, queryParams).fetchall()
            data = {
                'component':component['component'],
                'permissions':permissions
            }
            permissions_list.append(data)
        return permissions_list
