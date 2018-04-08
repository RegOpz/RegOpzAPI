from Helpers.DatabaseHelper import DatabaseHelper
from flask_restful import Resource,request
import json

class PermissionController(Resource):
    def __init__(self):
        tenant_info = json.loads(request.headers.get('Tenant'))
        self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
        self.dbhelper=DatabaseHelper(self.tenant_info)
        
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
