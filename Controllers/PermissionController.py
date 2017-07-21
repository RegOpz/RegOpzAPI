from Helpers.DatabaseHelper import DatabaseHelper
from flask_restful import Resource,request

class PermissionController(Resource):
    def get(self):
        dbhelper = DatabaseHelper()
        permissions_list = []
        queryString = "SELECT * FROM components"
        components = dbhelper.query(queryString).fetchall()
        for component in components:
            queryString = "SELECT id AS permission_id, permission FROM permission_def \
                     WHERE component_id = %s"
            queryParams = (component['id'],)
            permissions = dbhelper.query(queryString, queryParams).fetchall()
            data = {
                'component':component['component'],
                'permissions':permissions
            }
            permissions_list.append(data)
        return permissions_list
