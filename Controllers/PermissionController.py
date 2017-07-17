from Helpers.DatabaseHelper import DatabaseHelper
from flask_restful import Resource,request

class PermissionController(Resource):
    def get(self):
        dbhelper = DatabaseHelper()
        query = "SELECT permission FROM permission_def"
        permissions = dbhelper.query(query).fetchall()
        return permissions
