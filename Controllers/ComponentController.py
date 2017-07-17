from Helpers.DatabaseHelper import DatabaseHelper
from flask_restful import Resource,request

class ComponentController(Resource):
    def get(self):
        dbhelper = DatabaseHelper()
        query = "SELECT component FROM components"
        components = dbhelper.query(query).fetchall()
        return components
