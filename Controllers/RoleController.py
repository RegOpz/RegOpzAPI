from flask_restful import Resource,request
from Models.UserPermission import UserPermission

class RoleController(Resource):
    def get(self, role = None):
        return UserPermission().get(role)

    def post(self):
        res = request.get_json(force=True)
        return UserPermission().post(res)

    def delete(self, role = None):
        return UserPermission().delete(role)
