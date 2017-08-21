from flask_restful import Resource,request
from Models.UserPermission import UserPermission
from Models.Token import Token

class RoleController(Resource):
    def get(self, role = None):
        return UserPermission().get(role)

    def post(self):
        userId = Token().authenticate()
        res = request.get_json(force=True)
        return UserPermission(userId).post(res)

    def delete(self, role = None):
        userId = Token().authenticate()
        return UserPermission(userId).delete(role)
