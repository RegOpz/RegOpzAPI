from flask_restful import Resource,request
from Models.UserPermission import UserPermission

class RoleController(Resource):
    def get(self):
        role = request.args.get('role')
        return UserPermission().get(role)

    def post(self):
        res = request.get_json(force=True)
        return UserPermission().save(res)

    def delete(self, role = None):
        return UserPermission().remove(role)
