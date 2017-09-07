from flask_restful import Resource,request
from Models.UserPermission import UserPermission
from Models.Token import Token

class RoleController(Resource):
    def get(self, role = None):
        inUseCheck = request.args.get('inUseCheck') if request.args.get('inUseCheck') else 'Y'
        return UserPermission().get(role, inUseCheck)

    def post(self):
        userId = Token().authenticate()
        res = request.get_json(force=True)
        return UserPermission(userId).post(res)

    def delete(self, role = None):
        userId = Token().authenticate()
        comment = request.args.get('comment')
        return UserPermission(userId).delete(role, comment)
