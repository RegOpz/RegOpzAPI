from flask_restful import Resource,request
from Models.UserPermission import UserPermission
from Models.Token import Token

class RoleController(Resource):
    def get(self, role = None):
        inUseCheck = request.args.get('inUseCheck') if request.args.get('inUseCheck') else 'Y'
        tenant_id = request.args.get('tenant_id')
        return UserPermission().get(role, inUseCheck, tenant_id)

    def post(self):
        userId = Token().authenticate()
        res = request.get_json(force=True)
        return UserPermission(userId=userId).post(res)

    def delete(self, role = None):
        userId = Token().authenticate()
        comment = request.args.get('comment')
        return UserPermission(userId=userId).delete(role, comment)
