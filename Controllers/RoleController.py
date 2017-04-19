from Helpers.DatabaseHelper import DatabaseHelper
from flask_restful import Resource,request
from Models.Role import Role
from Constants.Status import *

class RoleController(Resource):

    def get(self,id=None):
        return Role().get(id)


    def put(self,id=None):
        res=request.get_json(force=True)
        if id==None:
            return ROLE_EMPTY

        dbhelper=DatabaseHelper()
        queryString="update role set name=%s where id=%s"
        values=(res['name'],id)
        rowid=dbhelper.transact(queryString,values)

        return rowid


    def post(self):
        res = request.get_json(force=True)

        dbhelper = DatabaseHelper()
        queryString = "insert into role(name) values(%s)"
        values = (res['name'],)
        rowid = dbhelper.transact(queryString, values)
        return rowid

    def delete(self,id=None):
        if id == None:
            return ROLE_EMPTY

        dbhelper = DatabaseHelper()
        queryString = "delete from role where id=%s"
        values = (id,)
        rowid = dbhelper.transact(queryString, values)
        return rowid


