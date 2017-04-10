from Helpers.DatabaseHelper import DatabaseHelper
from flask_restful import Resource,request
import Models.Resource as rsc
from Constants.Status import *
class ResourceController(Resource):

    def get(self,id=None):
        return rsc.Resource().get(id)


    def put(self,id=None):
        res=request.get_json(force=True)
        if id==None:
            return RESOURCE_EMPTY

        dbhelper=DatabaseHelper()
        queryString="update resource set name=%s where id=%s"
        values=(res['name'],id)
        rowid=dbhelper.transact(queryString,values)

        return rowid


    def post(self):
        res = request.get_json(force=True)

        dbhelper = DatabaseHelper()
        queryString = "insert into resource(name) values(%s)"
        values = (res['name'],)
        rowid = dbhelper.transact(queryString, values)
        return rowid

    def delete(self,id=None):
        if id == None:
            return RESOURCE_EMPTY

        dbhelper = DatabaseHelper()
        queryString = "delete from resource where id=%s"
        values = (id,)
        rowid = dbhelper.transact(queryString, values)
        return rowid


