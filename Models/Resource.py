from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.utils import autheticateTenant

class Resource(object):
    def __init__(self,params=None):
        self.domain_info = autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.dbhelper = DatabaseHelper(self.tenant_info)
        if params:
            self.id=params['id']
            self.name=params['name']
        else:
            self.id=None
            self.name=None

    def add(self):
        values=(self.id,self.name)
        queryString = "INSERT INTO resource(id, name) VALUES (%s, %s)"

        rowid = self.dbhelper.transact(queryString, values)
        return self.get(rowid)

    def get(self,id=None):

        if id:
            queryParams=(id,)
            queryString="select * from resource where id=%s"

            resources=self.dbhelper.query(queryString,queryParams)
            resource = resources.fetchone()

            if resource:
                resource=resources.fetchone()
                self.id=resource['id']
                self.name=resource['name']
                return self.__dict__
            return None
        else:
            resourceslist=[]
            queryString="select * from resource"
            allresources=self.dbhelper.query(queryString)

            for rsc in allresources:
              resourceslist.append({'id':rsc['id'],'name':rsc['name']})

            return resourceslist

        return None
