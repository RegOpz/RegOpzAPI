from Helpers.DatabaseHelper import DatabaseHelper

class Role(object):
    def __init__(self,params=None):
        if params:
            self.id=params['id']
            self.name=params['name']
        else:
            self.id=None
            self.name=None

    def add(self):
        dbhelper=DatabaseHelper()
        values=(self.id,self.name)
        queryString = "INSERT INTO role(id, name) VALUES (%s, %s)"

        rowid = dbhelper.transact(queryString, values)
        return self.get(rowid)

    def get(self,id=None):
        dbhelper=DatabaseHelper()

        if id:
            queryParams=(id,)
            queryString="select * from role where id=%s"

            roles=dbhelper.query(queryString,queryParams)
            role = roles.fetchone()

            if role:
                print("Hey there " + str(roles.rowcount))
                self.id=role['id']
                self.name=role['name']
                return self.__dict__
            return None
        else:
            roleslist=[]
            queryString="select * from role"
            allroles=dbhelper.query(queryString)

            for rsc in allroles:
              roleslist.append({'id':rsc['id'],'name':rsc['name']})

            return roleslist

        return None


