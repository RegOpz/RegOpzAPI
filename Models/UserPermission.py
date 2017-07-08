from Helpers.DatabaseHelper import DatabaseHelper

class UserPermission(object):
    def __init__(self):
        pass

    def get(self, userId=None):
        if userId:
            queryString = 'SELECT * FROM vuserpermissions WHERE username=%s'
            queryParams = (userId, )
            dbhelper = DatabaseHelper()
            permissions = dbhelper.query(queryString, queryParams)
            permissionList = permissions.fetchall()
            if len(permissionList) == 0:
                return { 'msg': 'No Permission Granted for this user' }
            self.role = permissionList[0]['role']
            self.permission = {}
            for entry in permissionList:
                self.permission[entry['component']] = entry['permission']
            return self.__dict__
        else:
            raise ValueError('UserId not specified!')
