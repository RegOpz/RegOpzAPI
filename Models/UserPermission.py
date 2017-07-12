from Helpers.DatabaseHelper import DatabaseHelper
from Constants.Status import *

class UserPermission(object):
    def __init__(self):
        pass

    def get(self):
        dbhelper = DatabaseHelper()
        queryString_1 = "SELECT role FROM roles"
        roleQuery = dbhelper.query(queryString_1)
        roles = roleQuery.fetchall()
        if len(roles) == 0:
            return ROLE_EMPTY
        queryString_2 = "SELECT component, permission FROM roles JOIN (permissions, components) ON (roles.id=permissions.role_id AND permissions.component_id=components.id) WHERE roles.role=%s"
        dataList = []
        for role in roles:
            queryParams = (role['role'], )
            permQuery = dbhelper.query(queryString_2, queryParams)
            permissions = permQuery.fetchall()
            data = {}
            if len(permissions) != 0:
                data['role'] = role['role']
                for perm in permissions:
                    data[perm['component']] = perm['permission']
            dataList.append(data)
        return dataList

    def obtain(self, userId=None):
        if userId:
            queryString = 'SELECT * FROM vuserpermissions WHERE username=%s'
            queryParams = (userId, )
            dbhelper = DatabaseHelper()
            permissions = dbhelper.query(queryString, queryParams)
            permissionList = permissions.fetchall()
            if len(permissionList) == 0:
                return False
            self.role = permissionList[0]['role']
            self.permission = {}
            for entry in permissionList:
                self.permission[entry['component']] = entry['permission']
            return self.__dict__
        else:
            raise ValueError('UserId not specified!')

    def save(self, entry=None):
        if entry:
            self.role = entry['role']
            self.permission = entry['permission']
            if len(self.permission) == 0:
                return PERMISSION_EMPTY
            dbhelper = DatabaseHelper()
            queryString = "SELECT id from roles where role=%s"
            queryParams_1 = (self.role, )
            cur = dbhelper.query(queryString, queryParams_1)
            data = cur.fetchone()
            if data:
                rowId = data['id']
            else:
                queryString_1 = "INSERT INTO roles (role) VALUES(%s)"
                try:
                    rowId = dbhelper.transact(queryString_1, queryParams_1)
                except Exception:
                    return { "msg": "Failed to create role " + self.role },403
            queryString_2 = "INSERT INTO permissions (role_id, component_id, permission) VALUES (%s,(SELECT id FROM components WHERE component=%s),%s) ON DUPLICATE KEY UPDATE permission=%s"
            queryParams_2 = []
            for comp, perm in self.permission.items():
                queryParams_2.append( (rowId, comp, perm, perm, ) )
            try:
                if len(self.permission) > 1:
                    lastRowId = dbhelper.transactmany(queryString_2, queryParams_2)
                else:
                    rowId = dbhelper.transact(queryString_2, queryParams_2[0])
            except Exception as e:
                print(e)
                return { "msg": "Failed to add permission for role " + self.role },403
            return { "msg": "Permission updation successful" },200
        return ROLE_EMPTY

    def remove(self, role=None):
        if role:
            self.role = role
            dbhelper = DatabaseHelper()
            queryString_1 = "SELECT id FROM roles WHERE role=%s"
            queryParams_1 = (self.role, )
            cur = dbhelper.query(queryString_1, queryParams_1)
            data = cur.fetchone()
            if data:
                rowId = data['id']
                queryString_2 = "DELETE FROM permissions WHERE role_id=%s"
                queryParams_2 = (rowId, )
                try:
                    lastRowId = dbhelper.transact(queryString_2, queryParams_2)
                except Exception:
                    return { "msg": "Failed to remove permissions for role " + self.role },403
                queryString_3 = "DELETE FROM roles WHERE id=%s"
                try:
                    lastRowId = dbhelper.transact(queryString_3, queryParams_2)
                    return { "msg": "Role and Permission deletion successful" },200
                except Exception:
                    return { "msg": "Failed to remove role " + self.role },403
            return ROLE_EMPTY
        return ROLE_EMPTY
