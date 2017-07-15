from Helpers.DatabaseHelper import DatabaseHelper
from Constants.Status import *

class UserPermission(object):
    def __init__(self):
        self.dbhelper = DatabaseHelper()

    def get(self):
        queryString_1 = "SELECT role FROM roles"
        roleQuery = self.dbhelper.query(queryString_1)
        roles = roleQuery.fetchall()
        if len(roles) == 0:
            return ROLE_EMPTY
        queryString_2 = "SELECT component, permission FROM roles JOIN (permissions, components, permission_def) ON\
            (roles.id = permissions.role_id AND permissions.component_id = components.id AND permissions.permission_id = permission_def.id)\
            WHERE roles.role=%s"
        dataList = []
        for role in roles:
            queryParams = (role['role'], )
            permQuery = self.dbhelper.query(queryString_2, queryParams)
            permissions = permQuery.fetchall()
            data = {
                'role': role['role'],
                'permissions': permissions
            }
            dataList.append(data)
        if len(dataList) == 0:
            return PERMISSION_EMPTY
        return dataList

    def obtain(self, userId=None):
        if userId:
            queryString = "SELECT permissions.id AS id, regopzuser.name AS username, role, component, permission\
                FROM regopzuser JOIN (roles, components, permissions, permission_def) ON\
                (regopzuser.role_id = roles.id = permissions.role_id AND components.id = permissions.component_id\
                AND permissions.permission_id = permission_def.id) WHERE regopzuser.name=%s"
            queryParams = (userId, )
            permissions = self.dbhelper.query(queryString, queryParams)
            permissionList = permissions.fetchall()
            if len(permissionList) == 0:
                return PERMISSION_EMPTY
            self.role = permissionList[0]['role']
            self.permission = {}
            for entry in permissionList:
                self.permission[entry['component']] = entry['permission']
            return self.__dict__
        else:
            raise ValueError('UserId not specified!')

    def save(self, entry = None):
        if entry:
            self.role = entry['role']
            self.permission = entry['permission']
            if len(self.permission) == 0:
                return PERMISSION_EMPTY
            queryString = "SELECT id from roles where role=%s"
            queryParams_1 = (self.role, )
            cur = self.dbhelper.query(queryString, queryParams_1)
            data = cur.fetchone()
            if data:
                rowId = data['id']
            else:
                queryString_1 = "INSERT INTO roles (role) VALUES(%s)"
                try:
                    rowId = self.dbhelper.transact(queryString_1, queryParams_1)
                except Exception:
                    return { "msg": "Failed to create role " + self.role },403
            queryString_2 = "INSERT INTO permissions (role_id, component_id, permission) VALUES \
                (%s,(SELECT id FROM components WHERE component=%s),%s) ON DUPLICATE KEY UPDATE permission=%s"
            queryParams_2 = []
            for comp, perm in self.permission.items():
                queryParams_2.append( (rowId, comp, perm, perm, ) )
            try:
                if len(self.permission) > 1:
                    lastRowId = self.dbhelper.transactmany(queryString_2, queryParams_2)
                else:
                    rowId = self.dbhelper.transact(queryString_2, queryParams_2[0])
            except Exception as e:
                return { "msg": "Failed to add permission for role " + self.role },403
            return { "msg": "Permission updation successful" },200
        return ROLE_EMPTY

    def remove(self, role = None):
        self.dbhelper = DatabaseHelper()
        if role:
            self.role = role
            queryString_1 = "SELECT id FROM roles WHERE role=%s"
            queryParams_1 = (self.role, )
            cur = self.dbhelper.query(queryString_1, queryParams_1)
            data = cur.fetchone()
            if data:
                rowId = data['id']
                queryString_2 = "DELETE FROM permissions WHERE role_id=%s"
                queryParams_2 = (rowId, )
                try:
                    lastRowId = self.dbhelper.transact(queryString_2, queryParams_2)
                except Exception:
                    return { "msg": "Failed to remove permissions for role " + self.role },403
                queryString_3 = "DELETE FROM roles WHERE id=%s"
                try:
                    lastRowId = self.dbhelper.transact(queryString_3, queryParams_2)
                    return { "msg": "Role and Permission deletion successful" },200
                except Exception:
                    return { "msg": "Failed to remove role " + self.role },403
            return ROLE_EMPTY
        return ROLE_EMPTY
