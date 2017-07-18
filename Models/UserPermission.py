from Helpers.DatabaseHelper import DatabaseHelper
from Constants.Status import *

class UserPermission(object):
    def __init__(self):
        self.dbhelper = DatabaseHelper()

    def get(self, roleId = None):
        queryString_1 = "SELECT * FROM roles"
        queryParams = ()
        if roleId:
            queryString_1 += " where role=%s"
            queryParams = (roleId,)
        roleQuery = self.dbhelper.query(queryString_1,queryParams)
        roles = roleQuery.fetchall()
        if len(roles) == 0:
            return ROLE_EMPTY
        dataList = []
        for role in roles:
            queryParams = (role['id'], )
            queryString = "SELECT p.*,c.component FROM components c \
                           JOIN permissions p ON\
                          p.component_id = c.id AND p.role_id = %s"
            if roleId:
                queryString = queryString.replace("JOIN","LEFT JOIN")
            compQuery = self.dbhelper.query(queryString, queryParams)
            components = compQuery.fetchall()
            print(components)
            componentList=[]
            for component in components:
                queryParams = (role['id'],component['component_id'], )
                queryString = "SELECT p.permission_id,pd.permission FROM permission_def pd \
                              LEFT JOIN permissions p ON\
                              p.permission_id = pd.id AND p.role_id = %s and p.component_id=%s"
                permQuery = self.dbhelper.query(queryString, queryParams)
                permissions = permQuery.fetchall()
                compData = {
                    'component':component['component'],
                    'permissions':permissions
                }
                componentList.append(compData)
            data = {
                'role': role['role'],
                'components': componentList
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
            self.permissions = entry['permissions']
            if len(self.permissions) == 0:
                return PERMISSION_EMPTY
            try:
                roleId = self.getRoleId()
            except ValueError as e:
                print("Error in Get Role", e)
                return { "msg": "Failed to create role " + self.role },403
            queryString = "INSERT INTO permissions (role_id, component_id, permission_id) VALUES\
                (%s,(SELECT id FROM components WHERE component=%s),%s)"
            queryParams = []
            for permission in self.permissions:
                component = permission['component']
                try:
                    permissionId = self.getPermissionId(permission['permission'])
                except ValueError as e:
                    print("Error in Get Permission", e)
                    continue
                queryParams.append( (roleId, component, permissionId, ) )
            try:
                if len(queryParams) > 1:
                    lastRowId = self.dbhelper.transactmany(queryString, queryParams)
                else:
                    rowId = self.dbhelper.transact(queryString, queryParams[0])
            except Exception as e:
                print("Error in save permission", e)
                return { "msg": "Failed to add permission for role " + self.role },403
            return { "msg": "Permission updation successful" },200
        return ROLE_EMPTY

    def getRoleId(self):
        if self.role:
            queryString = "SELECT id FROM roles WHERE role=%s"
            queryParams = (self.role, )
            cur = self.dbhelper.query(queryString, queryParams)
            data = cur.fetchone()
            if data:
                return data['id']
            else:
                queryString1 = "INSERT INTO roles (role) VALUES(%s)"
                try:
                    return self.dbhelper.transact(queryString1, queryParams)
                except Exception:
                    raise ValueError("Cannot create role from given data!")
        raise ValueError("Cannot find role from given data!")

    def getPermissionId(self, permission = None):
        if permission:
            queryString = "SELECT id FROM permission_def WHERE permission=%s"
            queryParams = (permission, )
            cur = self.dbhelper.query(queryString, queryParams)
            data = cur.fetchone()
            if data:
                return data['id']
            else:
                queryString1 = "INSERT INTO permission_def (permission) VALUES(%s)"
                try:
                    self.dbhelper.transact(queryString1, queryParams)
                except Exception:
                    raise ValueError("Cannot create permission from given data!")
        raise ValueError("Cannot find permission from given data!")

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
            return ROLE_EMPTY
        return ROLE_EMPTY
