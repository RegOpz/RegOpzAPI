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
            componentList = []
            for component in components:
                queryString = "SELECT p.permission_id,pd.permission FROM permission_def pd \
                              LEFT JOIN permissions p ON\
                              (p.permission_id = pd.id AND p.role_id = %s \
                              AND p.component_id=%s) WHERE pd.component_id=%s AND p.in_use='Y'"
                queryParams = (role['id'],component['component_id'],component['component_id'], )
                permQuery = self.dbhelper.query(queryString, queryParams)
                permissions = permQuery.fetchall()
                compData = {
                    'component': component['component'],
                    'permissions': permissions
                }
                componentList.append(compData)
            data = {
                'role': role['role'],
                'components': componentList
            }
            if roleId:
                return data
            dataList.append(data)
        if len(dataList) == 0:
            return PERMISSION_EMPTY
        return dataList

    def post(self, entry = None, delete = None):
        if entry:
            print("Data recieved via POST in permissions:", entry)
            self.role = entry['role']
            self.data = entry['components']
            roleId = self.getRoleId()
            if not roleId:
                roleId = self.setRoleId(True)
                print("New Role Added.")
            if not roleId:
                return ROLE_EMPTY
            for item in self.data:
                component = item['component']
                componentId = self.getComponentId(component)
                if not componentId:
                    print("Invalid component specified.")
                    continue
                permissions = item['permissions']
                for permission in permissions:
                    permissionId = self.getPermissionId(permission['permission'], component);
                    if permissionId:
                        Permissionflag = permission['permission_id']
                        Queryflag = not delete and Permissionflag
                        rowId = self.setPermission(roleId, componentId, permissionId, Queryflag)
                        if not rowId:
                            print("Error occured while updating permissions")
                    else:
                        print("Invalid permissions given against", component)
                        continue
            return { "msg": "Permission update successful." },200
        else:
            return ROLE_EMPTY

    def delete(self, role = None):
        if role:
            data = self.get(role)
            if data:
                res = self.post(self, data, True)
                rowId = self.setRoleId(False)
                if not rowId:
                    print("Error occured while deleting role:", role)
                return res
        return ROLE_EMPTY

    def getRoleId(self):
        if self.role:
            queryString = "SELECT id FROM roles WHERE role=%s and in_use='Y'"
            cur = self.dbhelper.query(queryString, (self.role, ))
            data = cur.fetchone()
            if data:
                return data['id']
            return False

    def setRoleId(self, inUseFlag = None):
        # Need to add who added it
        inUse = 'Y' if inUseFlag else 'N'
        queryString = "INSERT INTO roles (role, in_use) VALUES(%s,%s) ON DUPLICATE KEY UPDATE in_use=%s"
        try:
            rowId = self.dbhelper.transact(queryString, (self.role, inUse, inUse ))
            self.dbhelper.commit()
            return rowId
        except Exception:
            return False

    def getComponentId(self, component = None):
        if component:
            queryString = "SELECT id FROM components WHERE component=%s"
            queryParams = (component, )
            cur = self.dbhelper.query(queryString, queryParams)
            data = cur.fetchone()
            if data:
                return data['id']
        return False

    def getPermissionId(self, permission = None, componentId = None):
        if permission and componentId:
            queryString = "SELECT id FROM permission_def WHERE permission=%s"
            queryParams = (permission, )
            cur = self.dbhelper.query(queryString, queryParams)
            data = cur.fetchone()
            if data:
                return data['id']
        return False

    def setPermission(self, roleId = None, componentId = None, permissionId = None, flag = None):
        # Get user id from token
        inUse = 'Y' if flag else 'N'
        if roleId and componentId and permissionId:
            queryString = "INSERT INTO permissions (role_id, component_id, permission_id, in_use) VALUES\
                (%s,%s,%s,'Y') ON DUPLICATE KEY UPDATE in_use=%s"
            queryParams = (roleId, componentId, permissionId, inUse, )
            try:
                rowId = self.dbhelper.transact(queryString, queryParams)
                self.dbhelper.commit()
                return rowId
            except Exception:
                return False
        return False

    def obtain(self, userId = None):
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
