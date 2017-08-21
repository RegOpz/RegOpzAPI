from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.AuditHelper import AuditHelper
from Constants.Status import *

class UserPermission(object):
    def __init__(self, userId = None):
        self.dbhelper = DatabaseHelper()
        self.audit = AuditHelper("def_change_log")
        self.user_id = userId

    def get(self, roleId = None):
        print("\nRecieved GET request in Permissions")
        queryString_1 = "SELECT * FROM roles WHERE in_use='Y'"
        queryParams = ()
        if roleId:
            queryString_1 += " AND role=%s"
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
                          p.component_id = c.id AND p.role_id = %s AND p.in_use='Y'"
            if roleId:
                queryString = queryString.replace("JOIN","LEFT JOIN")
            compQuery = self.dbhelper.query(queryString, queryParams)
            components = compQuery.fetchall()
            # print(components)
            componentList = []
            for component in components:
                queryString = "SELECT p.permission_id,pd.permission FROM permission_def pd \
                              LEFT JOIN permissions p ON\
                              (p.permission_id = pd.id AND p.role_id = %s \
                              AND p.component_id = %s) WHERE p.in_use = 'Y'"
                queryParams = (role['id'], component['component_id'], )
                permQuery = self.dbhelper.query(queryString, queryParams)
                permissions = permQuery.fetchall()
                if permissions:
                    compData = {
                        'component': component['component'],
                        'permissions': permissions
                    }
                    if compData not in componentList:
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

    def post(self, entry = None, add = True):
        if entry:
            print("\nData recieved via", "POST" if add else "DELETE", "in permissions:") #, entry)
            self.role = entry['role']
            self.data = entry['components']
            roleId = self.getRoleId(True)
            if not roleId:
                roleId = self.setRoleId(True)
                if roleId:
                    print("New Role", self.role, "Added.")
                else:
                    return { "msg": "Failed to add role " + self.role},402
            for item in self.data:
                self.component = item['component']
                componentId = self.getComponentId(self.component)
                if not componentId:
                    print("Invalid component specified.")
                    continue
                permissions = item['permissions']
                for permission in permissions:
                    add = True if permission['permission_id'] else False
                    self.permission = permission['permission']
                    permissionId = self.getPermissionId(self.permission, componentId)
                    if permissionId:
                        rowId = self.setPermission(roleId, componentId, permissionId, add)
                        if not rowId:
                            print("Error occured while updating permission", self.permission, "on", self.component)
                    else:
                        print("Invalid permissions given against", self.component)
                        continue
            return { "msg": "Permission update successful for " + self.role },200
        else:
            return ROLE_EMPTY

    def delete(self, role = None):
        if role:
            data = self.get(role)
            if data:
                res = self.post(data, False)
                rowId = self.setRoleId(False)
                if not rowId:
                    print("Error occured while deleting role:", role)
                return res
        return ROLE_EMPTY

    def getRoleId(self, checkInUse = None):
        if self.role:
            queryString = "SELECT id FROM roles WHERE role=%s"
            if checkInUse:
                queryString += " AND in_use='Y'"
            cur = self.dbhelper.query(queryString, (self.role, ))
            data = cur.fetchone()
            if data:
                return data['id']
            return False

    def setRoleId(self, dml_flag = False):
        dml = "INSERT" if dml_flag else "DELETE"
        audit_info = {
            "table_name": "roles",
            "change_type": dml,
            "comment": "Nothing",
            "change_reference": "Role: " + self.role,
            "maker": self.user_id
        }
        id = self.getRoleId(False)
        if dml == "INSERT":
            if not id:
                queryString = "INSERT INTO roles (role, dml_allowed, in_use) VALUES(%s,'N','N')"
                queryParams = (self.role, )
                try:
                    id = self.dbhelper.transact(queryString, queryParams)
                    self.dbhelper.commit()
                except Exception:
                    return False
            return self.audit.audit_insert({ "audit_info": audit_info }, id)
        else:
            return self.audit.audit_delete({ "audit_info": audit_info }, id)

    def getComponentId(self, component = None):
        if component:
            queryString = "SELECT id FROM components WHERE component=%s"
            queryParams = (component, )
            cur = self.dbhelper.query(queryString, queryParams)
            data = cur.fetchone()
            if data:
                return data['id']
        return False

    def getPermissionId(self, permission = None, componentId = None ):
        if permission:
            queryString = "SELECT id FROM permission_def WHERE permission=%s and component_id=%s"
            queryParams = (permission, componentId, )
            cur = self.dbhelper.query(queryString, queryParams)
            data = cur.fetchone()
            if data:
                return data['id']
        return False

    def setPermission(self, roleId = None, componentId = None, permissionId = None, dml_flag = None):
        dml = "INSERT" if dml_flag else "DELETE"
        if roleId and componentId and permissionId:
            audit_info = {
                "table_name": "permissions",
                "change_type": dml,
                "comment": "Nothing",
                "change_reference": "Role: " + self.role + " Permission of " + self.permission + " on component " + self.component,
                "maker": self.user_id
            }
            queryString = "SELECT id FROM permissions WHERE role_id=%s AND component_id=%s AND permission_id=%s"
            queryParams = (roleId, componentId, permissionId, )
            cur = self.dbhelper.query(queryString, queryParams)
            data = cur.fetchone()
            id = data['id'] if data else None
            if dml == "INSERT":
                if not id:
                    queryString = "INSERT INTO permissions (role_id, component_id, permission_id, dml_allowed, in_use) VALUES (%s,%s,%s,'N','N')"
                    queryParams = (roleId, componentId, permissionId, )
                    try:
                        id = self.dbhelper.transact(queryString, queryParams)
                        self.dbhelper.commit()
                    except Exception:
                        return False
                return self.audit.audit_insert({ "audit_info": audit_info }, id)
            else:
                return self.audit.audit_delete({ "audit_info": audit_info }, id)
        return False
