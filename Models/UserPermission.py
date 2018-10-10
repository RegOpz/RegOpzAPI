from Helpers.DatabaseHelper import DatabaseHelper
from Controllers.DefChangeController import DefChangeController
from Constants.Status import *
from flask import request
import json
from Helpers.utils import autheticateTenant

class UserPermission(object):
    def __init__(self, tenant_info = None,userId = None):
        if tenant_info:
            self.tenant_info=tenant_info
        else:
            self.domain_info = autheticateTenant()
            if self.domain_info:
                tenant_info = json.loads(self.domain_info)
                self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
        self.dbhelper = DatabaseHelper(self.tenant_info)
        self.audit = DefChangeController(tenant_info=self.tenant_info)
        self.user_id = userId

    def get(self, roleId = None, inUseCheck = 'Y', tenant_id='regopz', getDetails=True):
        print("\nRecieved GET request in Permissions",inUseCheck, tenant_id)
        isMaster = True if tenant_id == 'regopz' else False
        queryString_1 = "SELECT * FROM roles WHERE in_use = 'Y'"
        queryParams = ()
        if roleId:
            queryString_1 += " AND role=%s"
            queryParams = (roleId,)
        roleQuery = self.dbhelper.query(queryString_1, queryParams)
        roles = roleQuery.fetchall()
        if len(roles) == 0:
            return ROLE_EMPTY
        dataList = []
        for role in roles:
            componentList = self.getComponentList(role,inUseCheck, isMaster, getDetails)
            sourceList = self.getsourceList(role,inUseCheck, isMaster, getDetails)
            reportList = self.getreportList(role,inUseCheck, isMaster, getDetails)
            data = {
                'role': role['role'],
                'last_updated_by': role['last_updated_by'],
                'components': componentList,
                'sources': sourceList,
                'reports': reportList
            }
            if roleId:
                return data
            dataList.append(data)
        if len(dataList) == 0:
            return PERMISSION_EMPTY
        return dataList

    def getComponentList(self, role, inUseCheck, isMaster, getDetails=True):
        queryString = "SELECT * FROM components "
        compQuery = self.dbhelper.query(queryString)
        components = compQuery.fetchall()
        # print(components)
        componentList = []
        if getDetails:
            select_cols = "p.permission_id,p.dml_allowed,p.in_use,pd.permission,dc.status,pd.id,p.id as p_id, " + \
                          "p.granted, p.granted as original_granted "

        else:
            select_cols = "p.permission_id,p.dml_allowed,p.in_use,pd.permission,p.granted "

        for component in components:
            queryString = "SELECT " + select_cols + \
                          " FROM permission_def pd JOIN roles r on r.id = %s and pd.component_id=%s " + \
                          ("" if inUseCheck == 'Y' else "LEFT") + " JOIN permissions p ON " +\
                          "(p.permission_id = pd.id AND p.role_id = %s " + \
                          "AND p.component_id = %s) " + \
                          "LEFT JOIN def_change_log dc ON " + \
                          "p.id = dc.id AND dc.status='PENDING' and dc.table_name='permissions'" + \
                          ("WHERE p.in_use = 'Y' and p.granted=1 " if inUseCheck == 'Y' else " ")
            queryParams = (role['id'], component['id'], role['id'], component['id'],  )
            permQuery = self.dbhelper.query(queryString, queryParams)
            permissions = permQuery.fetchall()
            for permission in permissions:
                if permission['granted']:
                    permission['granted'] = True
                    permission['original_granted'] = True
                if not permission['granted'] or (permission['in_use']=='N' and not permission['status']):
                    permission['granted']=False
                    permission['original_granted']=False
            if permissions:
                compData = {
                    'component': component['component'],
                    'permissions': permissions
                }
                if compData not in componentList:
                    componentList.append(compData)
        return componentList

    def getsourceList(self, role, inUseCheck, isMaster, getDetails=True):
        sourceList=[]
        if isMaster:
            return sourceList

        if getDetails:
            select_cols = "role,source_id,source_file_name,source_table_name,source_description,permission_details,dc.status," + \
                            "permission_details as original_permission_details,op.dml_allowed, op.in_use,op.id as op_id  "

        else:
            select_cols = "role,source_id,source_file_name,source_table_name,permission_details,op.in_use,dc.status "

        queryString = "select " + select_cols + \
                        " from data_source_information dsi join roles r on role=%s " + \
                        ("" if inUseCheck == 'Y' else "LEFT") + " join object_permissions op " + \
                        " on object_type='SOURCE' and object_id = source_id and op.role_id = r.id " +\
                        "LEFT JOIN def_change_log dc ON " + \
                        "op.id = dc.id AND dc.status='PENDING' and dc.table_name='object_permissions' " + \
                        ("WHERE op.in_use = 'Y'" if inUseCheck == 'Y' else " ")
        queryParams = (role['role'],)
        permQuery = self.dbhelper.query(queryString, queryParams)
        sourceList = permQuery.fetchall()
        for i,src in enumerate(sourceList):
            if not src['permission_details'] or (src['in_use']=='N' and not src['status']):
                src['permission_details']=json.dumps({'access_type':'No access','access_condition':''})
                src['original_permission_details']=json.dumps({'access_type':'No access','access_condition':''})
            else:
                src['permission_details']=src['permission_details']
        return sourceList

    def getreportList(self, role, inUseCheck, isMaster, getDetails=True):
        reportList=[]
        if getDetails:
            select_cols = "role,report_id,report_type,report_description,permission_details, dc.status, " + \
                            "permission_details as original_permission_details,op.dml_allowed, op.in_use,op.id as op_id "

        else:
            select_cols = "role,report_id,report_type,permission_details,op.in_use,dc.status "

        queryString = "select " + select_cols + \
                        " from report_def_catalog" + ("_master " if isMaster else "") + " join roles r on role=%s " + \
                        ("" if inUseCheck == 'Y' else "LEFT") + " join object_permissions op " + \
                        "on object_type='REPORT' and object_id = report_id and op.role_id=r.id " + \
                        "LEFT JOIN def_change_log dc ON " + \
                        "op.id = dc.id AND dc.status='PENDING' and dc.table_name='object_permissions' " + \
                        ("WHERE op.in_use = 'Y'" if inUseCheck == 'Y' else " ")
        queryParams = (role['role'],)
        permQuery = self.dbhelper.query(queryString, queryParams)
        reportList = permQuery.fetchall()
        for i,rpt in enumerate(reportList):
            if not rpt['permission_details'] or (rpt['in_use']=='N' and not rpt['status']):
                rpt['permission_details']=json.dumps({'access_type':'No access'})
                rpt['original_permission_details']=json.dumps({'access_type':'No access'})
            else:
                rpt['permission_details']=rpt['permission_details']
        return reportList

    def post(self, entry = None, add = True):
        if entry:
            print("\nData recieved via", "POST" if add else "DELETE", "in permissions:") #, entry)
            print(entry)
            self.role = entry['role']
            self.comment = entry['comment']
            self.maker = entry['maker']
            self.maker_tenant_id = entry['maker_tenant_id']
            self.group_id = entry['group_id']
            self.components = entry['components']
            self.sources = entry['sources'] if self.maker_tenant_id != 'regopz' else []
            self.reports = entry['reports']
            roleId = self.getRoleId(True)
            if not roleId:
                roleId = self.setRoleId(True)
                if roleId:
                    print("New Role", self.role, "Added.")
                else:
                    return { "msg": "Failed to add role " + self.role},402
            self.processComponents(roleId)
            self.processSources(roleId)
            self.processReports(roleId)
            return { "msg": "Permission update successful for " + self.role },200
        else:
            return ROLE_EMPTY

    def delete(self, role = None, comment = None):
        if role:
            data = self.get(role)
            if data:
                data["comment"] = comment
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
        id = self.getRoleId(False)
        audit_info = {
            "table_name": "roles",
            "id": id,
            "change_type": dml,
            "comment": self.comment,
            "change_reference": "Role: " + self.role,
            "maker": self.maker,
            "maker_tenant_id": self.maker_tenant_id,
            "group_id": self.group_id
        }
        update_info={
            "role": self.role,
            "id": id
        }
        audit_data = {
            "table_name": "roles",
            "change_type": dml,
            "audit_info": audit_info,
            "update_info": update_info
        }
        if dml == "INSERT" and not id:
            return self.audit.insert_data(data=audit_data)
        else:
            return self.audit.update_or_delete_data(data=audit_data, id=id, create_new_flag=False)

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

    def processComponents(self, roleId):
        for item in self.components:
            self.component = item['component']
            componentId = self.getComponentId(self.component)
            if not componentId:
                print("Invalid component specified.")
                continue
            permissions = item['permissions']
            for permission in permissions:
                add = True if not permission['p_id'] else False
                dml_narration= "GRANT" if permission['granted'] else "REVOKE"
                id = permission['p_id']
                permissionDefId = permission['id']
                granted = permission['granted']
                self.permission = permission['permission']
                self.change_reference = "Role: " + self.role + " " \
                                        + dml_narration + " Permission of " + self.permission \
                                        + " on component " + self.component
                self.table_name = "permissions"
                self.update_info = {
                    "role_id": roleId,
                    "component_id": componentId,
                    "permission_id": permissionDefId,
                    "granted": granted,
                    "id": id
                }
                rowId = self.setPermission(add, id)
                if not rowId:
                    print("Error occured while updating permission", self.permission, "on", self.component)

    def processSources(self, roleId):
        for item in self.sources:
            objectId = item['source_id']
            if not objectId:
                print("Invalid source specified.")
                continue
            # op_id exists indicating existing object_permissions entry
            add = True if not item['op_id'] else False
            permission_details = json.loads(item['permission_details'])
            dml_narration= "NEW ACCESS" if add else "AMEND ACCESS"
            id = item['op_id']
            self.permission = permission_details['access_type']
            self.change_reference = "Role: " + self.role + " " \
                                    + dml_narration + " [" + self.permission \
                                    + "] on source " + item['source_file_name'] \
                                    + " for table " + item['source_table_name']
            self.table_name = "object_permissions"
            self.update_info = {
                "role_id": roleId,
                "object_id": objectId,
                "object_type": "SOURCE",
                "permission_details": item['permission_details'],
                "id": id
            }
            rowId = self.setPermission(add, id)
            if not rowId:
                print("Error occured while updating source permission", self.permission, "on", item['source_table_name'])

    def processReports(self, roleId):
        for item in self.reports:
            objectId = item['report_id']
            if not objectId:
                print("Invalid report specified.")
                continue
            # op_id exists indicating existing object_permissions entry
            add = True if not item['op_id'] else False
            permission_details = json.loads(item['permission_details'])
            dml_narration= "NEW ACCESS" if add else "AMEND ACCESS"
            id = item['op_id']
            self.permission = permission_details['access_type']
            self.change_reference = "Role: " + self.role + " " \
                                    + dml_narration + " [" + self.permission \
                                    + "] on " + item['report_type'] + " report " + objectId
            self.table_name = "object_permissions"
            self.update_info = {
                "role_id": roleId,
                "object_id": objectId,
                "object_type": "REPORT",
                "permission_details": item['permission_details'],
                "id": id
            }
            rowId = self.setPermission(add, id)
            if not rowId:
                print("Error occured while updating report permission", self.permission, "on", item['report_id'])


    def setPermission(self, dml_flag = False, id=None):
        dml = "INSERT" if dml_flag else "UPDATE"
        if self.update_info:
            update_info=self.update_info
            audit_info = {
                "table_name": self.table_name,
                "id": id,
                "change_type": dml,
                "comment": self.comment,
                "change_reference": self.change_reference,
                "maker": self.maker,
                "maker_tenant_id": self.maker_tenant_id,
                "group_id": self.group_id
            }
            audit_data = {
                "table_name": self.table_name,
                "change_type": dml,
                "audit_info": audit_info,
                "update_info": update_info
            }
            print("setPermission : {} {}".format(dml,update_info))
            if dml == "INSERT":
                return self.audit.insert_data(data=audit_data, create_new_flag=True if not id else False)
            else:
                return self.audit.update_or_delete_data(data=audit_data, id=id, create_new_flag=False)
        return False
