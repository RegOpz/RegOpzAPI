from Helpers.DatabaseHelper import DatabaseHelper
from datetime import datetime
from Constants.Status import *

# approval_status  'U' - update
#                  'D' -delete
#                  'I' - insert
#                  'A'-appoved
#                  'R' - rejected

class AuditHelper(object):
    def __init__(self):
        self.db=DatabaseHelper()

    def update_approval_status(self,table_name,id,status):
        sql = "update " + table_name + " set approval_status='"+status+"'  where id=%s"
        res = self.db.transact(sql, (id,))
        self.db.commit()

    def audit_delete(self,data,id):
        audit_info=data['audit_info']
        sql="insert into def_change_log(id,table_name,change_type,maker_comment,status) values(%s,%s,%s,%s,%s)"
        res=self.db.transact(sql,(id,audit_info['table_name'],audit_info['change_type'],audit_info['comment'],'PENDING'))
        self.update_approval_status(audit_info['table_name'], id, 'D')
        self.db.commit()

        return id

    def audit_update(self,data,id):
        audit_info=data['audit_info']
        update_info=data['update_info']
        table_name=data['table_name']

        for col in update_info.keys():
            old_val=self.db.query("select "+col + " from "+ table_name+" where id="+str(id)).fetchone()[col]
            new_val = update_info[col]
            print(col,old_val,new_val)

            def_change_insert=0
            if old_val != new_val and col!='approval_status':
                print(col, old_val, new_val)
                sql="insert into def_change_log(id,table_name,field_name,old_val,new_val,change_type,maker_comment,status)\
                    values(%s,%s,%s,%s,%s,%s,%s,%s)"
                params=(id,audit_info['table_name'],col,old_val,new_val,audit_info['change_type'],audit_info['comment'],'PENDING')
                res=self.db.transact(sql,params)
                def_change_insert+=1

        if def_change_insert >0:
            self.update_approval_status(audit_info['table_name'], id, 'U')

        self.db.commit()

        return id

    def audit_insert(self, data, id):
        audit_info = data['audit_info']
        sql = "insert into def_change_log(id,table_name,change_type,maker_comment,status) values(%s,%s,%s,%s,%s)"
        res = self.db.transact(sql, (id, audit_info['table_name'], audit_info['change_type'], audit_info['comment'], 'PENDING'))
        self.update_approval_status(audit_info['table_name'], id, 'I')
        self.db.commit()

        return id


