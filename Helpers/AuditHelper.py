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

    def audit_delete(self,audit_info,id):
        self.update_approval_status(audit_info['table_name'],id,'D')
        sql="insert into def_change_log(id,table_name,change_type,maker_comment,status) values(%s,%s,%s,%s,%s)"
        res=self.db.transact(sql,(id,audit_info['table_name'],audit_info['change_type'],audit_info['comment'],'PENDING'))
        self.db.commit()

        return id


