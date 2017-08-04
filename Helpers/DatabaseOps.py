from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.AuditHelper import AuditHelper
from datetime import datetime
from Constants.Status import *

class DatabaseOps(object):
    def __init__(self,audit_table_name):
        self.db=DatabaseHelper()
        self.audit=AuditHelper(audit_table_name)

    def update_or_delete_data(self,data,id):
        if data['change_type']=='DELETE':
            res=self.delete_data(data,id)
        if data['change_type']=='UPDATE':
            res=self.update_data(data,id)
        return res

    def delete_data(self, data, id):
        # sql = "delete from " + table_name + " where id=%s"
        # print(sql)
        #
        # params = (id,)
        # #print(params)
        # res = self.db.transact(sql, params)
        # self.db.commit()
         res=self.audit.audit_delete(data,id)

         return res

    def update_data(self, data, id):
        # table_name = data['table_name']
        # update_info = data['update_info']
        # update_info_cols = update_info.keys()
        #
        # sql = 'update ' + table_name + ' set '
        # params = []
        # for col in update_info_cols:
        #     sql += col + '=%s,'
        #     params.append(update_info[col])
        #
        # sql = sql[:len(sql) - 1]
        # sql += " where id=%s"
        # params.append(id)
        # params_tuple = tuple(params)
        #
        # print(sql)
        # print(params_tuple)
        #
        # res = self.db.transact(sql, params_tuple)
        #
        # if res == 0:
        #     self.db.commit()
        #     return self.ret_source_data_by_id(table_name, id)
        #
        # self.db.rollback()
        # return UPDATE_ERROR

        res = self.audit.audit_update(data, id)
        return res

    def insert_data(self, data):
        table_name = data['table_name']
        update_info = data['update_info']
        update_info_cols = update_info.keys()

        sql = "insert into " + table_name + "("
        placeholders = "("
        params = []

        for col in update_info_cols:
            sql += col + ","
            placeholders += "%s,"
            if col == 'id':
                params.append(None)
            else:
                params.append(update_info[col])

        placeholders = placeholders[:len(placeholders) - 1]
        placeholders += ")"
        sql = sql[:len(sql) - 1]
        sql += ") values " + placeholders

        params_tuple = tuple(params)
        print(sql)
        print(params_tuple)
        id = self.db.transact(sql, params_tuple)
        self.db.commit()

        res=self.audit.audit_insert(data,id)

        return self.ret_source_data_by_id(table_name,id)

    def ret_source_data_by_id(self, table_name, id):
        query = 'select * from ' + table_name + ' where id = %s'
        cur = self.db.query(query, (id,))
        data = cur.fetchone()
        for k, v in data.items():
            if isinstance(v, datetime):
                data[k] = data[k].isoformat()
                print(data[k], type(data[k]))
        if data:
            return data
        return NO_BUSINESS_RULE_FOUND