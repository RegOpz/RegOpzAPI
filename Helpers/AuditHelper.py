from Helpers.DatabaseHelper import DatabaseHelper
from datetime import datetime
from Constants.Status import *


class AuditHelper(object):
    def __init__(self,audit_table_name):
        self.audit_table_name = audit_table_name
        self.db=DatabaseHelper()
        self.business_date_present=False
        column_names=[v['Field'] for v in self.db.query('desc '+audit_table_name).fetchall()]
        print(audit_table_name)
        if('business_date' in column_names):
            self.business_date_present=True



    def update_approval_status(self,table_name,id,dml_allowed,in_use=None):
        sql = "update " + table_name + " set dml_allowed='"+dml_allowed+"'"
        if in_use is not None:
            sql+=",in_use='"+in_use+"' "
        sql+= "  where id=%s"
        res = self.db.transact(sql, (id,))
        self.db.commit()

    def update_audit_record(self,data):
        print(data)
        print(self.audit_table_name)
        sql="update "+self.audit_table_name +" set status=%s,checker_comment=%s,date_of_checking=%s,checker=%s where table_name=%s and id=%s and status='PENDING'"
        #print(sql)
        params=(data["status"],data["checker_comment"],datetime.now(),data["checker"],data["table_name"],data["id"])
        res = self.db.transact(sql,params)
        self.db.commit()
        return res


    def audit_delete(self,data,id):
        audit_info=data['audit_info']
        if self.business_date_present:
            sql = "insert into  " + self.audit_table_name + \
                  " (id,table_name,change_type,maker_comment,status,change_reference,date_of_change,maker,business_date)\
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            res = self.db.transact(sql, (id, audit_info['table_name'], audit_info['change_type'],
                                         audit_info['comment'], 'PENDING', audit_info['change_reference'],
                                         datetime.now(), audit_info['maker'],audit_info['business_date']))
        else:
            sql="insert into  "+self.audit_table_name +\
                " (id,table_name,change_type,maker_comment,status,change_reference,date_of_change,maker)\
                 values(%s,%s,%s,%s,%s,%s,%s,%s)"
            res=self.db.transact(sql,(id,audit_info['table_name'],audit_info['change_type'],audit_info['comment'],
                                      'PENDING',audit_info['change_reference'],datetime.now(),audit_info['maker']))

        self.update_approval_status(table_name=audit_info['table_name'], id=id, dml_allowed='N')
        self.db.commit()

        return id

    def audit_update(self,data,id):
        audit_info=data['audit_info']
        update_info=data['update_info']
        table_name=data['table_name']

        def_change_insert = 0
        old_rec=self.db.query("select * from "+ table_name+" where id="+str(id)).fetchone()
        for col in update_info.keys():
            old_val = old_rec[col]
            new_val = update_info[col]
            #print(col,old_val,new_val)

            if old_val != new_val and col not in ('dml_allowed','in_use'):
                #print(col, old_val, new_val)
                if self.business_date_present:
                    sql="insert into  "+self.audit_table_name +" (id,table_name,field_name,old_val,new_val,\
                    change_type,maker_comment,status,change_reference,date_of_change,maker,business_date)\
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    params=(id,audit_info['table_name'],col,old_val,new_val,audit_info['change_type'],audit_info['comment'],
                    'PENDING',audit_info['change_reference'],datetime.now(),audit_info['maker'],audit_info['business_date'])
                    res=self.db.transact(sql,params)
                else:
                    sql = "insert into  " + self.audit_table_name + " (id,table_name,field_name,old_val,new_val,\
                                        change_type,maker_comment,status,change_reference,date_of_change,maker)\
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    params = (id, audit_info['table_name'], col, old_val, new_val, audit_info['change_type'],
                              audit_info['comment'],'PENDING', audit_info['change_reference'], datetime.now(), audit_info['maker'])
                    res = self.db.transact(sql, params)

                def_change_insert+=1

        if def_change_insert >0:
            self.update_approval_status(table_name=audit_info['table_name'],id=id, dml_allowed='N')

        self.db.commit()

        return id

    def audit_insert(self, data, id):
        audit_info = data['audit_info']
        if self.business_date_present:
            sql = "insert into  " + self.audit_table_name + " (id,table_name,change_type,maker_comment,status,change_reference,\
            date_of_change,maker,business_date) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            res = self.db.transact(sql, (id, audit_info['table_name'], audit_info['change_type'], audit_info['comment'],
            'PENDING', audit_info['change_reference'], datetime.now(), audit_info['maker'],audit_info['business_date']))

        else:
            sql = "insert into  "+self.audit_table_name +" (id,table_name,change_type,maker_comment,status,change_reference,\
            date_of_change,maker) values(%s,%s,%s,%s,%s,%s,%s,%s)"
            res = self.db.transact(sql, (id, audit_info['table_name'], audit_info['change_type'], audit_info['comment'],
            'PENDING',audit_info['change_reference'],datetime.now(),audit_info['maker']))
            #print("this should be id of the record inserted..what it actually is ",res)
        self.update_approval_status(table_name=audit_info['table_name'],id=id, dml_allowed='N',in_use='N')
        self.db.commit()

        return res

    def reject_dml(self,data):
        if data["change_type"]=="INSERT":
            self.update_approval_status(table_name=data["table_name"],id=data["id"],dml_allowed='Y',in_use='N')
        elif data["change_type"]=="UPDATE":
            self.update_approval_status(table_name=data["table_name"], id=data["id"], dml_allowed='Y')
        elif data["change_type"]=="DELETE":
            self.update_approval_status(table_name=data["table_name"], id=data["id"], dml_allowed='Y')
        self.update_audit_record(data)
        return data

    def regress_dml(self,data):
        if data["change_type"]=="INSERT":
            self.update_approval_status(table_name=data["table_name"],id=data["id"],dml_allowed='Y',in_use='N')
        elif data["change_type"]=="UPDATE":
            self.update_approval_status(table_name=data["table_name"], id=data["id"], dml_allowed='Y')
        elif data["change_type"]=="DELETE":
            self.update_approval_status(table_name=data["table_name"], id=data["id"], dml_allowed='Y')
        self.update_audit_record(data)
        return data


    def approve_dml(self,data):
        print(data)
        if data["change_type"]=="INSERT":
            self.update_approval_status(table_name=data["table_name"],id=data["id"],dml_allowed='Y',in_use='Y')
        elif data["change_type"]=="DELETE":
            self.update_approval_status(table_name=data["table_name"], id=data["id"], dml_allowed='N',in_use='N')
        elif data["change_type"] == "UPDATE":
            sql="update "+data["table_name"]+" set "
            params=[]
            for col in data["update_info"]:
                sql+= col["field_name"]+"=%s,"
                params.append(col["new_val"])
            sql=sql[:-1]+" where id="+str(data["id"])
            print(sql,tuple(params))
            res=self.db.transact(sql,tuple(params))
            self.db.commit()
            self.update_approval_status(table_name=data["table_name"], id=data["id"], dml_allowed='Y',in_use='Y')

        self.update_audit_record(data)
        return data

    def get_audit_list(self,sql=None,sqlparams=None):
        if sql:
            sql = sql.format(self.audit_table_name)
            audit_list = None
            if sqlparams:
                audit_list = self.db.query(sql, sqlparams).fetchall()
            else:
                audit_list = self.db.query(sql).fetchall()
            for i,d in enumerate(audit_list):
                print('Processing index ',i)
                for k,v in d.items():
                    if isinstance(v,datetime):
                        d[k] = d[k].isoformat()
                        print(d[k], type(d[k]))

            for idx,item in enumerate(audit_list):
                if item["change_type"]=="UPDATE":
                    values=self.db.query("select field_name,old_val,new_val from " + self.audit_table_name + " where id="+str(item["id"])+
                                         " and table_name='"+str(item["table_name"])+"' and status='"+str(item["status"])+
                                         "' and date_of_change='"+str(item["date_of_change"])+"'").fetchall()
                    update_info_list=[]
                    for val in values:
                        update_info_list.append({"field_name":val["field_name"],"old_val":val["old_val"],"new_val":val["new_val"]})
                    audit_list[idx]["update_info"]=update_info_list

            return audit_list
