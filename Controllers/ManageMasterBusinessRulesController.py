from app import *
from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
from Helpers.DatabaseOps import DatabaseOps
from Helpers.AuditHelper import AuditHelper
import json
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *
import pandas as pd
from Helpers.AuditHelper import AuditHelper
from Models.Token import Token

class ManageMasterBusinessRulesController(Resource):

    def __init__(self):
        self.master_db=DatabaseHelper()
        self.dbOps_master=DatabaseOps(audit_table_name='def_change_log',tenant_info=None)
        self.domain_info = autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.tenant_db=DatabaseHelper(self.tenant_info)
            self.tenant_audit=AuditHelper('def_change_log',self.tenant_info)
            self.user_id=Token().authenticate()

    def get(self):
        country = request.args.get('country')
        rule = request.args.get('rule')
        return self.fetch_repository_rules(country,rule)

    def put(self):
        br = request.get_json(force=True)
        if br['update_info']['id']:
            self.id = br['update_info']['id']
        res = self.dbOps_master.update_or_delete_data(br, self.id)
        return res

    def post(self, source_id=None):
        br = request.get_json(force=True)
        if source_id:
            br_list = br['rules']
            audit_info = br['audit_info']
            res = self.copy_to_tenant(br_list,source_id,audit_info)
        else:
            res = self.dbOps_master.insert_data(br)
        return res

    def fetch_repository_rules(self,country,rule):
        app.logger.info("Fetching repository business rules from master for country {} rule {}".format(country,rule))
        try:
            business_rules = {}
            sql = "select * from business_rules_master where 1=1 "
            sqlparams=[]
            if country:
                sql += "and country=%s "
                sqlparams.append(country)

            if rule:
                sql += "and business_rule=%s "
                sqlparams.append(rule)

            cur=self.master_db.query(sql,sqlparams)
            rules=cur.fetchall()
            cols = [i[0] for i in cur.description]
            business_rules['cols'] = cols

            # for br in business_rules:
            # 	business_rules_list.append(br)
            business_rules['rows'] = rules
            return business_rules
        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500

    def fetch_from_master(self,country,business_rules_list=None):
        app.logger.info("Fetching business rules from master for country {}".format(country))
        try:
            if not business_rules_list:
                business_rules=self.master_db.query("select * from business_rules_master where country=%s and in_use='Y'",(country,)).fetchall()
            else:
                business_rules=self.master_db.query("select * from business_rules_master where country=%s and in_use='Y' and business_rule in (%s)",(country,",".join(business_rules_list))).fetchall()
            return business_rules
        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500

    def copy_to_tenant(self,business_rules_list,source_id,audit_info):
        app.logger.info("Copying business rules to tenant")
        try:
            business_rules=pd.DataFrame(business_rules_list)
            business_rules['rule_string']="'" + business_rules['business_rule'] + "'"
            sql = "select business_rule,in_use, id,logical_condition from business_rules " + \
                  "where source_id={0} and business_rule in ({1})" \
                  .format(source_id,",".join(business_rules["rule_string"].tolist()))
            existing_rules=self.tenant_db.query(sql).fetchall()
            if existing_rules:
                existing_business_rules=pd.DataFrame(existing_rules)
                existing_business_rules['exists']='Y'
                app.logger.info("existing_business_rules {}".format(existing_business_rules,))
                business_rules=pd.merge(business_rules,existing_business_rules,how='left',on='business_rule',suffixes=('','_tenant'))
                business_rules['exists'].fillna('N',inplace=True)
            else:
                business_rules['exists']='N'

            new_business_rules=business_rules.loc[business_rules['exists']=='N']
            # app.logger.info("new_business_rules {}".format(new_business_rules,))

            for index,row in new_business_rules.iterrows():
                app.logger.info("Inserting business rule to tenant table")
                id=self.tenant_db.transact("insert into business_rules(rule_execution_order,business_rule,rule_description,source_id)\
                                                        values(%s,%s,%s,%s)", (1,row['business_rule'],row['rule_description'],source_id))
                self.tenant_db.commit()
                # app.logger.info("Preparing audit info {}".format(audit_info))
                audit_info_new = {
                    "table_name": "business_rules",
                    "change_type": "INSERT",
                    "comment": audit_info["audit_comment"],
                    "change_reference": "Copying business rule {} from master database".format(row['business_rule']),
                    "maker": audit_info["maker"],
                    "maker_tenant_id": audit_info["maker_tenant_id"],
                    "group_id": audit_info["group_id"]
                }

                app.logger.info("Inserting audit info")
                self.tenant_audit.audit_insert({"audit_info":audit_info_new},id)
            self.tenant_db.commit()

            # fillna with proper values to avoid error while processing to_dict, else it sends wrong
            # JSON formatted string!!!!!
            sucessfully_copied_df=business_rules.loc[business_rules['exists']=='N']
            if existing_rules:
                sucessfully_copied_df['logical_condition'].fillna('Not Applicable',inplace=True)
                sucessfully_copied_df['in_use_tenant'].fillna('N',inplace=True)
                sucessfully_copied_df['id_tenant'].fillna(0,inplace=True)

            sucessfully_copied=sucessfully_copied_df.to_dict(orient='records')
            not_copied=business_rules.loc[business_rules['exists']=='Y'].to_dict(orient='records')

            return {"successfully_copied":sucessfully_copied,"not_copied":not_copied},200
        except Exception as e:
            self.tenant_db.rollback()
            app.logger.error(str(e))
            return {"msg":str(e)},500
