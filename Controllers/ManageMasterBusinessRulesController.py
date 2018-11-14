from app import *
from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
from Controllers.DefChangeController import DefChangeController
import json
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *
import pandas as pd
from Models.Token import Token

class ManageMasterBusinessRulesController(Resource):

    def __init__(self):
        self.master_db=DatabaseHelper()
        self.dcc_master=DefChangeController(tenant_info="master")
        self.domain_info = autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.tenant_db=DatabaseHelper(self.tenant_info)
            self.dcc_tenant=DefChangeController(tenant_info=self.tenant_info)
            self.user_id=Token().authenticate()

    def get(self):
        if request.endpoint == "master_business_rules_ep":
            country = request.args.get('country')
            rule = request.args.get('rule')
            return self.fetch_repository_rules(country,rule)
        if request.endpoint == "master_business_rule_linkage_multiple_ep":
            country = request.args.get('country')
            rules = request.args.get('rules')
            #print(source,rules)
            return self.list_master_reports_for_rule_list(country=country,rules=rules)

    def put(self):
        br = request.get_json(force=True)
        if br['update_info']['id']:
            self.id = br['update_info']['id']
        res = self.dcc_master.update_or_delete_data(br, self.id)
        return res

    def post(self, source_id=None):
        br = request.get_json(force=True)
        if source_id and request.endpoint=="copy_business_rules_to_tenant_ep":
            br_list = br['rules']
            audit_info = br['audit_info']
            res = self.copy_to_tenant(br_list,source_id,audit_info)
            return res
        if request.endpoint == "master_business_rules_ep":
            res = self.dcc_master.insert_data(br)
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
            sql = "select business_rule,in_use, id,logical_condition,rule_description from business_rules " + \
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
                update_info={
                                'rule_execution_order':1,
                                'business_rule':row['business_rule'],
                                'rule_description':row['rule_description'],
                                'source_id':source_id
                            }
                audit_info_new = {
                                "table_name": "business_rules",
                                "change_type": "INSERT",
                                "comment": audit_info["audit_comment"],
                                "change_reference": "Copying business rule {} from master database".format(row['business_rule']),
                                "maker": audit_info["maker"],
                                "maker_tenant_id": audit_info["maker_tenant_id"],
                                "group_id": audit_info["group_id"],
                            }
                data = {
                          'table_name': "business_rules",
                          "change_type": "INSERT",
                          'update_info': update_info,
                          'audit_info': audit_info_new
                        }

                app.logger.info("Inserting audit info")
                self.dcc_tenant.insert_data(data)

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

    def list_master_reports_for_rule_list(self,country,rules):

        business_rule_list = rules.split(',')
        sql = "select rc.report_id,rc.sheet_id,rc.cell_id,rc.cell_business_rules,rc.in_use " + \
                " from report_calc_def_master rc, report_def_catalog_master rd" + \
                " where rc.report_id = rd.report_id " + \
                " and country='" + str(country) + "'"
        cur=self.master_db.query(sql)
        data_list = cur.fetchall()

        result_set = []
        if len(business_rule_list)==1 and business_rule_list[0] in ['undefined','null']:
            result_set = data_list
        else:
            for data in data_list:
                # print(data['cell_business_rules'])
                cell_rule_list = data['cell_business_rules'].split(',')
                # print(type(cell_rule_list))
                if set(business_rule_list).issubset(set(cell_rule_list)):
                    # print(data)
                    result_set.append(data)

        trans_result_set = self.list_master_trans_reports_for_rule_list(country, business_rule_list)
        for trans in trans_result_set:
            result_set.append(trans)

        print('data_list append result_set final {}'.format(result_set))
        return result_set

    def list_master_trans_reports_for_rule_list(self, country, business_rule_list):

        sql = "select rc.report_id,rc.sheet_id,rc.section_id,rc.cell_calc_render_ref,rc.in_use " + \
				" from report_dyn_trans_calc_def_master rc, report_def_catalog_master rd" + \
                " where rc.report_id = rd.report_id " + \
                " and country='" + str(country) + "'"

        cur=self.master_db.query(sql)
        trans_data_list = cur.fetchall()
        data_list=[]
        for data in trans_data_list:
            calc_rule_ref = json.loads(data['cell_calc_render_ref'])
            newdata = {
						'report_id': 	data['report_id'],
						'sheet_id': 	data['sheet_id'],
						'cell_id': 		data['section_id'],
						'cell_business_rules': calc_rule_ref['rule'],
						'in_use':		data['in_use']
					}
            data_list.append(newdata)
            print('data_list append {}'.format(data_list))

        result_set = []
        if len(business_rule_list)==1 and business_rule_list[0] in ['undefined','null']:
            result_set = data_list
        else:
            for data in data_list:
                # print(data['cell_business_rules'])
                cell_rule_list = data['cell_business_rules'].split(',')
                # print(type(cell_rule_list))
                if set(business_rule_list).issubset(set(cell_rule_list)):
                    # print(data)
                    result_set.append(data)

        print('data_list append result_set trans {}'.format(result_set))
        return result_set
