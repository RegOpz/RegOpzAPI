from app import *
from flask import Flask, jsonify, request
from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
from Controllers.DefChangeController import DefChangeController
from datetime import datetime
from decimal import Decimal
import json
import ast
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *
import pandas as pd
from Models.Token import Token


class ManageMasterReportController(Resource):

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

    def get(self,country=None, report_id=None):
        if request.endpoint == 'repository_drill_down_rule_ep':
            self.report_id = request.args.get('report_id')
            sheet_id = request.args.get('sheet_id')
            cell_id = request.args.get('cell_id')
            return self.cell_drill_down_rules(report_id=self.report_id,sheet_id=sheet_id,cell_id=cell_id)
        if request.endpoint == "repository_report_rule_audit_ep":
            self.report_id = request.args.get('report_id')
            sheet_id = request.args.get('sheet_id')
            cell_id = request.args.get('cell_id')
            return self.get_report_audit_list(report_id=self.report_id, sheet_id=sheet_id, cell_id=cell_id)

        if request.endpoint == "master_report_business_rules_ep":
            report_id = request.args.get('report_id')
            sheet_id = request.args.get('sheet_id')
            cell_id = request.args.get('cell_id')
            return self.master_report_cell_business_rules(report_id=report_id,sheet_id=sheet_id,cell_id=cell_id)
        if report_id:
            self.report_id = report_id
            return self.render_report_json()
        if request.endpoint == 'fetch-report-id':
            self.report_id=request.args.get("report_id")
            country=request.args.get("country")
            return self.check_report_id(report_id=self.report_id, country=country)

        return self.report_template_catalog_list(country)

    def post(self):
        data = request.get_json(force=True)
        if request.endpoint == 'repository_report_rule_ep':
            return self.dcc_master.insert_data(data)

        country = data["country"]
        report_description = data["report_description"]
        report_type = data["report_type"]
        ref_report_id = data["ref_report_id"]
        target_report_id = data["target_report_id"]
        ref_domain = data["ref_domain"]
        target_domain = data["target_domain"]
        target_group_id = data["target_groupId"]
        table_list = data['table_list']
        return self.copy_template_to_tenant(country=country, report_type=report_type, ref_report_id=ref_report_id,
                                            target_report_id=target_report_id, ref_domain=ref_domain,
                                            target_domain=target_domain,
                                            target_group_id=target_group_id, report_description=report_description,
                                            table_list=table_list)

    def put(self,report=None):
        data = request.get_json(force=True)
        if request.endpoint == 'repository_report_rule_ep':
            id = data['audit_info']['id']
            if not id:
                return BUSINESS_RULE_EMPTY
            return self.dcc_master.update_or_delete_data(data, id)
        if request.endpoint == 'edit_repository_report_parameter_ep':
            return self.update_report_parameters(data, report)

    def check_report_id(self,report_id,country):
        app.logger.info("Checking if report template already present in tenant space.")
        existing_record = self.tenant_db.query("select * from report_def_catalog where report_id=%s and country=%s",
                                               (report_id, country)).fetchone()
        if existing_record:
            return {"msg": "Target report id name already exists create a new report id"},500
        app.logger.info("OK new entry for copy")
        return {"msg": "report-id is available"},200

    def copy_def_master_table(self,ref_table, target_table,ref_report_id,target_report_id,review_columns=True):
        try:
            app.logger.info("Def Data copy into tenant started from {0} into {1}".format(ref_table,target_table,))
            exclusion_list = ['id','report_id','in_use','dml_allowed']
            table_dict = self.tenant_db.query("describe " + target_table).fetchall()
            data_dict_tenant = [ r['Field'] for r in table_dict if r['Field'] not in exclusion_list]
            app.logger.info("Data dict tenant {}".format(data_dict_tenant,))
            table_dict = self.master_db.query("describe " + ref_table).fetchall()
            data_dict_master = [ r['Field'] for r in table_dict if r['Field'] in data_dict_tenant]
            app.logger.info("Data dict master {}".format(data_dict_tenant,))
            columns = ",".join(data_dict_master)
            if review_columns:
                columns += ",'Y' as dml_allowed, 'N' as in_use"

            app.logger.info("columns {}".format(columns,))
            data_master = self.master_db.query("select " + columns + ",%s as report_id " + \
                            " from " + ref_table + " where report_id=%s " + \
                            (" and in_use='Y'" if review_columns else ""),(target_report_id,ref_report_id,)).fetchall()
            if data_master:
                data_tenant = [tuple(x.values()) for x in data_master]
                columns = ",".join(data_dict_master)
                if review_columns:
                    columns += ",dml_allowed,in_use"
                    placeholders = ",".join(['%s'] * (len(data_dict_master) + 3))
                else:
                    placeholders = ",".join(['%s'] * (len(data_dict_master) + 1))
                self.tenant_db.transactmany("insert into " + target_table + "(" + columns + ",report_id)" + \
                                " values(" + placeholders + ")",data_tenant)
            app.logger.info("Def Data copy into tenant completed from {0} into {1}".format(ref_table,target_table,))

        except Exception as e:
            raise(e)

    def copy_template_to_tenant(self, country, target_report_id, ref_report_id, report_type, target_group_id,
                                ref_domain, target_domain, report_description, table_list):

        try:

            # copy entry of report_def_catalog_master to report_def_catalog
            report_master = self.master_db.query(
                "select id,report_parameters from report_def_catalog_master where report_id=%s and country = %s",
                (ref_report_id,country,)).fetchone()
            if report_master["report_parameters"]:
                report_parameters = json.loads(report_master["report_parameters"])
            else:
                report_parameters = {}
            if 'report_id' in report_parameters.keys():
                report_parameters['report_id'] = target_report_id
            def_id = self.tenant_db.transact("insert into report_def_catalog(report_id,country,report_description,report_parameters,\
                                        last_updated_by,date_of_change,report_type) values(%s,%s,%s,%s,%s,%s,%s)",
                                          (target_report_id, country, report_description, json.dumps(report_parameters), None, None,
                                           report_type))

            app.logger.info("Copied report_def_catalog_master")
            # print('id to be inserted is: ',id)
            # copy entry of report_def_master to report_def
            for t in table_list:
                self.copy_def_master_table(ref_table=t['ref_table'],target_table=t['target_table'],
                                            ref_report_id=ref_report_id,target_report_id=target_report_id,
                                            review_columns=t['review_columns'])

            # post a notification in ManageDef Change Controller
            tenant_id=str(json.loads(self.domain_info)["tenant_id"])
            #print(tenant_id)
            maker=target_group_id.split(tenant_id)[0]
            #print(maker)
            change_ref="Copy of report-id:{} to target-report-id:{}".format(ref_report_id,target_report_id)
            data = {
                "audit_info": {"table_name": "report_def_catalog", "change_type": "COPY", "comment": report_description,
                               "change_reference":change_ref, "maker": maker,
                               "maker_tenant_id":tenant_id, "group_id": target_group_id}}
            self.dcc_tenant.audit_insert(data, def_id,self.tenant_db)
            app.logger.info("Copy completed")
            self.tenant_db.commit()
            return {"msg":"Template successfully copied into tenant space."},200

        except Exception as e:
            self.tenant_db.rollback()
            app.logger.error(str(e))
            return {"msg": str(e)}, 500


    def report_template_catalog_list(self,country='ALL'):

        try:
            data_dict=[]
            where_clause = ''

            sql = "select distinct country from report_def_catalog_master where 1 "
            if country is not None and country !='ALL':
                 where_clause =  " and country = '{}'".format(country.upper())

            country = self.master_db.query(sql + where_clause).fetchall()

            if country:
                for i,c in enumerate(country):
                    sql = "select * from report_def_catalog_master where country = %s"
                    report = self.master_db.query(sql,(c['country'],)).fetchall()
                    data_dict.append({'country': c['country'], 'report': report})

            return data_dict

        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500

    def render_report_json(self):

        app.logger.info("Rendering repository report")

        try:
            app.logger.info("Getting list of sheet for report {0}".format(self.report_id))
            sheets = self.master_db.query("select distinct sheet_id from report_def_master where report_id=%s",
                                   (self.report_id,)).fetchall()

            sheet_d_list = []
            for sheet in sheets:
                matrix_list = []
                row_attr = {}
                col_attr = {}
                cell_style = {}
                app.logger.info("Getting report repository definition for report {0},sheet {1}".format(self.report_id,sheet["sheet_id"]))
                report_template = self.master_db.query(
                    "select cell_id,cell_render_def,cell_calc_ref from report_def_master where report_id=%s and sheet_id=%s",
                    (self.report_id, sheet["sheet_id"])).fetchall()

                app.logger.info("Writing report definition to dictionary")
                for row in report_template:
                    cell_d = {}
                    if row["cell_render_def"] == 'STATIC_TEXT':
                        cell_d['cell'] = row['cell_id']
                        cell_d['value'] = row['cell_calc_ref']
                        cell_d['origin'] = "TEMPLATE"
                        matrix_list.append(cell_d)


                    elif row['cell_render_def'] == 'MERGED_CELL':
                        start_cell, end_cell = row['cell_id'].split(':')
                        cell_d['cell'] = start_cell
                        cell_d['value'] = row['cell_calc_ref']
                        cell_d['merged'] = end_cell
                        cell_d['origin'] = "TEMPLATE"
                        matrix_list.append(cell_d)


                    elif row['cell_render_def'] == 'ROW_HEIGHT':
                        if row['cell_calc_ref'] == 'None':
                            row_height = '12.5'
                        else:
                            row_height = row['cell_calc_ref']
                        row_attr[row['cell_id']] = {'height': row_height}


                    elif row['cell_render_def'] == 'COLUMN_WIDTH':
                        if row['cell_calc_ref'] == 'None':
                            col_width = '13.88'
                        else:
                            col_width = row['cell_calc_ref']
                        col_attr[row['cell_id']] = {'width': col_width}

                    elif row['cell_render_def'] == 'CELL_STYLE':
                        if ':' in row['cell_id']:
                            start_cell, end_cell = row['cell_id'].split(':')
                        else:
                            start_cell=row['cell_id']

                        app.logger.info("Inside CELL_STYLE for cell {}".format(start_cell,))
                        cell_style[start_cell] = eval(row['cell_calc_ref'])

                comp_agg_def = self.master_db.query("select cell_id,cell_render_def,cell_calc_ref from report_def_master where \
                report_id=%s and sheet_id=%s and cell_render_def='COMP_AGG_REF'",
                                             (self.report_id, sheet["sheet_id"])).fetchall()

                for row in comp_agg_def:
                    cell_d = {}
                    if ':' in row['cell_id']:
                        start_cell, end_cell = row['cell_id'].split(':')
                    else:
                        start_cell=row['cell_id']

                    current_cell_exists=next((item for item in matrix_list if item['cell']==start_cell),False)
                    if not current_cell_exists:
                        cell_d['cell'] = start_cell
                        cell_d['value'] = '' #row['cell_calc_ref']
                        cell_d['origin'] = "TEMPLATE"
                        #print(cell_d)
                        matrix_list.append(cell_d)

                sheet_d = {}
                sheet_d['sheet'] = sheet['sheet_id']
                # print(sheet_d['sheet'])
                sheet_d['matrix'] = matrix_list
                sheet_d['row_attr'] = row_attr
                sheet_d['col_attr'] = col_attr
                sheet_d['cell_style'] = cell_style
                sheet_d_list.append(sheet_d)


            json_dump = (sheet_d_list)
            # print(json_dump)
            return json_dump
        except Exception as e:
            app.logger.error(e)
            return {"msg": e}, 500

    def cell_drill_down_rules(self,report_id,sheet_id,cell_id):

        try:
            app.logger.info("Cell drill down rules for report {} sheet {} cell {}".format(report_id,sheet_id,cell_id))
            sql="select cell_calc_ref from report_def_master where report_id=%s and sheet_id=%s and (cell_id=%s or cell_id like %s) and cell_render_def='COMP_AGG_REF'"
            comp_agg_ref=self.master_db.query(sql,(report_id,sheet_id,cell_id,cell_id+":%")).fetchone()

            sql="select * from report_comp_agg_def_master where report_id=%s and sheet_id=%s and cell_id=%s"

            cell_calc_ref_list = ''
            comp_agg_rules=self.master_db.query(sql,(report_id,sheet_id,cell_id)).fetchall()
            app.logger.info("cell comp_agg_rules {}".format(comp_agg_rules,))
            if comp_agg_rules:
                formula = comp_agg_rules[0]['comp_agg_rule'].replace('.','')
                app.logger.info("cell formula {}".format(formula,))
                variables = list(set([node.id for node in ast.walk(ast.parse(formula)) if isinstance(node, ast.Name)]))
                app.logger.info("cell variables {}".format(variables,))
                cell_calc_ref_list = ','.join(variables)

            agg_rules=[]

            sql = "select  * from report_calc_def_master where \
                report_id=%s and sheet_id=%s and cell_id=%s"
            if cell_calc_ref_list != '':
                sql += " union select  * from report_calc_def_master where \
                    report_id=%s and cell_calc_ref in (%s)"
                app.logger.info("cell calc ref list sql {}".format(sql,))
                cell_rules = self.master_db.query(sql, (report_id, sheet_id, cell_id, report_id, cell_calc_ref_list)).fetchall()
            else:
                app.logger.info("else part of cell calc ref list sql {}".format(sql,))
                cell_rules = self.master_db.query(sql, (report_id, sheet_id, cell_id)).fetchall()


            #print(sql)


            for i,c in enumerate(cell_rules):
                print('Processing index ',i)
                for k,v in c.items():
                    if isinstance(v,Decimal):
                        c[k] = str(c[k])
                    if isinstance(v,datetime):
                        c[k] = c[k].isoformat()
                        #print(c[k], type(c[k]))

            for i,c in enumerate(comp_agg_rules):
                print('Processing index ',i)
                for k,v in c.items():
                    if isinstance(v,Decimal):
                        c[k] = str(c[k])
                    if isinstance(v,datetime):
                        c[k] = c[k].isoformat()

            display_dict={}

            display_dict['comp_agg_ref']=comp_agg_ref['cell_calc_ref'] if comp_agg_ref else ''
            display_dict['comp_agg_rules']=comp_agg_rules
            display_dict['agg_rules']=agg_rules
            display_dict['cell_rules']=cell_rules

            return display_dict
        except Exception as e:
            app.logger.error(str(e))
            return {"msg": str(e)}, 500

    def get_report_audit_list(self, report_id=None, sheet_id=None, cell_id=None):
        app.logger.info("Getting repository report audit list")
        try:
            audit_list=[]
            sql = "SELECT id FROM {0} WHERE 1 "
            if report_id:
                sql += " AND report_id='{}' ".format(report_id,)
                if sheet_id:
                    sql += " AND sheet_id='{}'".format(sheet_id,)
                if cell_id:
                    sql += " AND cell_id='{}'".format(cell_id)
                app.logger.info("SQL for check {}".format(sql.format('report_calc_def_master',)))
                calc_id_list = self.master_db.query(sql.format('report_calc_def_master',)).fetchall()
                if calc_id_list:
                    calc_id_list = ",".join(map(str,[id['id'] for id in calc_id_list]))
                else:
                    calc_id_list = "-99999999"
                audit_list+=self.dcc_master.get_audit_history(id_list=calc_id_list,table_name='report_calc_def_master')

                agg_id_list = self.master_db.query(sql.format('report_comp_agg_def_master',)).fetchall()
                if agg_id_list:
                    agg_id_list = ",".join(map(str,[id['id'] for id in agg_id_list]))
                else:
                    agg_id_list = "-99999999"
                audit_list+=self.dcc_master.get_audit_history(id_list=agg_id_list,table_name='report_comp_agg_def_master')
                return audit_list
        except Exception as e:
            app.logger.error(e)
            return {"msg": e}, 500

    def master_report_cell_business_rules(self,report_id,sheet_id,cell_id):

        additional_filter = ''
        comp_agg_ref = None
        if sheet_id != 'undefined' and cell_id != 'undefined':
            additional_filter = " and sheet_id=%s and cell_id=%s "
            # Now get the comp agg ref to calculate the next sequence number for the cell calc ref
            sql="select cell_calc_ref from report_def_master where report_id=%s and sheet_id=%s and (cell_id=%s or cell_id like %s) and cell_render_def='COMP_AGG_REF'"
            comp_agg_ref=self.master_db.query(sql,(report_id,sheet_id,cell_id,cell_id+":%")).fetchone()

        calc_sql = "select  a.* from report_calc_def_master a " + \
            " where a.report_id=%s" + additional_filter

        if additional_filter=='':
            calc_parameters=(report_id,)
        else:
            calc_parameters=(report_id, sheet_id, cell_id,)

        # print("sql {0} \n parameters {1}".format(calc_sql,calc_parameters))
        cell_rules = self.master_db.query(calc_sql, calc_parameters).fetchall()

        agg_sql="select * from report_comp_agg_def_master where report_id=%s " + additional_filter

        if additional_filter =='':
            parameters=(report_id,)
        else:
            parameters=(report_id,sheet_id,cell_id)

        # print("Agg sql {0} \n parameters {1}".format(agg_sql,parameters))
        comp_agg_rules=self.master_db.query(agg_sql,parameters).fetchall()

        display_dict={}

        display_dict['comp_agg_ref']=comp_agg_ref['cell_calc_ref'] if comp_agg_ref else ''
        display_dict['comp_agg_rules']=comp_agg_rules
        display_dict['agg_rules']=[]
        display_dict['cell_rules']=cell_rules
        display_dict['report_snapshot'] = {}

        return display_dict

    def update_report_parameters(self,data,report):
        app.logger.info("Updating Master Report Parameters in Report Def Catalog {}".format(data,))
        sql = "update report_def_catalog_master set report_parameters='{0}' where report_id='{1}'"
        sql = sql.format(json.dumps(data),report)
        try:
            self.master_db.transact(sql)
            self.master_db.commit()
            return {"msg": "Repository Report parameters updated successfully for {0}".format(report,)}, 200
        except Exception as e:
            return {"msg": "Repository Report parameters update error : " + str(e)},500
