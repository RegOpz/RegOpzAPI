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

        if report_id:
            self.report_id = report_id
            return self.render_report_json()

        return self.report_template_catalog_list(country)

    def post(self):
        data = request.get_json(force=True)
        return self.dcc_master.insert_data(data)

    def put(self):
        data = request.get_json(force=True)
        id = data['audit_info']['id']
        if not id:
            return BUSINESS_RULE_EMPTY
        return self.dcc_master.update_or_delete_data(data, id)

    def copy_template_to_tenant(self,country,report_id,comment,overwrite=False):

        try:
            app.logger.info("Checking if report template already present in tenant space.")
            existing_record=self.tenant_db.query("select * from report_def_catalog where report_id=%s",(report_id,)).fetchone()
            if existing_record:
                components=existing_record['report_components'].split(',')
                template_present=('TEMPLATE'in components)
                if template_present and not overwrite:
                    return {"msg":"Template for report {} alreday exists.".format(report_id)},500

            app.logger.info("Fetching report template from master space.")
            report_template=self.master_db.query("select * from report_def_master where country=%s and report_id=%s",(country,report_id)).fetchall()

            # if template_present and overwrite:
            #    app.logger.info("Erasing existing report template.")
            #    self.tenant_db.transact("delete from report_def where report_id=%s",(report_id,))
            #    components.remove('TEMPLATE')
            #    components_list=",".join(components)
            #    self.tenant_db.transact("update report_def_catalog set report_components=%s where report_id=%s",(components_list,report_id))
            #
            # for rec in report_template:
            #     id=self.tenant_db.transact("insert into report_def(report_id,sheet_id,cell_id,cell_render_def,cell_calc_ref) values(%s,%s,%s,%s,%s)",
            #                                rec["report_id"],rec["sheet_id"],rec["cell_id"],rec["cell_render_def"],rec["cell_calc_ref"])
            #
            #     audit_info = {
            #         "table_name": "report_def",
            #         "change_type": "INSERT",
            #         "comment": comment,
            #         "change_reference": "Copying report template from master database",
            #         "maker": self.user_id
            #     }
            #
            #     app.logger.info("Inserting audit info")
            #     self.dcc_tenant.audit_insert({"audit_info": audit_info}, id)
            #     self.tenant_db.commit()

            return {"msg":"Template successfully copied into tenant space."},200

        except Exception as e:
            self.tenant_db.rollback()
            app.logger.error(str(e))
            return {"msg":str(e)},500


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
