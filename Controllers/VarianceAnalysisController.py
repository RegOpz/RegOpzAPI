from flask_restful import Resource,abort
import os
from flask import Flask, request, redirect, url_for
from werkzeug.utils import secure_filename
import uuid
from Constants.Status import *
from Helpers.DatabaseHelper import DatabaseHelper
from Models.Document import Document
import datetime
import openpyxl as xls
import Helpers.utils as util
from openpyxl.styles import Border, Side, PatternFill, Font, GradientFill, Alignment, Protection
from openpyxl.utils import get_column_letter
import Helpers.utils as util
import json
import ast
from operator import itemgetter
from datetime import datetime

class VarianceAnalysisController(Resource):
    def get(self):
        if request.endpoint=='get_variance_country_suggestion_list':
            return self.get_country_suggestion_list()

        if request.endpoint=='get_variance_report_suggestion_list':
            country=request.args.get('country') if request.args.get('country') != None else 'ALL'
            return self.get_report_suggestion_list(country)

        if request.endpoint == 'get_variance_date_suggestion_list':
            country=request.args.get('country') if request.args.get('country') != None else 'ALL'
            report_id=request.args.get('report_id') if request.args.get('report_id') !=None else 'ALL'
            excluded_date=request.args.get('excluded_date')
            return self.get_date_suggestion_list(country,report_id,excluded_date)
        if request.endpoint == 'get_variance_report':
            report_id=request.args.get('report_id')
            first_reporting_date=request.args.get('first_date')
            subsequent_reporting_date=request.args.get('subsequent_date')
            return self.get_variance_report(report_id,first_reporting_date,subsequent_reporting_date)

    def get_country_suggestion_list(self):
        db=DatabaseHelper()

        country_list=db.query("select distinct country from report_catalog").fetchall()

        if not country_list:
            return []
        else:
            return country_list

    def get_report_suggestion_list(self,country):
        db=DatabaseHelper()

        where_clause=''
        if country!='ALL':
            where_clause=" where country= '"+country+"'"

        report_list=db.query("select distinct report_id from report_catalog " + where_clause).fetchall()

        if not report_list:
            return []
        else:
            return report_list

    def get_date_suggestion_list(self,country,report_id,excluded_date=None):
        db=DatabaseHelper()


        where_clause=" where 1=1 "
        if country!="ALL":
            where_clause+=" and country='"+country+"'"
        if report_id!="ALL":
            where_clause+=" and report_id='"+report_id+"'"
        if excluded_date:
            where_clause+=" and  reporting_date != '"+excluded_date +"'"

        sql="select as_of_reporting_date,reporting_date,report_create_date from report_catalog " +where_clause

        date_list=db.query(sql).fetchall()

        if not date_list:
            return []
        else:
            return date_list

    def get_variance_report(self,report_id, first_reporting_date,subsequent_reporting_date, cell_format_yn='Y'):

        db = DatabaseHelper()

        sheets = db.query("select distinct sheet_id from report_def where report_id=%s", (report_id,)).fetchall()


        sheet_d_list = []
        for sheet in sheets:
            matrix_list = []
            row_attr = {}
            col_attr = {}

            report_template = db.query(
                "select cell_id,cell_render_def,cell_calc_ref from report_def where report_id=%s and sheet_id=%s",
                (report_id, sheet["sheet_id"])).fetchall()

            for row in report_template:
                cell_d = {}
                if row["cell_render_def"] == 'STATIC_TEXT':
                    cell_d['cell'] = row['cell_id']
                    cell_d['type']='STATIC_TEXT'
                    cell_d['value'] = row['cell_calc_ref']
                    matrix_list.append(cell_d)


                elif row['cell_render_def'] == 'MERGED_CELL':
                    start_cell, end_cell = row['cell_id'].split(':')
                    cell_d['cell'] = start_cell
                    cell_d['type']='STATIC_TEXT'
                    cell_d['value'] = row['cell_calc_ref']
                    cell_d['merged'] = end_cell
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

            data = db.query('select b.report_id,b.sheet_id,b.cell_id,a.cell_summary,\
                                            b.reporting_scale,b.rounding_option,a.reporting_date \
                                            from report_comp_agg_def b left join report_summary a\
                                            on a.report_id=b.report_id and\
                                            a.sheet_id=b.sheet_id and \
                                            a.cell_id=b.cell_id \
                                            where b.report_id=%s \
                                            and b.sheet_id=%s\
                                            and a.reporting_date in(%s,%s)\
                                            order by b.report_id,b.sheet_id,b.cell_id',
                            (report_id, sheet["sheet_id"],first_reporting_date,subsequent_reporting_date)).fetchall()

            for row in data:
                cell_d={}
                if cell_format_yn == 'Y':
                    # print(row["cell_id"],row["cell_summary"])
                    cell_summary = util.round_value(
                        float(util.if_null_zero(row["cell_summary"])) / float(row["reporting_scale"]),
                        row["rounding_option"])

                else:
                    cell__summary= float(util.if_null_zero(row["cell_summary"]))

                idx = list(map(itemgetter('cell'), matrix_list)).index(row['cell_id']) \
                    if row['cell_id'] in map(itemgetter('cell'), matrix_list) else None

                if idx==None:
                    cell_d['cell']=row['cell_id']
                    cell_d['type']='DATA_VALUE'
                    cell_d['value']={str(row['reporting_date']):cell_summary}
                    #print(cell_d)
                    matrix_list.append(cell_d)
                else:
                    matrix_list[idx]['value'][str(row['reporting_date'])]=cell_summary
                    #print(idx,row['reporting_date'],matrix_list[idx]['value'])

                    if matrix_list[idx]['value'][first_reporting_date]==0 :
                        matrix_list[idx]['value'][first_reporting_date] = 1e-15

                    if matrix_list[idx]['value'][subsequent_reporting_date] == 0:
                        matrix_list[idx]['value'][subsequent_reporting_date] = 1e-15

                    matrix_list[idx]['pct']=util.round_value((matrix_list[idx]['value'][subsequent_reporting_date]/
                                                     matrix_list[idx]['value'][first_reporting_date] -1)*100,row['rounding_option'])



            sheet_d = {}
            sheet_d['sheet'] = sheet['sheet_id']
            # print(sheet_d['sheet'])
            sheet_d['matrix'] = matrix_list
            sheet_d['row_attr'] = row_attr
            sheet_d['col_attr'] = col_attr
            sheet_d_list.append(sheet_d)

        return sheet_d_list
