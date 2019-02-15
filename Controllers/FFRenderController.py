from app import *
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
from openpyxl.utils import column_index_from_string,get_column_letter,coordinate_from_string,coordinate_to_tuple
import Helpers.utils as util
import json
import ast
from operator import itemgetter
from datetime import datetime
import time
import math
import re
from Pipeline.PyDAG import DAG
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *
import pandas as pd


class FFRenderController(Resource):

    def __init__(self):
        self.domain_info = autheticateTenant()
        self.db_master=DatabaseHelper()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.db=DatabaseHelper(self.tenant_info)

    def get(self,report_id=None):
        if request.endpoint == 'view_free_formt_report_ep':
            self.report_id = report_id
            self.db_object_suffix= request.args.get('domain_type')
            reporting_date = request.args.get('reporting_date')
            version = request.args.get('version')
            report_parameters = request.args.get('report_parameters')
            report_snapshot = request.args.get('report_snapshot')
            # print(reporting_date)
            if not reporting_date:
                return self.render_template_json()



    def render_template_json(self):

        app.logger.info("Rendering free format report")
        try:
            def_object = "report_free_format_def"
            ref_object="report_free_format_ref"
            sec_object="report_free_format_section"

            if self.db_object_suffix:
                def_object += "_" + self.db_object_suffix
                ref_object+="_" + self.db_object_suffix
                sec_object+="_"+ self.db_object_suffix

            template_df = pd.DataFrame(self.db.query("select a.report_id,a.sheet_id,a.cell_id,a.col_id,a.row_id,a.cell_type,a.cell_ref,\
                                    b.entity_type,b.entity_ref,b.content_type,b.content from {0} a,\
                                    {1} b where a.report_id=%sand a.report_id=b.report_id\
                                    and a.sheet_id=b.sheet_id and a.cell_ref=b.entity_ref".format(ref_object,def_object),(self.report_id,)).fetchall())
            hw_df=pd.DataFrame(self.db.query("select report_id,sheet_id,entity_type,entity_ref,content_type,content from {}\
                                     where report_id=%s and ((entity_type='ROW'and content_type='ROW_HEIGHT')\
                                    or (entity_type='COL' and content_type='COLUMN_WIDTH'))".format(def_object),(self.report_id,)).fetchall())
            sec_df=pd.DataFrame(self.db.query("select sheet_id,section_id,section_type,section_range,section_ht_range,section_dependency\
                                     from {} where report_id=%s".format(sec_object),(self.report_id,)).fetchall())

            sheet_content=[]
            for sheet in template_df['sheet_id'].unique():

                #Get row heights
                row_attr=hw_df.loc[(hw_df['sheet_id']==sheet) & (hw_df['entity_type']=='ROW') & (hw_df['content_type']=='ROW_HEIGHT')].to_dict('records')
                row_heights = [None] * len(row_attr)
                for row in row_attr:
                    if row['content'] == "None" or int(float(row['content'])) < 25:
                        row_heights[int(row['entity_ref']) - 1] = 25
                    else:
                        row_heights[int(row['entity_ref']) - 1] = int(float(row['content']))


                #Get column widths
                col_attr = hw_df.loc[(hw_df['sheet_id']==sheet) & (hw_df['entity_type']=='COL') & (hw_df['content_type']=='COLUMN_WIDTH')].to_dict('records')
                col_widths = [None] * len(col_attr)
                for col in col_attr:
                    # Note that column index from openpyxl utils starts at 1, but array starts at 0
                    # col width multiplier is 8 for rendering properly in UX
                    # app.logger.info("col {}".format(col))
                    if col['content'] == "None" or int(float(col['content']) * 8) < 90:
                        col_widths[column_index_from_string(col['entity_ref']) - 1] = 90
                    else:
                        col_widths[column_index_from_string(col['entity_ref']) - 1] = int(float(col['content']) * 8)

                if not sec_df.empty:
                    sec_attr=sec_df.loc[sec_df['sheet_id']==sheet].to_dict('records')
                else:
                    sec_attr={}

                section_details=[]
                # {"ht_range": [0, 0, 0, 1], "range": "A1:B1", "section_id": "s1", "section_position": [], "section_type": "FIXEDFORMAT"}
                for sec in sec_attr:
                    section_details.append({'ht_range':sec['section_ht_range'].split(','),'range':sec['section_range'],'section_id':sec['section_id'],
                           'section_position':sec['section_dependency'].split(','),'section_type':sec['section_type']})




                data=[[None]*len(col_attr) for row in range(len(row_attr))]
                merged_cells=[]
                sheet_styles = {'style_classes': {}, 'td_styles': []}

                sheet_template=template_df[template_df['sheet_id']==sheet].to_dict('records')

                for t in sheet_template:

                    if t['cell_type']=='MERGED':
                        cell = t['cell_id'].split(':')
                        start_xy = coordinate_from_string(cell[0])
                        # note that openpyxls util provides visual coordinates, but array elements starts with 0
                        start_row = start_xy[1] - 1
                        start_col = column_index_from_string(start_xy[0]) - 1
                        end_xy = coordinate_from_string(cell[1])
                        end_row = end_xy[1] - 1
                        end_col = column_index_from_string(end_xy[0]) - 1
                        merged_cells.append({'row': start_row, 'col': start_col, 'rowspan': end_row - start_row + 1,'colspan': end_col - start_col+1})

                    elif t['cell_type']=='SINGLE':
                        start_xy = coordinate_from_string(t['cell_id'])
                        start_row=start_xy[1]-1
                        start_col=column_index_from_string(start_xy[0]) - 1

                    if t['content_type'] in ('BLANK','DATA',''):
                        data[start_row][start_col] = None
                    elif t['content_type']=='STATIC_TEXT':
                        data[start_row][start_col]=t['content']
                    elif t['content_type'] == 'CELL_STYLE':
                        td_style = json.loads(t['content'])
                        td_class_name = {'classes': ''}
                        util.process_td_class_names(td_style, td_class_name, sheet_styles)
                        sheet_styles['td_styles'].append({'row': start_row, 'col': start_col, 'class_name': td_class_name['classes']})

                sheet_d = {}
                sheet_d['sheet'] = sheet
                sheet_d['sheet_styles'] = sheet_styles
                sheet_d['row_heights'] = row_heights
                sheet_d['col_widths'] = col_widths
                sheet_d['data'] = data
                sheet_d['merged_cells'] = merged_cells
                sheet_d['sections'] = section_details
                sheet_content.append(sheet_d)

            return sheet_content

        except Exception as e:
            app.logger.error(str(e))
            raise












