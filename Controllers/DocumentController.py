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
import json
UPLOAD_FOLDER = './uploads/templates'
ALLOWED_EXTENSIONS = set(['txt', 'xls', 'xlsx'])
class DocumentController(Resource):
    def allowed_file(self,filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
    def get(self, doc_id=None):
        """document = Document()
        if doc_id != None:
            return (document.get(doc_id))
        return (document.get())"""
        self.report_id = "MAS1003"
        return self.render_report_template_json()
    def post(self):
        if 'file' not in request.files:
            return NO_FILE_SELECTED
        self.report_id = request.form.get('report_id')
        if self.report_id == None or self.report_id == "":
            return REPORT_ID_EMPTY
        file = request.files['file']
        if file and not self.allowed_file(file.filename):
            return FILE_TYPE_IS_NOT_ALLOWED
        filename = str(uuid.uuid4()) + "_" + secure_filename(file.filename)
        file.save(os.path.join(UPLOAD_FOLDER, filename))
        document = Document({
            'id': None,
            'file': filename,
            'uploaded_by': 1,
            'time_stamp': str (datetime.datetime.utcnow()),
            'ip': '1.1.1.1',
            'comment': "Sample comment by model"
        })
        self.load_report_template(filename)
        return self.render_report_template_json()
    def load_report_template(self,template_file_name):
        formula_dict = {'SUM': 'CALCULATE_FORMULA',
                        '=SUM': 'CALCULATE_FORMULA',
                        }
        cell_render_ref = None
        target_dir = UPLOAD_FOLDER + "/"
        wb = xls.load_workbook(target_dir + template_file_name)

        db = DatabaseHelper()

        for sheet in wb.worksheets:

            db.transact('delete from report_def where report_id=%s and sheet_id=%s', (self.report_id, sheet.title,))

            # First capture the dimensions of the cells of the sheet
            rowHeights = [sheet.row_dimensions[r + 1].height for r in range(sheet.max_row)]
            colWidths = [sheet.column_dimensions[get_column_letter(c + 1)].width for c in range(sheet.max_column)]

            for row, height in enumerate(rowHeights):
                db.transact('insert into report_def(report_id,sheet_id,cell_id,cell_render_def,cell_calc_ref)\
                             values(%s,%s,%s,%s,%s)', (self.report_id, sheet.title, str(row + 1), 'ROW_HEIGHT', str(height)))

            for col, width in enumerate(colWidths):
                db.transact('insert into report_def(report_id,sheet_id,cell_id,cell_render_def,cell_calc_ref)\
                            values(%s,%s,%s,%s,%s)',
                            (self.report_id, sheet.title, get_column_letter(col + 1), 'COLUMN_WIDTH', str(width)))

            rng_startcell = []
            for rng in sheet.merged_cell_ranges:
                # print rng
                startcell, endcell = rng.split(':')
                # print sheet.cell(startcell).border
                rng_startcell.append(startcell)

                db.transact('insert into report_def(report_id,sheet_id,cell_id,cell_render_def,cell_calc_ref)\
                            values(%s,%s,%s,%s,%s)',
                            (self.report_id, sheet.title, rng, 'MERGED_CELL', sheet[startcell].value))

            for all_obj in sheet['A1':util.cell_index(sheet.max_column, sheet.max_row)]:
                for cell_obj in all_obj:
                    cell_ref = str(cell_obj.column) + str(cell_obj.row)
                    if len(rng_startcell) > 0 and cell_ref not in rng_startcell:
                        if cell_obj.value:
                            for key in formula_dict.keys():
                                cell_obj_value = str(cell_obj.value)
                                if key in cell_obj_value:
                                    cell_render_ref = formula_dict[key]
                                    break
                                else:
                                    cell_render_ref = 'STATIC_TEXT'

                            db.transact('insert into report_def(report_id,sheet_id ,cell_id ,cell_render_def ,cell_calc_ref)\
                                      values(%s,%s,%s,%s,%s)',
                                        (self.report_id, sheet.title, cell_ref, cell_render_ref, cell_obj_value.strip()))
        db.commit()
        return 0

    def render_report_template_json(self):

        db = DatabaseHelper()

        cur = db.query("select distinct sheet_id from report_def where report_id='%s' ", (self.report_id,))
        sheets = cur.fetchall()
        print(sheets)

        sheet_d_list=[]
        for sheet in sheets:
            matrix_list = []
            cur = db.query(
                "select cell_id,cell_render_def,cell_calc_ref from report_def where report_id='%s' and sheet_id='%s'",
                (self.report_id, sheet["sheet_id"]))
            report_template = cur.fetchall()

            for row in report_template:
                cell_d = {}
                if row["cell_render_def"] == 'STATIC_TEXT':
                    cell_d['cell'] =row['cell_id']
                    cell_d['value']=row['cell_calc_ref']
                    matrix_list.append(cell_d)


                elif row['cell_render_def'] == 'MERGED_CELL':
                    start_cell, end_cell = row['cell_id'].split(':')
                    cell_d['cell'] = start_cell
                    cell_d['value'] = row['cell_calc_ref']
                    cell_d['merged'] = end_cell
                    matrix_list.append(cell_d)

            sheet_d={}
            sheet_d['sheet']=sheet['sheet_id']
            #print(sheet_d['sheet'])
            sheet_d['matrix']=matrix_list
            sheet_d_list.append(sheet_d)


        json_dump = (sheet_d_list)
        #print(json_dump)
        return json_dump
