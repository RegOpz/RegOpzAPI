from app import *
from flask_restful import Resource,abort
from Helpers.DatabaseHelper import DatabaseHelper
import openpyxl as xls
from openpyxl.utils import column_index_from_string,get_column_letter,coordinate_from_string,coordinate_to_tuple
import Helpers.utils as util
import json
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *
import pandas as pd

UPLOAD_FOLDER='.'

class FFCaptureTemplateController(Resource):
    def __init__(self):
        self.domain_info = autheticateTenant()
        self.db_master=DatabaseHelper()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.db=DatabaseHelper(self.tenant_info)

    def post(self, report_id=None):
        self.report_id = report_id
        self.db_object_suffix = request.args.get('domain_type')
        report_data = request.get_json(force=True)
        return self.save_hot_table_report_template(report_data)

    def extract_content_type(val):
        if not val:
            return '', 'BLANK'
        return val, 'STATIC_TEXT'

    def load_report_template(self,template_file_name):

        app.logger.info("Loading report template")
        try:
            def_object ="report_free_format_def"
            ref_object="report_free_format_ref"
            sec_object="report_free_format_section"

            if self.db_object_suffix:
                def_object += "_" + self.db_object_suffix
                ref_object += "_" + self.db_object_suffix
                sec_object += "_" + self.db_object_suffix

            ref_columns = self.db.query("select * from {} limit 1".format(ref_object)).description
            def_columns = self.db.query("select * from {} limit 1".format(def_object)).description
            #sec_columns = self.db.query("select * from {} limit 1").format(sec_object).description

            ref_df = pd.DataFrame(columns=[i[0] for i in ref_columns])
            def_df = pd.DataFrame(columns=[i[0] for i in def_columns])

            target_dir = UPLOAD_FOLDER + "/"
            app.logger.info("Loading {} file from {} directory".format(template_file_name, target_dir))
            wb = xls.load_workbook(target_dir + template_file_name,read_only=True)

            sheet_index = 0
            for sheet in wb.worksheets:

                app.logger.info("Processing definition entries for sheet {} ,report {}".format(sheet.title, self.report_id))

                row_heights = [sheet.row_dimensions[r + 1].height for r in range(sheet.max_row)]
                col_widths = [sheet.column_dimensions[get_column_letter(c + 1)].width for c in range(sheet.max_column)]

                app.logger.info("Creating entries for row height")
                for row, height in enumerate(row_heights):
                    def_df = def_df.append({'report_id': self.report_id, 'sheet_id': sheet.title, 'entity_type': 'ROW', \
                         'entity_ref': str(row+1), 'content_type': 'ROW_HEIGHT','content': str(height)}, ignore_index=True)

                app.logger.info("Creating entries for column width")
                for col, width in enumerate(col_widths):
                    def_df = def_df.append({'report_id': self.report_id, 'sheet_id': sheet.title, 'entity_type': 'COL',
                         'entity_ref': get_column_letter(col + 1), 'content_type': 'COLUMN_WIDTH', 'content': str(width)}, ignore_index=True)

                merged_cell = {}
                for cell_rng in sheet.merged_cell_ranges: #move to 2.5.14 sheet.merged_cells.ranges
                    # print rng
                    startcell, endcell = cell_rng.split(':')
                    # print sheet.cell(startcell).border
                    start_xy = coordinate_to_tuple(startcell)
                    end_xy=coordinate_to_tuple(endcell)
                    start_row,start_col = start_xy
                    end_row,end_col = end_xy
                    for r in range(start_row,end_row+1):
                        for c in range(start_col, end_col+1):
                            cell = get_column_letter(c) + str(r)
                            merged_cell[cell] = {'cell_rng': cell_rng, 'start_cell': startcell}

                cell_added=[]
                sheet_index += 1
                ref=0
                for row in sheet.rows:
                    for cell_idx in row:
                        cell = str(cell_idx.column) + str(cell_idx.row)
                        row = cell_idx.row
                        col = str(cell_idx.column)

                        if cell in merged_cell.keys():
                            cell_id = merged_cell[cell]['cell_rng']
                            start_cell=merged_cell[cell]['start_cell']
                            is_merged_cell = True
                        else:
                            cell_id = cell
                            start_cell=cell

                        try :
                            idx=cell_added.index(cell_id)
                        except ValueError:
                            ref+=1
                            cell_ref='S'+str(sheet_index)+'{:04d}'.format(ref)
                            content,content_type=self.extract_content_type(cell_idx.value)
                            ref_df=ref_df.append({'report_id':self.report_id,'sheet_id':sheet['sheet'],'cell_id':cell_id,
                                           'col_id':col,'row_id':row,'cell_type':'MERGED' if is_merged_cell else 'SINGLE',
                                           'cell_ref':cell_ref,'section_id':None,'section_type':None,},ignore_index=True)
                            def_df=def_df.append({'report_id':self.report_id,'sheet_id':sheet['sheet'],'entity_type':'CELL',
                                                  'entity_ref':cell_ref,'content_type':content_type,'content':content},ignore_index=True)
                            cell_style = util.get_css_style_from_openpyxl(cell_idx)
                            def_df=def_df.append({'report_id':self.report_id,'sheet_id':sheet['sheet'],'entity_type':'CELL',\
                                                  'entity_ref':cell_ref,'content_type':'CELL_STYLE','content':json.dumps(cell_style)},ignore_index=True)
                            cell_added.append(cell_id)

            app.logger.info("Deleting definition entries for report {}".format(self.report_id, ))
            self.db.transact('delete from {} where report_id=%s'.format(def_object, ), (self.report_id,))
            self.db.transact('delete from {} where report_id=%s'.format(ref_object), (self.report_id,))
            self.db.transact('delete from {} where report_id=%s'.format(sec_object), (self.report_id,))

            def_df.fillna('',inplace=True)
            ref_df.fillna('',inplace=True)

            self.db.transact("insert into {0}({1}) values({2})".format(def_object, ",".join(def_df.columns),",".join(['%s'] * len(def_df.columns))),
                             list(def_df.itertuples(index=False, name=None)))

            self.db.transact("insert into {0}({1}) values({2})".format(ref_object, ",".join(ref_df.columns),",".join(['%s'] * len(def_df.columns))),
                             list(ref_df.itertuples(index=False, name=None)))

            self.db.commit()

            return {"msg": "Report [" + self.report_id + "] template updates has been captured successfully "}, 200
        except Exception as e:
            app.logger.error(str(e))
            return {"msg": str(e)}, 500


    def save_hot_table_report_template(self,report_data):
        app.logger.info("Saving hot table report template")
        try:
            def cell_section(sec_df,cell_rng):
                cells=cell_rng.split(':')
                for cell in cells:
                    r,c=coordinate_to_tuple(cell)
                    for row in sec_df:
                        if row['start_row']<= r and row['end_row'] >= r and row['start_col'] <= c and row['end_col'] >= c:
                            return row['section_id'],row['section_type']
                return None,None


            ref_object ="report_free_format_ref"
            def_object="report_free_format_def"
            sec_object="report_free_format_section"

            if self.db_object_suffix:
                def_object += "_" + self.db_object_suffix
                ref_object += "_" + self.db_object_suffix
                sec_object += "_" + self.db_object_suffix

            ref_columns=self.db.query("select * from {} limit 1".format(ref_object)).description
            def_columns=self.db.query("select * from {} limit 1".format(def_object)).description
            sec_columns=self.db.query("select * from {} limit 1").format(sec_object).description

            ref_df=pd.DataFrame(columns=[i[0] for i in ref_columns])
            def_df=pd.DataFrame(columns=[i[0] for i in def_columns])

            sheet_index =0
            for sheet in report_data:
                #{"ht_range": [0, 0, 0, 1], "range": "A1:B1", "section_id": "s1", "section_position": [], "section_type": "FIXEDFORMAT"}
                sec_df=[]
                sec_df2=pd.DataFrame(columns=[i[0] for i in sec_columns])
                for sec in sheet['sections']:
                    sec_df.append({'section_id':sec['secction_id'],'section_type':sec['section_type'],'start_row':sec['ht_range'][0],
                                'start_col':sec['ht_range'][1],'end_row':sec['ht_range'][2],'end_col':sec['ht_range'][3]})
                    sec_df2.append({'report_id':self.report_id ,'sheet_id':sheet['sheet'],'section_id':sec['section_id'] ,
                                    'section_range':sec['range'],'section_ht_range':",".join(sec['ht_range']),
                                    'section_depenendency':",".join(sec['section_position']),'section_type':sec['section_type']},ignore_index=True)

                merged_cell = {}
                for rng in sheet['mergedCells']:
                    # print rng
                    startcell = get_column_letter(rng['col'] + 1) + str(rng['row'] + 1)
                    endcell = get_column_letter(rng['col'] + rng['colspan']) + str(rng['row'] + rng['rowspan'])
                    cell_rng = startcell + ':' + endcell
                    start_row = rng['row'] + 1
                    start_col = get_column_letter(rng['col'] + 1)

                    for r in range(rng['row']+1,rng['row']+rng['rowspan']):
                      for c in range(rng['col']+1,rng['col']+rng['colspan']):
                          cell=get_column_letter(c)+str(r)
                          merged_cell[cell] = {'cell_rng': cell_rng,'start_cell':startcell}

                cell_added=[]
                sheet_index+=1
                ref=0
                for r,cols in enumerate(sheet['sheetData']):
                    for c,val in enumerate(cols):
                        is_merged_cell = False
                        row = r+1
                        col = get_column_letter(c+1)
                        cell = col + str(row)
                        if cell in merged_cell.keys():
                            cell_id = merged_cell[cell]['cell_rng']
                            start_cell=merged_cell[cell]['start_cell']
                            col,row=coordinate_from_string(start_cell)
                            is_merged_cell = True
                        else:
                            cell_id = cell
                            start_cell=cell
                        section,section_type=cell_section(sec_df,cell_rng)
                        try :
                            idx=cell_added.index(cell_id)
                        except ValueError:
                            ref+=1
                            cell_ref='S'+str(sheet_index)+'{:04d}'.format(ref)
                            content,content_type=self.extract_content_type(val)
                            ref_df=ref_df.append({'report_id':self.report_id,'sheet_id':sheet['sheet'],'cell_id':cell_id,
                                           'col_id':col,'row_id':row,'cell_type':'MERGED' if is_merged_cell else 'SINGLE',
                                           'cell_ref':cell_ref,'section_id':section,'section_type':section_type,},ignore_index=True)
                            def_df=def_df.append({'report_id':self.report_id,'sheet_id':sheet['sheet'],'entity_type':'CELL',
                                                  'entity_ref':cell_ref,'content_type':content_type,'content':content},ignore_index=True)
                            if start_cell in sheet['sheetStyles'].keys():
                                cell_style = sheet['sheetStyles'][start_cell]
                                def_df=def_df.append({'report_id':self.report_id,'sheet_id':sheet['sheet'],'entity_type':'CELL',\
                                                  'entity_ref':cell_ref,'content_type':'CELL_STYLE','content':json.dumps(cell_style)},ignore_index=True)
                            cell_added.append(cell_id)



                        app.logger.info("Creating entries for row height")
                        for row in sheet['rowHeights'].keys():
                            def_df = def_df.append({'report_id': self.report_id, 'sheet_id': sheet['sheet'], 'entity_type': 'ROW',\
                        'entity_ref':str(row),'content_type':'ROW_HEIGHT', 'content': str(sheet['rowHeights'][str(row)])}, ignore_index = True)

                        app.logger.info("Creating entries for column width")
                        for col in sheet['colWidths'].keys():
                            def_df = def_df.append(
                                {'report_id': self.report_id, 'sheet_id': sheet['sheet'], 'entity_type': 'COL','entity_ref': str(row), 'content_type': 'COLUMN_WIDTH',\
                                 'content': str(sheet['colWidths'][col]/8)}, ignore_index=True)


            app.logger.info("Deleting definition entries for report {}".format(self.report_id, ))
            self.db.transact('delete from {} where report_id=%s'.format(def_object, ),(self.report_id,))
            self.db.transact('delete from {} where report_id=%s'.format(ref_object), (self.report_id,))
            self.db.transact('delete from {} where report_id=%s'.format(sec_object), (self.report_id,))

            self.db.transact("insert into {0}({1}) values({2})".format(def_object,",".join(def_df.columns),
                             ",".join(['%s'] * len(def_df.columns))),list(def_df.itertuples(index=False, name=None)))

            self.db.transact("insert into {0}({1}) values({2})".format(ref_object, ",".join(ref_df.columns),
                            ",".join(['%s'] * len(def_df.columns))),list(ref_df.itertuples(index=False, name=None)))
            self.db.transact("insert into {0}({1}) values({2})".format(sec_object,",".join(sec_df2.columns),
                            ",".join(['%s'] * len(sec_df2.columns))),list(sec_df2.itertuples(index=False, name=None)))
            self.db.commit()

            return {"msg": "Report [" + self.report_id + "] template updates has been captured successfully "}, 200
        except Exception as e:
            app.logger.error(str(e))
            raise e
            # return {"msg": str(e)}, 500

