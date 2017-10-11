from flask_restful import Resource,abort
from flask import Flask, request, redirect, url_for
from Helpers.DatabaseHelper import DatabaseHelper
import openpyxl as xls
from openpyxl.styles import Border, Side, PatternFill, Font, GradientFill, Alignment, Protection
import Helpers.utils as util
import time
from Controllers.GenerateReportController import GenerateReportController as report

class ViewReportController(Resource):
    def __init__(self):
        self.db = DatabaseHelper()

    def get(self,report_id=None):
        if request.endpoint == 'view_report_ep':
            self.report_id = report_id
            reporting_date = request.args.get('reporting_date')
            # print(reporting_date)
            return self.render_report_json(reporting_date)

        if request.endpoint == 'report_list_ep':
            reporting_date = request.args.get('reporting_date')
            reporting_date_start = request.args.get('reporting_date_start')
            reporting_date_end = request.args.get('reporting_date_end')
            return self.render_report_list(reporting_date=reporting_date,reporting_date_start=reporting_date_start,
                                           reporting_date_end=reporting_date_end)

        if request.endpoint == 'get_report_export_to_excel_ep':
            self.report_id = request.args.get('report_id')
            reporting_date = request.args.get('reporting_date')
            cell_format_yn = request.args.get('cell_format_yn')
            if cell_format_yn == None or cell_format_yn == "":
                cell_format_yn = 'N'
            return self.export_to_excel(reporting_date=reporting_date,cell_format_yn=cell_format_yn)

    def render_report_json(self, reporting_date='19000101', cell_format_yn='Y'):

        # db = DatabaseHelper()

        sheets = self.db.query("select distinct sheet_id from report_def where report_id=%s",
                               (self.report_id,)).fetchall()

        agg_format_data = {}
        if cell_format_yn == 'Y':
            summary_set = report.create_report_summary_final(self, populate_summary=False,
                                                             cell_format_yn=cell_format_yn,
                                                             report_id=self.report_id,
                                                             business_date_from=str(reporting_date)[:8],
                                                             business_date_to=str(reporting_date)[8:])
            # print(summary_set)
            for e in summary_set:
                agg_format_data[e[0] + e[1] + e[2]] = e[3]
                # print(agg_format_data)

        sheet_d_list = []
        for sheet in sheets:
            matrix_list = []
            row_attr = {}
            col_attr = {}
            report_template = self.db.query(
                "select cell_id,cell_render_def,cell_calc_ref from report_def where report_id=%s and sheet_id=%s",
                (self.report_id, sheet["sheet_id"])).fetchall()

            data = self.db.query('select b.report_id,b.sheet_id,b.cell_id,a.cell_summary,\
                                b.reporting_scale,b.rounding_option \
                                from report_comp_agg_def b left join report_summary a\
                                on a.report_id=b.report_id and\
                                a.sheet_id=b.sheet_id and \
                                a.cell_id=b.cell_id and \
                                a.reporting_date=%s \
                                where b.report_id=%s \
                                and b.sheet_id=%s\
                                order by b.report_id,b.sheet_id,b.cell_id',
                                 (reporting_date, self.report_id, sheet["sheet_id"])).fetchall()

            for row in report_template:
                cell_d = {}
                if row["cell_render_def"] == 'STATIC_TEXT':
                    cell_d['cell'] = row['cell_id']
                    cell_d['value'] = row['cell_calc_ref']
                    matrix_list.append(cell_d)


                elif row['cell_render_def'] == 'MERGED_CELL':
                    start_cell, end_cell = row['cell_id'].split(':')
                    cell_d['cell'] = start_cell
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

            for row in data:
                cell_d = {}
                if cell_format_yn == 'Y':
                    # print(row["cell_id"],row["cell_summary"])
                    try:
                        cell_summary = agg_format_data[row['report_id'] + row['sheet_id'] + row['cell_id']]
                    except KeyError:
                        cell_summary = util.round_value(
                            float(util.if_null_zero(row["cell_summary"])) / float(row["reporting_scale"]),
                            row["rounding_option"])

                else:
                    cell_summary = float(util.if_null_zero(row["cell_summary"]))

                cell_d['cell'] = row['cell_id']
                cell_d['value'] = cell_summary
                matrix_list.append(cell_d)

            sheet_d = {}
            sheet_d['sheet'] = sheet['sheet_id']
            # print(sheet_d['sheet'])
            sheet_d['matrix'] = matrix_list
            sheet_d['row_attr'] = row_attr
            sheet_d['col_attr'] = col_attr
            sheet_d_list.append(sheet_d)

        json_dump = (sheet_d_list)
        # print(json_dump)
        return json_dump

    def render_report_list(self,reporting_date=None, reporting_date_start=None, reporting_date_end=None):
        #db=DatabaseHelper()
        if reporting_date:
            sql = "select * from report_catalog where as_of_reporting_date='{0}'".format(reporting_date)

        if reporting_date_start and reporting_date_end:
            data_sources={}
            data_sources['start_date']=reporting_date_start
            data_sources['end_date']=reporting_date_end
            sql = "select * from report_catalog where as_of_reporting_date between '{0}' and '{1}'".format(reporting_date_start ,reporting_date_end)

        reports = self.db.query(sql).fetchall()

        #print(data_sources)
        if reporting_date:
            return (reports)
        else:
            data_sources['data_sources']=reports
            return data_sources

    def export_to_excel(self, **kwargs):
        parameter_list = ['reporting_date','cell_format_yn']
        if set(parameter_list).issubset(set(kwargs.keys())):
            report_id = self.report_id
            reporting_date=kwargs['reporting_date']
            cell_format_yn=kwargs['cell_format_yn']
            target_file_name = report_id+ '_' + str(reporting_date) + '_' + str(time.time())+ '.xlsx'
        else:
            print("Please supply parameters: " + str(parameter_list))

        #Create report template
        wr = xls.Workbook()
        # target_dir='../output/'
        target_dir = './static/'
        db=DatabaseHelper()

        sheets=db.query('select distinct sheet_id from report_def where report_id=%s', (report_id,)).fetchall()

        agg_format_data ={}
        if cell_format_yn == 'Y':
            summary_set = report.create_report_summary_final(self,populate_summary = False,
                            cell_format_yn = cell_format_yn,
                            report_id = self.report_id,
                             business_date_from = str(reporting_date)[:8],
                             business_date_to = str(reporting_date)[8:])
            #print(summary_set)
            for e in summary_set:
                agg_format_data[e[0]+e[1]+e[2]] = e[3]
            #print(agg_format_data)
        # print sheets
        # The default sheet of the workbook
        al = Alignment(horizontal="center", vertical="center", wrap_text=True, shrink_to_fit=True)
        ws = wr.worksheets[0]
        for sheet in sheets:
            # The first sheet title will be Sheet, so do not create any sheet, just rename the title
            if ws.title == 'Sheet':
                ws.title = sheet["sheet_id"]
            else:
                ws = wr.create_sheet(title=sheet["sheet_id"])

            report_template=db.query('select cell_id,cell_render_def,cell_calc_ref from report_def where report_id=%s and sheet_id=%s',
                        (report_id, sheet["sheet_id"],)).fetchall()

            for row in report_template:
                if row["cell_render_def"] == 'MERGED_CELL':
                    ws.merge_cells(row["cell_id"])
                    startcell, endcell = row["cell_id"].split(':')
                    ws[startcell].value = row["cell_calc_ref"]
                    ws[startcell].value = row["cell_calc_ref"]
                    ws[startcell].fill = PatternFill("solid", fgColor="DDDDDD")
                    ws[startcell].font = Font(size=9)
                    ws[startcell].alignment = al
                elif row["cell_render_def"] == 'STATIC_TEXT':
                    ws[row["cell_id"]].value = row["cell_calc_ref"]
                    ws[row["cell_id"]].fill = PatternFill("solid", fgColor="DDDDDD")
                    ws[row["cell_id"]].font = Font(size=9)
                    if row["cell_calc_ref"][:1] != '=' and row["cell_calc_ref"][
                                                           len(row["cell_calc_ref"]) - 1:1] != '%' and 'SUM' not in row[
                        "cell_calc_ref"]:
                        ws[row["cell_id"]].alignment = al
                elif row["cell_render_def"] == 'CALCULATE_FORMULA':
                    ws[row["cell_id"]].fill = PatternFill("solid", fgColor="DDDDDD")
                    ws[row["cell_id"]].font = Font(bold=True, size=9)
                    if '=' in row["cell_calc_ref"]:
                        ws[row["cell_id"]].value = row["cell_calc_ref"]
                    else:
                        ws[row["cell_id"]].value = '=' + row["cell_calc_ref"]
                if row["cell_render_def"] == 'ROW_HEIGHT' and row["cell_calc_ref"] != 'None':
                    # print('row ' + row["cell_id"])
                    ws.row_dimensions[int(row["cell_id"])].height = float(row["cell_calc_ref"])
                elif row["cell_render_def"] == 'COLUMN_WIDTH' and row["cell_calc_ref"] != 'None':
                    # print(row["cell_id"])
                    ws.column_dimensions[row["cell_id"]].width = float(row["cell_calc_ref"])
                else:
                    pass

        wr.save(target_dir + target_file_name)
        #End create report template

        #Create report body
        wb = xls.load_workbook(target_dir + target_file_name)
        data=db.query('select b.report_id,b.sheet_id,b.cell_id,a.cell_summary,\
                    b.reporting_scale,b.rounding_option \
                    from report_comp_agg_def b left join report_summary a\
                    on a.report_id=b.report_id and\
                    a.sheet_id=b.sheet_id and \
                    a.cell_id=b.cell_id and \
                    a.reporting_date=%s \
                    where b.report_id=%s \
                    order by b.report_id,b.sheet_id,b.cell_id',(reporting_date,report_id,)).fetchall()

        for row in data:
            ws = wb.get_sheet_by_name(row["sheet_id"])
            if cell_format_yn == 'Y':
                # print(row["cell_id"],row["cell_summary"])
                try:
                    cell_summary = agg_format_data[row['report_id']+row['sheet_id']+row['cell_id']]
                except KeyError:
                    cell_summary = util.round_value(
                    float(util.if_null_zero(row["cell_summary"])) / float(row["reporting_scale"]),
                    row["rounding_option"])
                ws[row["cell_id"]].value = cell_summary
            else:
                ws[row["cell_id"]].value = float(util.if_null_zero(row["cell_summary"]))

            ws[row["cell_id"]].font = Font(size=9)


        wb.save(target_dir + target_file_name)

        #End create report body

        return { "file_name": target_file_name }

