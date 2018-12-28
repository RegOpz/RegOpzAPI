from fpdf import FPDF
from collections import OrderedDict

output_filepath='./'
class PdfCreator():
    def __init__(self):
        self.report=None
        self.report_grid=OrderedDict()

    def set_report(self,report):
        self.report=report

    def create_grid(self,sheet,scaling_factor=1):
        cell_start={}
        cell_end={}
        merged_cells=sheet['merged_cells']
        row_heights=[ht*scaling_factor for ht in sheet['row_heights']]
        col_widths=[wd*scaling_factor for wd in sheet['col_widths']]
        data_grid = [[int(str(j+1)+str(i+1)) for i in range(len(col_widths))] for j in range(len(row_heights))]

        for merge_cell in merged_cells:
            start_row=merge_cell['row']
            start_col=merge_cell['col']
            end_row=start_row+merge_cell['rowspan']-1
            end_col=start_col+merge_cell['colspan']-1
            st_r=start_row

            #print(start_row,start_col,end_row,end_col)
            while st_r <= end_row:
                st_c = start_col
                while st_c <= end_col:
                    #print(st_r,st_c)
                    data_grid[st_r][st_c]=int(str(start_row+1)+str(start_col+1))
                    st_c+=1
                st_r+=1
        #print(data_grid)

        y=0
        for row in range(len(data_grid)):
            x =0
            last_end_x = 0
            last_end_y = 0
            for col in range(len(data_grid[row])):
                val=data_grid[row][col]
                if  val>0:
                    r = row
                    end_y=y
                    while r <len(data_grid):
                        end_x = x
                        c = col
                        while c <len(data_grid[r]):
                            #print(r,c)

                            if data_grid[r][c]==val:
                               end_x+=col_widths[c]
                               last_end_x = end_x
                               data_grid[r][c] = 0
                               c+=1
                            else:
                                break
                        last_end_y = end_y
                        end_y += row_heights[r]
                        if r==len(data_grid) -1 and end_x!=x:
                            last_end_y=end_y
                        r+=1
                        if end_x==x:
                            break


                    cell_start[str(row) + ',' + str(col)] = (x,y)
                    cell_end[str(row) + ',' + str(col)] = (last_end_x, last_end_y)
                x += col_widths[col]
                col+=1
            y+=row_heights[row]
            row+=1

        page_length=x
        page_width=y

        return cell_start,cell_end,page_length,page_width

    def create_fpdf(self,page_length,page_width,unit='cm'):
        pdf=FPDF('P',unit,(page_length,page_width))
        return pdf

            #self.pdf=FPDF()


    def create_pdf(self,report,filename,margin=0,unit='cm',scaling_factor=1):
        self.set_report(report)
        for sheet in self.report:
            #print(sheet)
            cell_start,cell_end,page_length,page_width=self.create_grid(sheet,scaling_factor)
            sheet_detail={'cell_start':cell_start,'cell_end':cell_end,'page_length':page_length,'page_width':page_width}
            self.report_grid[sheet['sheet']]=sheet_detail

        page_length=0
        page_width=0
        for sheet in self.report_grid.values():
             page_length=sheet['page_length'] if page_length < sheet['page_length'] else page_length
             page_width=sheet['page_width'] if page_width < sheet['page_width'] else page_width

        page_length+=2*margin
        page_width+=2*margin
        pdf=self.create_fpdf(page_length,page_width,unit)
        pdf.set_auto_page_break(False)


        for report_sheet in self.report:
            sheet=report_sheet['sheet']
            cell_start=self.report_grid[sheet]['cell_start']
            cell_end = self.report_grid[sheet]['cell_end']
            sheet_length = self.report_grid[sheet]['page_length']
            sheet_width= self.report_grid[sheet]['page_width']

            sheet_offset_y=(page_width-sheet_width)/2
            sheet_offset_x=(page_length-sheet_length)/2
            pdf.add_page()
            for cell in cell_start.keys():
                start_coord=cell_start[cell]
                end_coord=cell_end[cell]
                x_len=(end_coord[0]-start_coord[0])
                y_len=(end_coord[1] - start_coord[1])
                array_ind=list(map(int,cell.split(',')))
                cell_val=report_sheet['data'][array_ind[0]][array_ind[1]]
                pdf.set_xy(start_coord[0]+sheet_offset_x,start_coord[1]+sheet_offset_y)
                pdf.set_font('Arial', '', 10)
                pdf.cell(x_len,y_len,str(cell_val),1)

        pdf.output(output_filepath+filename,'F')

if __name__=='__main__':
    num=10
    row = [i for i in range(num)]
    data = [row] * num
    row_heights=[1]*num
    col_widths=[2]*num
    merged_cells=[{'row': 0, 'col': 0,'rowspan': 1, 'colspan': 3},{'row': 8, 'col': 8,'rowspan': 2, 'colspan': 2},
                  {'row': 2, 'col': 0, 'rowspan': 3, 'colspan': 1}]
    sheet_d={}
    sheet_d['sheet'] ='Test sheet'
    sheet_d['row_heights'] = row_heights
    sheet_d['col_widths'] = col_widths
    sheet_d['data'] = data
    sheet_d['merged_cells'] = merged_cells
    report=[sheet_d]

    num = 20
    row = [i for i in range(num)]
    data = [row] * num
    row_heights = [1] * num
    col_widths = [2] * num
    merged_cells = [{'row': 0, 'col': 0, 'rowspan': 1, 'colspan': 3}, {'row': 8, 'col': 8, 'rowspan': 2, 'colspan': 2},
                    {'row': 2, 'col': 0, 'rowspan': 3, 'colspan': 1}]
    sheet_d = {}
    sheet_d['sheet'] = 'Test sheet 1'
    sheet_d['row_heights'] = row_heights
    sheet_d['col_widths'] = col_widths
    sheet_d['data'] = data
    sheet_d['merged_cells'] = merged_cells
    report.append(sheet_d)

    pdf=PdfCreator()
    pdf.create_pdf(report=report,filename='test.pdf',margin=1,unit='cm',scaling_factor=0.5)
