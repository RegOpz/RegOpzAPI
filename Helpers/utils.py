#!/usr/bin/python3
# -*- coding: utf-8 -*-

from openpyxl.utils import get_column_letter
import math
from flask import Flask, request, Response
from jwt import JWT, jwk_from_pem

def cell_index(col_idx,row_idx):
    return get_column_letter(col_idx)+str(row_idx)

def if_null_zero(value):
    return 0 if value is None else value

def if_null_blank(value):
    return '""' if value is None else value

def flatten(two_d_array):
    return [element for row in two_d_array for element in row]


def split(arr, size):
    arrs = []
    while len(arr) > size:
        pice = arr[:size]
        arrs.append(pice)
        arr = arr[size:]
    arrs.append(arr)
    return arrs

def dict_factory(cursor, row):
    d = {}
    for idx,col in enumerate(cursor.description):
        print(row[idx], col[0])
        d[col[0]] = row[idx]


    return d

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def get_keycolumn(cur,table_name):
    cur.execute("select br.python_implementation from business_rules br,data_source_information ds where \
                 br.source_id = ds.source_id and ds.source_table_name =\'"+table_name+"\'  and br.rule_type='KEYCOLUMN'")
    keycolumn = cur.fetchone()
    return keycolumn["python_implementation"].replace("[","").replace("]","")

def clean_table(cur,table_name,business_date,reporting_date,where_condition=''):
    sql='delete from  '+table_name+' where 1=1'
    if business_date!='':
        sql+=' and business_date =' + business_date
    if reporting_date!='':
        sql+=' and reporting_date =' + reporting_date
    if where_condition!='':
        sql+=' and ' + where_condition
    cur.execute(sql)

def round_value(number_to_round,option):
    if option=='' or option == None:
        option='NONE'
    if option=='CEIL':
        rounded_number=math.ceil(number_to_round)
    elif option=='FLOOR':
        rounded_number = math.floor(number_to_round)
    elif option=='TRUNC':
        rounded_number=math.trunc(number_to_round)
    elif 'DECIMAL' in option:
        decimal_point=int(option.replace('DECIMAL',''))
        rounded_number=round(number_to_round,decimal_point)
    else:
        rounded_number=number_to_round

    return rounded_number

def autheticateTenant():
    subscriber = request.headers.get('Subscriber')
    if subscriber:
        token = subscriber
    else:
        token = request.headers.get('Authorization')
    if token:
        extracted_token = token.replace("Bearer ", "")
        with open('public_key', 'r') as fh:
            salt = jwk_from_pem(fh.read().encode())
        try:
            token_decode = JWT().decode(extracted_token, salt)
            if 'domainInfo' in token_decode.keys():
                return token_decode['domainInfo']
            else:
                # raise ValueError("Forbidden! No valid subscriber info found!")
                return None
        except Exception:
            # print("autheticateTenant ...........Invalid Token Recieved for Subscriber Authentication")
            # raise TypeError("Invalid Token Recieved for Subscriber Authentication")
            return None
    else:
        # raise ValueError("Forbidden! No Token Recieved for Subscriber Authentication")
        return None

def get_css_style_from_openpyxl(_cell):
    cell_style={}
    # Font
    # if _cell.font.color:
    cell_style['color'] = _cell.font.color.rgb if _cell.font.color else '00000000'
    cell_style['font']={}
    if _cell.font.name:
        cell_style['font']['family'] = _cell.font.name
    if _cell.font.b:
        cell_style['font']['weight'] = 'bold'
    if _cell.font.i:
        cell_style['font']['style'] = 'italic'
    if _cell.font.sz:
        cell_style['font']['size'] = str(int(_cell.font.sz)) + "px"
    # Background
    if _cell.fill.fill_type and _cell.fill.fgColor.rgb and _cell.fill.fgColor.rgb.upper()!='FFFFFFFF':
        cell_style['background'] = {'color': _cell.fill.fgColor.rgb }
    # alignment
    cell_style['alignment']={}
    if _cell.alignment.horizontal:
        cell_style['alignment']['horizontal'] = _cell.alignment.horizontal
    if _cell.alignment.vertical:
        cell_style['alignment']['vertical'] = _cell.alignment.vertical
    # Border
    cell_style['border']={}
    if _cell.border.top.style:
        cell_style['border']['top']={ 'style': 'solid', 'width': 'thin',
                                    'color': _cell.border.top.color.rgb if _cell.border.top.color else '00000000' }
    if _cell.border.bottom.style:
        cell_style['border']['bottom']={ 'style': 'solid', 'width': 'thin',
                                    'color': _cell.border.bottom.color.rgb if _cell.border.bottom.color else '00000000' }
    if _cell.border.left.style:
        cell_style['border']['left']={ 'style': 'solid', 'width': 'thin',
                                    'color': _cell.border.left.color.rgb if _cell.border.left.color else '00000000' }
    if _cell.border.right.style:
        cell_style['border']['right']={ 'style': 'solid', 'width': 'thin',
                                    'color': _cell.border.right.color.rgb if _cell.border.right.color else '00000000' }

    return cell_style

def process_td_class_names(style_obj , td_class_name, sheet_styles, path=()):
    # app.logger.info("inside process_td_class_names {} ".format(style_obj,))
    for k, v in style_obj.items():
        if hasattr(v, 'items'):
            process_td_class_names(v, td_class_name, sheet_styles, path + (k,))
        else:
            # Check whether path is alignment
            if 'alignment' in path :
                if str(v) != "None":
                    new_value =  "middle" if k=='vertical' and str(v)=="center" else str(v)
                    class_name = ' ht' + new_value.capitalize()
                    td_class_name['classes'] += class_name
            else :
                new_class_type = '-'.join(path + (k,))[0:]
                # check for background color
                if k =='color':
                    new_value =  '#' + (str(v)[2:] if len(str(v))==8 else str(v))
                else:
                    new_value = str(v)

                class_name = ' ht' + new_class_type + '-' + new_value.replace('#','HEX').replace(' ','_')
                td_class_name['classes'] += class_name
                sheet_styles['style_classes'][class_name]={'new_class_type' : new_class_type, 'new_value' : new_value}
