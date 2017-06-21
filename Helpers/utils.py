#!/usr/bin/python3
# -*- coding: utf-8 -*-

from openpyxl.utils import get_column_letter
import math

def cell_index(col_idx,row_idx):
    return get_column_letter(col_idx)+str(row_idx)

def if_null_zero(value):
    return 0 if value is None else value

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
