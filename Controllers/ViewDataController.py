from flask import Flask, jsonify, request
from flask_restful import Resource
import openpyxl as xls
import time
from multiprocessing import Pool
from functools import partial
import csv
import Helpers.utils as util
from openpyxl.styles import Border, Side, PatternFill, Font, GradientFill, Alignment, Protection
from openpyxl.utils import get_column_letter
from collections import defaultdict
import sys
import ast
import mysql.connector as mysql
import pandas as pd
from Helpers.DatabaseHelper import DatabaseHelper
from operator import itemgetter
from datetime import datetime
class ViewDataController(Resource):
    def get(self):
        if(request.endpoint == 'get_date_heads_ep'):
            return self.render_data_load_dates()
        if(request.endpoint == 'get_report_ep'):
            source_id = request.args['source_id']
            business_date = request.args['business_date']
            page = request.args['page']
            return self.getDataSource(source_id=source_id,business_date=business_date,page=page)
        if(request.endpoint == 'get_source_ep'):
            business_date = request.args['business_date']
            return self.render_data_source_list(business_date=business_date)
    def getDataSource(self,**kwargs):
        parameter_list = ['source_id', 'business_date', 'page']

        if set(parameter_list).issubset(set(kwargs.keys())):
            source_id = kwargs['source_id']
            business_date = kwargs['business_date']
            page = kwargs['page']
        else:
            print("Please supply parameters: " + str(parameter_list))
        db = DatabaseHelper()
        cur = db.query(
            "select source_table_name from data_source_information where source_id='" + str(source_id) + "'")
        table = cur.fetchone()

        startPage = int(page) * 100
        data_dict = {}
        cur = db.query("select * from " + table['source_table_name'] +
                       " where business_date='" + business_date + "' limit " + str(startPage) + ", 100")
        data = cur.fetchall()
        cols = [i[0] for i in cur.description]
        count = db.query('select count(*) as count from ' +
                         table['source_table_name'] +
                         ' where business_date=\'' + business_date + '\'').fetchone()
        data_dict['cols'] = cols
        data_dict['rows'] = data
        data_dict['count'] = count['count']

        # print(data_dict)
        return data_dict

    def render_data_load_dates(self):
        month_lookup={ '1': 'January',
                       '2':'February',
                       '3':'March',
                       '4':'April',
                       '5':'May',
                       '6':'June',
                       '7':'July',
                       '8':'August',
                       '9':'Sepember',
                       '10':'October',
                       '11':'November',
                       '12':'December'
                       }
        db = DatabaseHelper()

        catalog=db.query("select distinct business_date from data_catalog order by business_date").fetchall()

        catalog_list=[]

        for cat in catalog:
            year=str(cat['business_date'].year)
            month_num=str(cat['business_date'].month)
            bus_date=str(cat['business_date'].day)
            month=month_lookup[month_num]

            #print(year,month,bus_date)
            #print(list(map(itemgetter('year'),catalog_list)))

            idx=list(map(itemgetter('year'),catalog_list)).index(year)\
                if year in map(itemgetter('year'),catalog_list) else None
            #print(list(map(itemgetter('year'), catalog_list)))
            if idx==None:
                d={'year':year,'month':{month:[bus_date]}}
                catalog_list.append(d)
                #print(catalog_list)

            else:
                if month in catalog_list[idx]['month'].keys():
                    catalog_list[idx]['month'][month].append(bus_date)
                else:
                    catalog_list[idx]['month'][month]=[bus_date]


        return (catalog_list)
    def render_data_source_list(self,**kwargs):

        parameter_list = ['business_date']

        if set(parameter_list).issubset(set(kwargs.keys())):
            business_date = kwargs['business_date']
        else:
            print("Please supply parameters: " + str(parameter_list))

        db=DatabaseHelper()
        data_sources = db.query("select source_id,data_file_name\
                        from data_catalog where business_date='"+business_date+"'").fetchall()

        #print(data_sources)
        return (data_sources)
