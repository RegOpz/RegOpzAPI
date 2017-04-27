from flask import Flask, jsonify, request
from flask_restful import Resource
import openpyxl as xls
import time
from multiprocessing import Pool
from functools import partial
import csv
import utils as util
from openpyxl.styles import Border, Side, PatternFill, Font, GradientFill, Alignment, Protection
from openpyxl.utils import get_column_letter
from collections import defaultdict
import sys
import ast
import mysql.connector as mysql
import pandas as pd
from Helpers.DatabaseHelper import DatabaseHelper
from operator import itemgetter


class ViewData(Resource):
    def getDataSource(**kwargs):
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
                         table['source_table_name']).fetchone()
        data_dict['cols'] = cols
        data_dict['rows'] = data
        data_dict['count'] = count['count']

        # print(data_dict)
        return data_dict

    def render_data_load_dates():
        db = DatabaseHelper()

        catalog = db.query(
            "select distinct business_date from data_catalog order by business_date").fetchall()

        catalog_list = []

        for cat in catalog:
            year = cat['business_date'][:4]
            month = cat['business_date'][4:6]
            bus_date = cat['business_date']

            # print(year,month,bus_date)
            # print(list(map(itemgetter('year'),catalog_list)))

            idx = list(map(itemgetter('year'), catalog_list)).index(year)\
                if year in map(itemgetter('year'), catalog_list) else None
            #print(list(map(itemgetter('year'), catalog_list)))
            if idx == None:
                d = {'year': year, month: [bus_date]}
                catalog_list.append(d)
                # print(catalog_list)

            else:
                if month in catalog_list[idx].keys():
                    catalog_list[idx][month].append(bus_date)
                else:
                    catalog_list[idx][month] = [bus_date]

        # print(catalog_list)
        return catalog_list
