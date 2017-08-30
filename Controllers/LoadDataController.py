from flask_restful import Resource,abort
from flask import Flask, request, redirect, url_for
from Helpers.DatabaseHelper import DatabaseHelper
import pandas as pd

UPLOAD_FOLDER='./uploads/source-files/'
class LoadDataController(Resource):
    def __init__(self):
        self.db=DatabaseHelper()

    def post(self):
        data=request.get_json(force=True)
        self.load_data(data['source_id'],data['data_file'],data['business_date'])


    def load_data(self,source_id,data_file,business_date):

        table_name=self.get_table_name(source_id)
        if self.check_if_data_exists(source_id,business_date):
            abort(400,error_message="Data already loaded for the given business date")

        fs=self.get_file_seperator(source_id)
        col_list=self.get_col_list(table_name)
        chunksize=10

        for chunk in pd.read_csv(UPLOAD_FOLDER+data_file,sep=fs,index_col=0,chunksize=chunksize,na_filter=False):
           #print(chunk)
           found_col_list=[]
           for col in col_list:
               if col in chunk.columns.values:
                   found_col_list.append(col)

           if not found_col_list:
               abort(400, error_message="Table column names can not be matched to data file")

           sql = "insert into " + table_name + "("
           placeholders = "("

           for col in found_col_list:
                sql += col + ","
                placeholders += "%s,"
                #print(chunk[col])
           params_tuple=[]
           for index,row in chunk.iterrows():
                params_tuple.append(tuple(row[found_col_list]))

           placeholders = placeholders[:len(placeholders) - 1]
           placeholders += ")"
           sql = sql[:len(sql) - 1]
           sql += ") values " + placeholders
           print(sql)
           print(params_tuple)

           res= self.db.transactmany(sql, params_tuple)

        return res




    def check_if_data_exists(self,source_id,business_date):
        sql="select * from data_catalog where business_date=%s and source_id=%s and file_load_status=%s"
        file=self.db.query(sql,(business_date,source_id,'SUCCESS')).fetchone()

        return True if file else False

    def get_file_seperator(self,source_id):
        sql="select * from data_source_information where source_id=%s"
        source=self.db.query(sql,(source_id,)).fetchone()

        return source["source_file_delimiter"]

    def get_col_list(self,source_table_name):
        sql="select * from data_source_cols where source_table_name=%s order by id"
        columns=self.db.query(sql,(source_table_name,)).fetchall()

        col_list=[]
        for col in columns:
            col_list.append(col['column_name'])

        return col_list

    def get_table_name(self,source_id):
        sql = "select * from data_source_information where source_id=%s"
        source_table_name = self.db.query(sql, (source_id,)).fetchone()['source_table_name']
        return source_table_name









