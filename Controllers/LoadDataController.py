from flask_restful import Resource,abort
from flask import Flask, request, redirect, url_for
from Helpers.DatabaseHelper import DatabaseHelper
import pandas as pd
from datetime import datetime

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
        else:
            self.delete_existing_data(table_name,business_date)

        fs=self.get_file_seperator(source_id)
        col_list=self.get_col_list(table_name)
        chunksize=10

        num_records=0
        file_load_status='SUCCESS'
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

           placeholders = placeholders[:len(placeholders) - 1]
           placeholders += ")"
           sql = sql[:len(sql) - 1]
           sql += ") values " + placeholders

           params_tuple=[]
           for index,row in chunk.iterrows():
                params_tuple.append(tuple(row[found_col_list]))

           #print(sql)
           #print(params_tuple)
           header_row = chunk.columns.values
           try:
              res= self.db.transactmany(sql, params_tuple)
              self.db.commit()
              num_records += chunk.shape[0]
              print("Another "+chunk.shape[0]+" records uploaded....")
           except Exception as e:
              file_load_status='FAILED'
              #print(e)
              break

        self.create_data_catalog(source_id,business_date,data_file,num_records,header_row,file_load_status)
        return res

    def create_data_catalog(self,source_id,business_date,file_name,number_of_rows,header_row,file_load_status):
        sql="insert into data_catalog(source_id,business_date,data_file_name,number_of_rows,file_load_status,\
        header_row,data_loaded_by,timestamp) values(%s,%s,%s,%s,%s,%s,%s,%s)"
        params=(source_id,business_date,file_name,number_of_rows,file_load_status,str(header_row),None,datetime.now())

        res=self.db.transact(sql,params)
        return res


    def delete_existing_data(self,table_name,business_date):
        sql="delete from "+table_name+" where business_date=%s"
        self.db.transact(sql,(str(business_date),))
        self.db.commit()


    def check_if_data_exists(self,source_id,business_date):
        sql="select * from data_catalog where business_date=%s and source_id=%s"
        file=self.db.query(sql,(business_date,source_id)).fetchone()

        return True if file and file['file_load_status']=='SUCCESS' else False

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









