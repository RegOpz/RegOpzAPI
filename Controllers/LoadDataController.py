from app import *
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
        return self.load_data(data['source_id'],data['data_file'],data['business_date'],data['file'])


    def load_data(self,source_id,data_file,business_date,selected_file):

        app.logger.info("Loading data")
        try:

            table_name=self.get_table_name(source_id)
            data_exists=self.check_if_data_exists(source_id,business_date)

            if data_exists:
                if 'SUCCESS' in data_exists['file_load_status']:
                    return {'msg': "Data already loaded for the given business date", 'filename': data_exists['data_file_name']}, 400
                elif data_exists['file_load_status']=='DATALOADING':
                    return {'msg': "Data loading currently running for the given business date", 'filename': data_exists['data_file_name']}, 400
                elif data_exists['file_load_status']=='APPLYRULES':
                    return {'msg': "Apply Business Rules currently running for the given business date", 'filename': data_exists['data_file_name']}, 400
                else:
                    self.update_data_catalog(source_id,business_date,selected_file,0,'','DATALOADING')
                    self.delete_existing_data(table_name,business_date)
            else:
                self.create_data_catalog(source_id,business_date,selected_file,0,'','DATALOADING')

            fs=self.get_file_seperator(source_id)
            col_list=self.get_col_list(table_name)
            chunksize=100000

            num_records=0
            file_load_status='DATALOADING-SUCCESS'
            for chunk in pd.read_csv(UPLOAD_FOLDER+data_file,sep=fs,index_col=0,chunksize=chunksize,na_filter=False):
               #print(chunk)
               found_col_list=[]
               for col in col_list:
                   if col in chunk.columns.values and col not in ['id','dml_allowed','in_use','last_updated_by']:
                       found_col_list.append(col)

               #print("found_col_list",found_col_list)
               if not found_col_list:
                   return {'msg': "Table column names can not be matched to data file", 'filename': data_exists['data_file_name']}, 400

               sql = "insert into " + table_name + "("
               placeholders = "("

               for col in found_col_list:
                    sql += col + ","
                    placeholders += "%s,"
                    #print(chunk[col])

               placeholders += "'Y','Y')"
               sql += "dml_allowed,in_use) values " + placeholders

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
                  app.logger.info("Another {} records uploaded...".format(chunk.shape[0]))

               except Exception as e:
                  file_load_status='DATALOADING-FAILED'
                  app.logger.error(e)
                  break


            self.update_data_catalog(source_id,business_date,selected_file,num_records,'['+data_file+']'+str(header_row),file_load_status)

            if "SUCCESS" in file_load_status:
                print("Data Loaded Successfully")
                return {'msg': 'Data Loaded Successfully', 'filename': data_file}, 200
            else:
                return {'msg': 'Data load failure, please check.', 'filename': data_file}, 400
        except Exception as e:
            app.logger.error(e)
            return {"msg":e, 'filename': data_file},500



    def create_data_catalog(self,source_id,business_date,file_name,number_of_rows,header_row,file_load_status):
        app.logger.info("Creating data catalog entry")
        sql="insert into data_catalog(source_id,business_date,data_file_name,number_of_rows,file_load_status,\
        header_row,data_loaded_by,timestamp) values(%s,%s,%s,%s,%s,%s,%s,%s)"
        params=(source_id,business_date,file_name,number_of_rows,file_load_status,str(header_row),None,datetime.now())

        res=self.db.transact(sql,params)
        self.db.commit()
        #return res

    def update_data_catalog(self,source_id,business_date,file_name,number_of_rows,header_row,file_load_status):
        app.logger.info("Uploading data catalog entry")
        sql="update data_catalog set data_file_name=%s,number_of_rows=%s,file_load_status=%s,\
        header_row=%s,data_loaded_by=%s,timestamp=%s where source_id= %s and business_date=%s"
        params=(file_name,number_of_rows,file_load_status,str(header_row),None,datetime.now(),source_id,business_date)

        res=self.db.transact(sql,params)
        self.db.commit()
        #return res

    def delete_existing_data(self,table_name,business_date):
        app.logger.info("Deleting existing data")
        sql="delete from "+table_name+" where business_date=%s"
        self.db.transact(sql,(str(business_date),))
        self.db.commit()

    def check_if_data_exists(self,source_id,business_date):

        app.logger.info("Checking if data already exists for source {0} and date {1}".format(source_id,business_date))
        sql="select * from data_catalog where business_date=%s and source_id=%s"
        file=self.db.query(sql,(business_date,source_id)).fetchone()

        return file

    def get_file_seperator(self,source_id):
        app.logger.info("Getting the file seperator for source {}".format(source_id))
        sql="select * from data_source_information where source_id=%s"
        source=self.db.query(sql,(source_id,)).fetchone()

        return source["source_file_delimiter"]

    def get_col_list(self,source_table_name):
        app.logger.info("Getting column list for source table {}".format(source_table_name))
        sql="select * from data_source_cols where source_table_name=%s order by id"
        columns=self.db.query(sql,(source_table_name,)).fetchall()

        col_list=[]
        for col in columns:
            col_list.append(col['column_name'])

        return col_list

    def get_table_name(self,source_id):
        app.logger.info("Getting table name for source {}".format(source_id))
        sql = "select * from data_source_information where source_id=%s"
        source_table_name = self.db.query(sql, (source_id,)).fetchone()['source_table_name']
        return source_table_name
