from flask import Flask, jsonify, request
from flask_restful import Resource
from RegOpzAPI.Helpers.DatabaseHelper import DatabaseHelper
import csv
from Constants.Status import *


class MaintainBusinessRulesController(Resource):
<<<<<<< 420f1a37af5b3c3a8bdd87ca63468634ca44d47e
     def get(self, id=None):
        if id :
             return self.render_business_rule_json(id)
        return self.render_business_rules_json()

     def post(self):
         br = request.get_json(force=True)
         res=self.insert_business_rules(br)
         return res

     def put(self, id=None):
         if(id == None):
             return BUSINESS_RULE_EMPTY
         br=request.get_json(force=True)
         res=self.update_business_rules(br, id)
         return res

     def delete(self, id=None):
         if(id == None):
             return BUSINESS_RULE_EMPTY
         res=self.delete_business_rules(id)
         return res

     def render_business_rules_json(self):
        db=DatabaseHelper()

        business_rules_dict={}
        business_rules_list=[]
        cur = db.query("select * from business_rules")
        business_rules = cur.fetchall()

        cols=[i[0] for i in cur.description]
        business_rules_dict['cols']=cols
        # print(business_rules_dict)

        for br in business_rules:
            business_rules_list.append(br)

        business_rules_dict['rows']=business_rules_list

        json_dump=(business_rules_dict)

        return json_dump

     def render_business_rule_json(self, id):
        db=DatabaseHelper()
        query = "select * from business_rules where id = %s"
        cur = db.query(query, (id, ))
        data = cur.fetchone()
        if data :
            return data
        return NO_BUSINESS_RULE_FOUND

     def insert_business_rules(self,br):
         db=DatabaseHelper()

         sql = "insert into business_rules(rule_execution_order,business_rule,source_id,rule_description,\
                                 logical_condition,data_fields_list,python_implementation,business_or_validation,\
                             rule_type,valid_from,valid_to) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

         if br:
           params= tuple((br["rule_execution_order"],br["business_rule"],br["source_id"],br["rule_description"], \
                     br["logical_condition"],br["data_fields_list"],br["python_implementation"],br["business_or_validation"], \
                     br["rule_type"],br["valid_from"],br["valid_to"]))

         else:
           params=(None,None,None,None,None,None,None,None,None,None,None)

         res=db.transact(sql,params)
         print("Result" +str(res))

         if res > 0 :
          return self.render_business_rule_json(res)
         return UPDATE_ERROR


     def update_business_rules(self,br,id):
         db=DatabaseHelper()
         sql="Update business_rules set " \
             "rule_execution_order=%s, \
             business_rule=%s,\
             source_id=%s,\
             rule_description=%s,\
             logical_condition=%s,\
             data_fields_list=%s,\
             python_implementation=%s,\
             business_or_validation=%s,\
             rule_type=%s,\
             valid_from=%s,\
             valid_to=%s where id=%s"

         params=(br["rule_execution_order"],br["business_rule"],br["source_id"],br["rule_description"], \
                     br["logical_condition"],br["data_fields_list"],br["python_implementation"],br["business_or_validation"], \
                     br["rule_type"],br["valid_from"],br["valid_to"],id )

         res=db.transact(sql,params)
         print("Result "+str(res))
         if(res == 0):
             return self.render_business_rule_json(id)
         return UPDATE_ERROR

     def delete_business_rules(self,id):
         db=DatabaseHelper()
         sql="delete from business_rules where id=%s"
         params=(id,)
         res=db.transact(sql,params)
         return res

     def export_to_csv(self,source_id='ALL'):
         db=DatabaseHelper()

         sql="select * from business_rules where 1=1"

         if source_id!='ALL':
             sql+=" and source_id='"+str(source_id)+"'"

         cur=db.query(sql)

         business_rules=cur.fetchall()
         #keys=business_rules[0].keys()
         keys=[i[0] for i in cur.description]

         with open("business_rules.csv","wt") as output_file:
             dict_writer=csv.DictWriter(output_file,keys)
             dict_writer.writeheader()
             dict_writer.writerows(business_rules)

     def list_reports_for_rule(**kwargs):

         parameter_list = ['business_rule']

         if set(parameter_list).issubset(set(kwargs.keys())):
             business_rule = kwargs['business_rule']

         else:
             print("Please supply parameters: " + str(parameter_list))
             sys.exit(1)

         db = DatabaseHelper()
         sql = "select distinct report_id,sheet_id,cell_id from report_calc_def \
                where cell_business_rules like  '%" + business_rule + "%'"
         cur=db.query(sql)
         report_list = cur.fetchall()

         return [(rpt['report_id'], rpt['sheet_id'], rpt['cell_id']) for rpt in report_list]

     def list_reports_for_rule_list(**kwargs):

         parameter_list = ['business_rule_list']

         if set(parameter_list).issubset(set(kwargs.keys())):
             business_rule_list = kwargs['business_rule_list']
         else:
             print("Please supply parameters: " + str(parameter_list))
             sys.exit(1)

         db=DatabaseHelper()
         sql = "select report_id,sheet_id,cell_id,cell_business_rules from report_calc_def"
         cur=db.query(sql)
         data_list = cur.fetchall()

         result_set = []
         for data in data_list:
             # print(data['cell_business_rules'])
             cell_rule_list = data['cell_business_rules'].split(',')
             # print(type(cell_rule_list))
             if set(business_rule_list).issubset(set(cell_rule_list)):
                 # print(data)
                 result_set.append(data)

         return result_set

