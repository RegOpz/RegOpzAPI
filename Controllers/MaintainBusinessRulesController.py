from flask import Flask, jsonify, request
from flask_restful import Resource
from Report_API.RegOpzAPI2.RegOpzAPI.Helpers.DatabaseHelper import DatabaseHelper
import csv
class MaintainBusinessRulesController(Resource):
     def get(self):
        return self.render_business_rules_json()

     def post(self):
         br = request.get_json(force=True)
         res=self.insert_business_rules(br)
         return res

     def put(self):
         br=request.get_json(force=True)
         res=self.update_business_rules(br)
         return res

     def delete(self):
        br=request.get_json(force=True)
        res=self.delete_business_rules(br)
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

        # print(json_dump)

        return json_dump

     def insert_business_rules(self,br):
         db=DatabaseHelper()
         sql= "insert into business_rules(rule_execution_order,business_rule,source_id,rule_description,\
                        logical_condition,data_fields_list,python_implementation,business_or_validation,\
                    rule_type,valid_from,valid_to) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

         params= tuple((br["rule_execution_order"],br["business_rule"],br["source_id"],br["rule_description"], \
                     br["logical_condition"],br["data_fields_list"],br["python_implementation"],br["business_or_validation"], \
                     br["rule_type"],br["valid_from"],br["valid_to"]))

         res=db.transact(sql,params)

         return res

     def update_business_rules(self,br):
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
             valid_to=%s where business_rule=%s"

         params=tuple((br["rule_execution_order"],br["business_rule"],br["source_id"],br["rule_description"], \
                     br["logical_condition"],br["data_fields_list"],br["python_implementation"],br["business_or_validation"], \
                     br["rule_type"],br["valid_from"],br["valid_to"],br["business_rule"]))

         res=db.transact(sql,params)

         return res

     def delete_business_rules(self,br):
         db=DatabaseHelper()
         sql="delete from business_rules where business_rule=%s"
         params=(br["business_rule"],)

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



