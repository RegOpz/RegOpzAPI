from flask_restful import Resource
from Helpers.DatabaseHelper import DatabaseHelper
class MaintainBusinessRulesController(Resource):
     def get(self):
        return self.render_business_rules_json()
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
