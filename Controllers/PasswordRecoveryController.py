
from flask_restful import Resource
import time
import re
from datetime import datetime
from app import *
import Helpers.utils as util
from Helpers.DatabaseHelper import DatabaseHelper
import json
import random
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *
from Controllers.OperationalLogController import OperationalLogController


class PasswordRecoveryController(Resource):
    def __init__(self):
        self.domain_info = autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.db=DatabaseHelper(self.tenant_info)
            self.user_id=Token().authenticate()


    def post(self):
        if request.endpoint == 'capture_password_policy_ep':
            req_data = request.get_json(force = True)
            return self.capture_password_policy(req_data)

    def put(self):
        if request.endpoint == 'edit_password_policy_ep':
            req_data = request.get_json(force = True)
            return self.edit_password_policy(req_data)

        if requestself.endpoint ==  'reset_password_using_seq_ques_ep':
            req_data = request.get_json(force = True)
            return self.reset_password_seq(req_data)


    def get(self):
        if request.endpoint == 'get_security_questions_ep':
            return self.get_questions()


    def reset_password_que(self, req_data):
        try:
            user_id         = req_data['usernamename']
            que_list        = req_data['que_id_list']
            answers         = req_data['answers']
            que_list        = json.loads(que_list)

            sql = "select answers from user_pwd_details where user_id = '{0}'".format(user_id)
            ans = self.db.query(sql)
            for val in que_list:
                entered_answer = answers['val'].encode('utf-8')
                hashed_answer = ans[str(val)].encode('utf-8')
                if hashpw(entered_password,hashed_answer) != hashed_answer:
                    return {'status': 'error', 'message' : 'Security Answers not matched, Please try again'}, 200

            return {'status': 'SUCCESS', 'message': 'Security Answers matched'}, 200
        except Exception as e:
            app.logger.error(e.__str__())
            return (e.__str__())

    def capture_sec_que_ans(self, req_data):
        try:

            uder_id         = req_id['username']
            que_id_list     = req_id['que_id_list']
            que_id_str      = json.dumps(que_id_list)
            answers         =  req_id['answers']
            id              = self.domain_info['tenant_id']
            hash_pwd        =  {'in_use': 0 }
            # For creating the dictionary for storing hashed password.
            self.dbs        = DatabaseHelper()
            sql = "select not_use_last from password_policy where  tenant_id = %s"
            num = self.dbs.query(sql,(id,)).fetchone()
            for i in range(num):
                hash_pwd[i] = ''

            #For creating hash of answers.
            for val in que_id_list:
                ans = answers[val]
                hash_ans = hashpw(ans.encode('utf-8'), gensalt())
                answers[val] = hash_ans

            sql = "insert into user_pwd_details (user_id, question_id_list, answers, hash_of_last_passwords)\
                    values('{0}', '{1}', '{2}', '{3}')".format(user_id, que_id_str, answers, hash_pwd)

            self.db.transact(sql,())
            return {'status':'SUCCESS', 'messsage':'Security questions added'}, 200
        except Exception as e:
            app.logger.error(e.__str__())
            return (e.__str__())

    def edit_password_policy(self, req_data):
        try:
            self.dbs = DatabaseHelper()
            app.logger.info('capturing password policy')
            ## What will be the request format for edit_passsword_policy
            ## _____________________TODO_______________________________
        except Exception as e:
            app.logger.error(e.__str__())
            return (e.__str__())


    def capture_password_policy(self, req_data):
        try:
            self.dbs = DatabaseHelper()

            app.logger.info('capturing password policy')
            id            = self.domain_info['tenant_id']
            # q1            = req_data['Q1']
            # q2            = req_data['Q2']
            # q3            = req_data['Q3']
            # q4            = req_data['Q4']
            # q5            = req_data['Q5']
            num_ans_to_capture     = req_data['num_ans_to_capture']
            num_qs_to_throw         = req_data['num_ques_to_throw']
            not_use_last            = req_data['not_use_last']
            expiry_period           = req_data['expiry_period']

            reg_exp = '/^'
            minm = req_data['minm_req']
            if minm['alpha'] == 'Y':
                reg_exp = reg_exp + '(?=.*[a-zA-Z])'

            if minm['numeric'] == 'Y':
                reg_exp = reg_exp + '(?=.*\d)'
            if minm['special'] == 'Y':
                reg_exp = reg_exp + '(?=.*[!#/$%&\?*@^_])'

            reg_exp = reg_exp + '{' + minm['length'] + ',}$/'
            print(reg_exp)

            sql = 'insert into password_policy(tenant_id, reg_exp, num_ans_to_capture, num_ques_to_throw,\
                    not_use_last, expiry_period) values(%s, %s, %s, %s, %s, %s)'

            self.dbs.transact(sql, (id, reg_exp, num_ans_to_capture, num_ques_to_throw, not_use_last,expiry_period))
            self.dbs.commit()
            app.logger.info('password policy captured successfully')
            return{'status': 'Password policy captured successfully'}, 200
        except Exception as e:
            app.logger.error(e.__str__())
            return (e.__str__())



    def get_questions_pwd_recovery(self, username):
        try:
            self.dbs = DatabaseHelper()
            app.logger.info("Getting list of security questions")
            id = self.domain_info['tenant_id']
            sql = "select num_ques_to_throw from password_policy where tenant_id = %s"
            num = self.dbs.quert(sql,(id,)).fetchone()
            num = num['num_ques_to_throw']

            sql = "select que_id_list from user_pwd_details where username = %s"
            id_str = self.db.query(sql,(username, ))

            id_lst = json.loads(id_str)
            random.shuffle(id_lst)
            id_lst = id_lst[0:num]
            sql = "select questions from security_questions where id in id_lst"
            # sql = "select Q1, Q2, Q3, Q4, Q5, num_qs from password_policy where tenant_id = '{}'".format(id)
            # data = self.dbs.query(sql).fetchone()
            # num = data['no_qs']
            # del data[num_qs]
            # keys = random.sample(list(d), num)
            return data
        except Exception as e:
            app.logger.error(e.__str__())
            return (e.__str__())
