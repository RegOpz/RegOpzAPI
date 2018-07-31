
from flask_restful import Resource
import time
import re
from datetime import datetime
from app import *
import Helpers.utils as util
from Helpers.DatabaseHelper import DatabaseHelper
import json
import random
from bcrypt import hashpw, gensalt
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *
from Controllers.OperationalLogController import OperationalLogController
from flask_mail import Mail, Message
from Configs import mailconfig


class PasswordRecoveryController(Resource):
    def __init__(self):
        self.dbs = DatabaseHelper()
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

        if request.endpoint == 'check_answers_ep':
            req_data = request.get_json(force = True)
            return self.check_answers(req_data)

        if request.endpoint == 'capture_security_answers_ep':
            req_data = request.get_json(force = True)
            return self.capture_sec_que_ans(req_data)

    def put(self):
        if request.endpoint == 'edit_password_policy_ep':
            req_data = request.get_json(force = True)
            return self.edit_password_policy(req_data)

        if requestself.endpoint ==  'change_pwd_ep':
            req_data = request.get_json(force = True)
            return self.change_pwd(req_data)


    def get(self, id = None):
        if request.endpoint == 'get_users_security_questions_ep':
            return self.get_questions_pwd_recovery(id)

        if request.endpoint == 'get_all_security_questions_ep':
            return self.get_all_questions()

        # if request.endpoint == 'match_answers_ep':
        #     req_data = request.get_json(forec = True)
        #     return self.capture_sec_que_ans(req_data)

    def get_all_questions(self):
        try:
            sql = 'select * from sec_questions'
            data = self.dbs.query(sql).fetchall()
            return data
        except Exception as e:
            app.logger.error(e.__str__())
            return (e.__str__())

    def change_pwd(self, req_data):
        try:
            user_id                 = req_data['username']
            password                = req_data['password']
            password_cnfrmation     = req_data['passwordConfirm']
            id                      = self.domain_info['tenant_id']
            if password_cnfrmation == password:

                sql = "select not_use_last from pwd_policy where  tenant_id = %s"
                num = self.dbs.query(sql,(id,)).fetchone()
                sql = "select hash_of_last_passwords from user_pwd_details where user_id = %s"
                pwd_hash = self.db.query(sql,(user_id)).fetchone()
                entered_pwd = password.encode('utf-8')
                for i in range num:
                    if pwd_hash[i]:
                        p_hash = pwd_hash[i].encode('utf-8')
                        if hashpw(entered_pwd, p_hash) == p_hash:
                            return {'msg': 'can not use last used passwords, Please try with new one'}, 500
                    else:
                        break
                in_use = pwd_hash['in_use']
                in_use = (in_use + 1) % num

                hash = hashpw(password.encode('utf-8'), gensalt())
                tym = datetime.now()
                sql = "update regopzuser set password = %s, pwd_change_tym = %s  where name = %s"
                self.db.transact(sql, (hash, tym, user_id))
                pwd_hash['in_use'] = in_use
                pwd_hash[in_use] = hash
                sql = "update user_pwd_details set hash_of_last_passwords = %s where user_id = %s"
                self.db.transact(sql,(pwd_hash, user_id))
                self.db.commit()
                return {'msg': 'Password Changes Successfully'}, 200
        except Exception as e:
             app.logger.error(e.__str__())
             return (e.__str__())



    def check_answers(self, req_data):
        try:
            user_id         = req_data['usernamename']
            #que_list        = req_data['que_id_list']
            answers         = req_data['answers']
            que_list        = answers.keys()
            #que_list        = json.loads(que_list)

            sql = "select answers from user_pwd_details where user_id = %s"
            ans = self.db.query(sql,(user_id,))
            for val in que_list:
                entered_answer = answers['val'].encode('utf-8')
                hashed_answer = ans[str(val)].encode('utf-8')
                if hashpw(entered_answer, hashed_answer) != hashed_answer:
                    return {'msg': 'Security Answers not matched, Please try again'}, 500

            return {'msg': 'Security Answers matched'}, 200
        except Exception as e:
            app.logger.error(e.__str__())
            return (e.__str__())

    def send_otp(self, req_data):
        try:
            user_name = req_data['username']
            sql = "select email from regopzuser where name = '{}'".format(username)
            email = self.db.transact(sql).fetchone()
            self.otp = str((uuid.uuid4()).int)[0:5]

            app.config.update(
            	DEBUG = True,
            	MAIL_SERVER = mailconfig.MAIL_CONFIG['mail_server'],
            	MAIL_PORT = mailconfig.MAIL_CONFIG['mail_port'],
            	MAIL_USE_SSL = True,
            	MAIL_USERNAME = mailconfig.MAIL_CONFIG['mail_username'],
            	MAIL_PASSWORD = mailconfig.MAIL_CONFIG['mail_password']
            )
            msg = Message('Sending mail from my applcation!',
            sender =  'xyz@xyz.com',
            recipients = [email])
            msg.body = "Enter The following OTP to reset your password '{}'".format(otp)
            mail = Mail(app)
            mail.send(msg)
            app.logger.info('OTP Sent')
            return {'Status': 'SUCCESS', 'message': 'OTP SENT'}
        except Exception as e:
            app.logger.error(e.__str__())
            return (e.__str__())

    # def reset_password_otp(self, req_data):
    #     try:
    #         print('WIll do it later')
    #     except Exception as e:
    #         app.logger.error(e.__str__())
    #         return (e.__str__())

    def capture_sec_que_ans(self, req_data):
        try:

            uder_id         = req_id['username']
            # que_id_list     = req_id['que_id_list']
            # que_id_str      = json.dumps(que_id_list)
            answers         =  req_id['answers']
            que_id_list     =  answers.keys()
            id              =  self.domain_info['tenant_id']
            que_id_str      =  json.dumps(que_id_list)
            hash_pwd        =  {'in_use': -1 }
            # For creating the dictionary for storing hashed password.
            self.dbs        = DatabaseHelper()
            # sql = "select not_use_last from pwd_policy where  tenant_id = %s"
            # num = self.dbs.query(sql,(id,)).fetchone()
            # for i in range(num):
            #     hash_pwd[i] = ''

            #For creating hash of answers.
            for val in que_id_list:
                ans = answers[val]
                hash_ans = hashpw(ans.encode('utf-8'), gensalt())
                answers[val] = hash_ans

            sql = "insert into user_pwd_details (user_id, que_id_list, answers, hash_of_last_passwords)\
                    values(%s, %s, %s, %s)"

            self.db.transact(sql,(user_id, que_id_str, answers, hash_pwd))
            return {'msg':'Security questions added'}, 200
        except Exception as e:
            app.logger.error(e.__str__())
            return (e.__str__())

    def edit_password_policy(self, req_data):
        try:
            #self.dbs = DatabaseHelper()
            app.logger.info('capturing password policy')
            ## What will be the request format for edit_passsword_policy
            ## _____________________TODO_______________________________
        except Exception as e:
            app.logger.error(e.__str__())
            return (e.__str__())


    def capture_password_policy(self, req_data):
        try:


            app.logger.info('capturing password policy')
            id            = self.domain_info['tenant_id']
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

            sql = 'insert into pwd_policy(tenant_id, reg_exp, num_ans_to_capture, num_ques_to_throw,\
                    not_use_last, expiry_period) values(%s, %s, %s, %s, %s, %s)'

            self.dbs.transact(sql, (id, reg_exp, num_ans_to_capture, num_ques_to_throw, not_use_last,expiry_period))
            self.dbs.commit()
            app.logger.info('password policy captured successfully')
            return{'msg': 'Password policy captured successfully'}, 200
        except Exception as e:
            app.logger.error(e.__str__())
            return (e.__str__())



    def get_questions_pwd_recovery(self, username):
        try:
            #self.dbs = DatabaseHelper()
            app.logger.info("Getting list of security questions")
            id = self.domain_info['tenant_id']
            sql = "select num_ques_to_throw from pwd_policy where tenant_id = %s"
            num = self.dbs.query(sql,(id,)).fetchone()
            num = num['num_ques_to_throw']

            sql = "select que_id_list from user_pwd_details where username = %s"
            id_str = self.db.query(sql,(username, ))

            id_lst = json.loads(id_str)
            random.shuffle(id_lst)
            id_lst = id_lst[0:num]
            id_lst = tuple(a)
            sql = "select questions from sec_questions where id in id_lst"
            data = self.db.query(sql).fetchall()
            return data
            # sql = "select Q1, Q2, Q3, Q4, Q5, num_qs from password_policy where tenant_id = '{}'".format(id)
            # data = self.dbs.query(sql).fetchone()
            # num = data['no_qs']
            # del data[num_qs]
            # keys = random.sample(list(d), num)
            #return data
        except Exception as e:
            app.logger.error(e.__str__())
            return (e.__str__())
