from flask_restful import Resource
import time
import re
import uuid
from datetime import datetime, timedelta
from app import *
import Helpers.utils as util
from Helpers.DatabaseHelper import DatabaseHelper
import json
import random
from bcrypt import hashpw, gensalt
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *
from Controllers.DefChangeController import DefChangeController
from flask_mail import Mail, Message
from Configs import mailconfig


class PasswordRecoveryController(Resource):
    def __init__(self):
        self.dbm = DatabaseHelper()
        self.domain_info = autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_id = tenant_info['tenant_id']
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.db=DatabaseHelper(self.tenant_info)
            # self.user_id=Token().authenticate()
            self.dcc=DefChangeController(tenant_info=self.tenant_info)


    def get(self, username = None):
        if request.endpoint == 'get_all_security_questions_ep':
            return self.get_all_questions()
        if request.endpoint == 'get_user_security_questions_ep':
            return self.get_pwd_recovery_questions(username)
        if request.endpoint == 'generate_pwd_recovery_otp_ep':
            return self.generate_otp(username)
        if request.endpoint == 'get_password_policy_ep':
            return self.get_password_policy()

    def post(self):
        if request.endpoint == 'validate_pwd_recovery_answers_ep':
            data = request.get_json(force=True)
            return self.validate_pwd_recovery_answers(data)
        if request.endpoint == 'validate_pwd_recovery_otp_ep':
            data = request.get_json(force=True)
            return self.validate_otp(data)

    def put(self, id=None):
        data = request.get_json(force=True)
        if id == None:
            return {'msg': 'No data found as record id not supplied'}, 500
        return self.update_password_policy(data, id)



    def get_all_questions(self):
        try:
            sql = "select * from pwd_policy where tenant_id=%s and in_use='Y'"
            policy = self.db.query(sql,(self.tenant_id,)).fetchone()
            if not policy:
                app.logger.info('No password policy defined for {}. Creating default passwordpolicy.'.format(self.tenant_id))
                self.create_default_password_policy(tenant_id=self.tenant_id)
                policy = self.db.query(sql,(self.tenant_id,)).fetchone()

            restrictions = json.loads(policy['pwd_restrictions'])
            reg_exp = restrictions['reg_exp']
            policy_statement = "Password should have "
            for k in restrictions.keys():
                if k != 'reg_exp' :
                    if k=='length':
                        policy_statement += (" length >= " + str(restrictions[k]) + " characters, ") if restrictions[k] else ""
                    else:
                        policy_statement += (" " + k.replace('_',' ') + ",") if restrictions[k] else ""

            sql = 'select * from sec_questions'
            data = self.dbm.query(sql).fetchall()
            random.shuffle(data)
            policy_data = {
                            'reg_exp': reg_exp,
                            'policy_statement': policy_statement,
                            'questions': data[0:policy['no_ans_to_capture']]
                            }
            return policy_data
        except Exception as e:
            app.logger.error(str(e))
            return {'msg': str(e)}, 500


    def create_default_password_policy(self, tenant_id=None, dbcon=None):
        try:

            app.logger.info('Creating default password policy')
            if dbcon:
                self.db = dbcon

            sql = 'insert into pwd_policy( ' + \
                                    	'tenant_id, ' + \
                                    	'no_ans_to_capture, ' + \
                                    	'no_ques_to_throw, ' + \
                                    	'pwd_restrictions, ' + \
                                    	'no_prev_pwd_not_to_use, ' + \
                                    	'expiry_period, ' + \
                                    	'dml_allowed, ' + \
                                    	'in_use)  ' + \
                                        'values(%s, %s, %s, %s, %s, %s, %s, %s)'
            params = ( self.tenant_id if not tenant_id else tenant_id,\
                        5,\
                        2,\
                        json.dumps({'lowercase': True, 'uppercase':True, 'number':True, 'special_char':True, 'length': 8, 'reg_exp':"(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9])(?=.*[!@#\$%\^&\*])(?=.{8,})"}),
                        3,\
                        90,\
                        'Y',\
                        'Y'\
                        )

            self.db.transact(sql, params)
            self.db.commit()
            app.logger.info('Default password policy created successfully')
        except Exception as e:
            app.logger.error(str(e))
            raise e

    def save_sec_question_answers(self, data, dbcon=None):
        try:
            if dbcon:
                self.db = dbcon

            # fields passed from GUI as ans_<sec question id>, so loop through to check the security sec_questions
            answers={}
            for k in data.keys():
                if k[0:4] =='ans_':
                     # Its a answer field for security question
                     id = k.replace('ans_','')
                     hash_ans = hashpw(data[k].encode('utf-8'), gensalt())
                     # Convert hash byte to string to facilitate JSON dumps
                     answers[id]=hash_ans.decode('utf-8')
                     # app.logger.info("save sec answer {}".format(answers) )

            # save changes only when there are changed answers
            if answers !={} :
                sql = "select * from user_pwd_recovery_details where user_name=%s and details_type='QA'"
                params = (data['name'],)

                qa = self.db.query(sql,params).fetchone()
                if qa:
                    sql = "update user_pwd_recovery_details set details_data=%s where user_name=%s and details_type='QA'"
                    params = (json.dumps(answers),data['name'])
                else:
                    sql = "insert into user_pwd_recovery_details (user_name, details_type, details_data) " + \
                            "values(%s, %s, %s)"
                    params = (data['name'],'QA',json.dumps(answers))

                self.db.transact(sql,params)

            self.save_password_change_date(data['name'],self.db)

            return {'msg':'Security questions saved successfully'}, 200
        except Exception as e:
            app.logger.error(str(e))
            raise e


    def get_pwd_recovery_questions(self, username):
        try:
            #self.dbm = DatabaseHelper()
            app.logger.info("Getting list of security questions for {}".format(username))
            sql = "select * from pwd_policy where tenant_id = %s and in_use='Y'"
            policy = self.db.query(sql,(self.tenant_id,)).fetchone()
            no_of_ques = policy['no_ques_to_throw']

            sql = "select * from user_pwd_recovery_details where user_name=%s and details_type='QA'"
            recovery = self.db.query(sql,(username, )).fetchone()

            if not recovery:
                return {'msg': 'No security question found for user {}'.format(username,),
                        'status': False, }, 400

            answers = json.loads(recovery['details_data'])
            question_ids = list(answers.keys())
            random.shuffle(question_ids)
            question_ids = ",".join(map(str,question_ids[0:no_of_ques]))
            sql = "select * from sec_questions where id in ({})".format(question_ids)
            data = self.dbm.query(sql).fetchall()
            return {'questions': data}
        except Exception as e:
            app.logger.error(str(e))
            return {'msg' : str(e)}, 500

    def validate_pwd_recovery_answers(self, data):
        try:
            app.logger.info("Getting saved security answers")

            sql = "select * from user_pwd_recovery_details where user_name=%s and details_type='QA'"
            recovery = self.db.query(sql,(data['name'], )).fetchone()

            answers = json.loads(recovery['details_data'])
            for k in data.keys():
                if k[0:4] =='ans_':
                     # Its a answer field for security question
                     id = k.replace('ans_','')
                     entered_answer = data[k].encode('utf-8')
                     saved_answer = answers[id].encode('utf-8')
                     if hashpw(entered_answer, saved_answer) != saved_answer:
                         return {'msg': 'Security answers could not be matched ! Please try again.',
                                 "donotUseMiddleWare": True,
                                 'status': False,
                                 'name': data['name']
                                 }, 200
            return {'msg': 'Security questions validated successfully',
                    "donotUseMiddleWare": True,
                    'status': True,
                    'name': data['name']
                    }, 200
        except Exception as e:
            app.logger.error(str(e))
            return {'msg' : str(e), 'status': False}, 500


    def validate_pwd(self, data, dbcon=None):
        try:
            if dbcon:
                self.db = dbcon

            username                 = data['name']
            password                = data['password']
            prvPassword = False

            sql = "select * from pwd_policy where  tenant_id = %s and in_use='Y'"
            policy = self.db.query(sql,(self.tenant_id,)).fetchone()

            sql = "select * from user_pwd_recovery_details where user_name=%s and details_type='PWD'"
            params = (username,)
            pwd_list = self.db.query(sql,params).fetchone()
            password_entered = password.encode('utf-8')
            password_entered_hash = hashpw(password_entered, gensalt()).decode('utf-8')

            if pwd_list:
                # check if it matches any of the previous passwords
                prvPwd=json.loads(pwd_list['details_data'])
                for p in prvPwd:
                    previous_password=p.encode('utf-8')
                    if hashpw(password_entered,previous_password)==previous_password:
                        prvPassword = True
                        break
                #If prvPassword is False after the loop that means its a valid password to be changed
                # We keep prv passwords FIFO method
                if not prvPassword:
                    prvPwd.append(password_entered_hash)
                    if len(prvPwd) > policy['no_prev_pwd_not_to_use']:
                        del prvPwd[0]

                    sql = "update user_pwd_recovery_details set details_data=%s where user_name=%s and details_type='PWD'"
                    params = (json.dumps(prvPwd),username,)

                    self.db.transact(sql,params)
            else:
                prvPwd = [password_entered_hash]

                sql = "insert into user_pwd_recovery_details (user_name, details_type, details_data) " + \
                        "values(%s, %s, %s)"
                params = (username,'PWD',json.dumps(prvPwd))

                self.db.transact(sql,params)

            self.save_password_change_date(username,self.db)

            return prvPassword
        except Exception as e:
             app.logger.error(str(e))
             raise e

    def save_password_change_date(self,username,dbcon):
        try:
            # Now set the password change date for the user
            sql = "select * from user_pwd_recovery_details where user_name=%s and details_type='PWDCHNGDATE'"
            params = (username,)
            pwd_change_date = dbcon.query(sql,params).fetchone()

            if pwd_change_date:
                sql = "update user_pwd_recovery_details set details_data=%s where user_name=%s and details_type='PWDCHNGDATE'"
                params = (datetime.now().strftime('%Y%m%d'),username,)

                dbcon.transact(sql,params)
            else:
                sql = "insert into user_pwd_recovery_details (user_name, details_type, details_data) " + \
                        "values(%s, %s, %s)"
                params = (username,'PWDCHNGDATE',datetime.now().strftime('%Y%m%d'))

                dbcon.transact(sql,params)
        except Exception as e:
            app.logger.error(str(e))
            raise e

    def generate_otp(self, username):
        try:

            sql = "select * from regopzuser where name = %s"
            user = self.db.query(sql,(username, )).fetchone()
            generated_otp = uuid.uuid4().hex[0:8]
            app.logger.info("Generated new OTP is : {}".format(generated_otp))
            hash_otp = hashpw(generated_otp.encode('utf-8'), gensalt())
            new_otp = {
                        'OTP' : hash_otp.decode('utf-8')
                        }

            sql = "select * from user_pwd_recovery_details where user_name=%s and details_type='OTP'"
            params = (username,)

            otp = self.db.query(sql,params).fetchone()
            if otp:
                sql = "update user_pwd_recovery_details set details_data=%s where user_name=%s and details_type='OTP'"
                params = (json.dumps(new_otp),username)
            else:
                sql = "insert into user_pwd_recovery_details (user_name, details_type, details_data) " + \
                        "values(%s, %s, %s)"
                params = (username,'OTP',json.dumps(new_otp))

            self.db.transact(sql,params)
            self.send_otp_email(user,generated_otp)
            self.db.commit()
            return {'msg': 'OTP generated successfully',
                    "donotUseMiddleWare": False,
                    'status': True,
                    'name': username
                    }, 200
        except Exception as e:
             app.logger.error(str(e))
             return {'msg': 'OTP could not be generated !' + str(e),
                    'status': False}, 500

    def send_otp_email(self, user, new_otp):
        try:
            mail_settings={
            	"DEBUG" : True,
            	"MAIL_SERVER" : mailconfig.MAIL_CONFIG['mail_server'],
            	"MAIL_PORT" : mailconfig.MAIL_CONFIG['mail_port'],
                "MAIL_USE_TLS" : False,
            	"MAIL_USE_SSL" : True,
            	"MAIL_USERNAME" : mailconfig.MAIL_CONFIG['mail_username'],
            	"MAIL_PASSWORD" : mailconfig.MAIL_CONFIG['mail_password'],
            }
            app.config.update(mail_settings)
            msg = Message( subject='RegOpz!',
                            sender =  ("RegOpz Mailer","donot-reply"),
                            reply_to =  'donot-reply@regopz.com',
                            recipients = [user['email']])
            msg.body = ("Hi {0},\n" + \
                        "Please use the following OTP to reset password:\n\n" + \
                        "{1} \n\n" + \
                        "Thanks,\n" + \
                        "Team RegOpz").format(user['first_name'],new_otp)
            mail = Mail(app)
            mail.send(msg)
            app.logger.info('OTP Sent for {}'.format(user['name']))

        except Exception as e:
            app.logger.error(str(e))
            raise e

    def validate_otp(self, data):
        try:
            app.logger.info("Getting saved OTP")

            sql = "select * from user_pwd_recovery_details where user_name=%s and details_type='OTP'"
            recovery = self.db.query(sql,(data['name'], )).fetchone()

            if not recovery:
                return {'msg': 'No valid OTP found ! Please try after generating OTP again.',
                         "donotUseMiddleWare": True,
                         'status': False,
                         'name': data['name']
                         }, 200
            otp = json.loads(recovery['details_data'])
            if not otp['OTP']:
                return {'msg': 'No valid OTP found ! Please try after generating OTP again.',
                         "donotUseMiddleWare": True,
                         'status': False,
                         'name': data['name']
                         }, 200
            entered_otp = data['otp'].encode('utf-8')
            saved_otp = otp['OTP'].encode('utf-8')
            if hashpw(entered_otp, saved_otp) != saved_otp:
                return {'msg': 'OTP could not be matched ! Please try again.',
                         "donotUseMiddleWare": True,
                         'status': False,
                         'name': data['name']
                         }, 200
            sql = "update user_pwd_recovery_details set details_data=%s where user_name=%s and details_type='OTP'"
            params = (json.dumps({'OTP':None}),data['name'])

            self.db.transact(sql,params)
            self.db.commit()
            return {'msg': 'OTP validated successfully',
                    "donotUseMiddleWare": True,
                    'status': True,
                    'name': data['name']
                    }, 200
        except Exception as e:
            app.logger.error(str(e))
            return {'msg' : str(e), 'status': False}, 500

    def check_password_expiry(self, username):
        try:
            sql = "select * from pwd_policy where tenant_id=%s and in_use='Y'"
            policy = self.db.query(sql,(self.tenant_id,)).fetchone()

            expiry_period = 90
            if policy:
                expiry_period = int(policy['expiry_period'])

            sql = "select * from user_pwd_recovery_details where user_name=%s and details_type='PWDCHNGDATE'"
            params = (username,)

            dp = self.db.query(sql,params).fetchone()
            if dp:
                pwd_change_date = datetime.strptime(dp['details_data'],'%Y%m%d')
                is_password_expired = (pwd_change_date + timedelta(expiry_period) < datetime.now())
                return is_password_expired
            else:
                return True

        except Exception as e:
            app.logger.error(str(e))
            raise e

    def get_password_policy(self):
        try:
            sql = "select * from pwd_policy where tenant_id=%s and in_use='Y'"
            policy = self.db.query(sql,(self.tenant_id,)).fetchone()
            if not policy:
                app.logger.info('No password policy defined for {}. Creating default passwordpolicy.'.format(self.tenant_id))
                self.create_default_password_policy(tenant_id=self.tenant_id)
                policy = self.db.query(sql,(self.tenant_id,)).fetchone()

            return policy
        except Exception as e:
            app.logger.error(str(e))
            return {'msg': str(e)}, 500

    def update_password_policy(self,data,id):
        app.logger.info("Updating Password Policy data")
        try:
            res = self.dcc.update_or_delete_data(data, id)
            return res
        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500
