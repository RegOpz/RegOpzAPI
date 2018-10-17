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
# from flask_mail import Mail, Message
# from Configs import mailconfig


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


    def get(self, username = None):
        if request.endpoint == 'get_all_security_questions_ep':
            return self.get_all_questions()
        if request.endpoint == 'get_user_security_questions_ep':
            return self.get_pwd_recovery_questions(username)

    def post(self):
        if request.endpoint == 'validate_pwd_recovery_answers':
            data = request.get_json(force=True)
            return self.validate_pwd_recovery_answers(data)



    def get_all_questions(self):
        try:
            sql = "select * from pwd_policy where tenant_id=%s and in_use='Y'"
            policy = self.dbm.query(sql,(self.tenant_id,)).fetchone()
            if not policy:
                return {'msg': 'No password policy defined for {}. Please contact administrator.'.format(self.tenant_id)}, 500
            restrictions = json.loads(policy['pwd_restrictions'])
            reg_exp = restrictions['reg_exp']
            policy_statement = "Password should include "
            for k in restrictions.keys():
                if k != 'reg_exp' :
                    if k=='length':
                        policy_statement += (" length >= " + str(restrictions[k]) + " characters, ") if restrictions[k] else ""
                    else:
                        policy_statement += (" " + k.replace('_',' ') + ",") if restrictions[k] == 'Y' else ""

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
                self.dbm = dbcon

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
                        json.dumps({'lowercase': 'Y', 'uppercase':'Y', 'number':'Y', 'special_char':'Y', 'length': 8, 'reg_exp':"(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9])(?=.*[!@#\$%\^&\*])(?=.{8,})"}),
                        3,\
                        90,\
                        'Y',\
                        'Y'\
                        )

            self.dbm.transact(sql, params)
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
            return {'msg':'Security questions saved successfully'}, 200
        except Exception as e:
            app.logger.error(str(e))
            raise e


    def get_pwd_recovery_questions(self, username):
        try:
            #self.dbm = DatabaseHelper()
            app.logger.info("Getting list of security questions for {}".format(username))
            sql = "select * from pwd_policy where tenant_id = %s"
            policy = self.dbm.query(sql,(self.tenant_id,)).fetchone()
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

            sql = "select * from pwd_policy where  tenant_id = %s"
            policy = self.dbm.query(sql,(self.tenant_id,)).fetchone()

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

            return prvPassword
        except Exception as e:
             app.logger.error(str(e))
             raise e

    def generate_otp(self, username):
        try:

            sql = "select * from regopzuser where name = %s"
            user = self.db.transact(sql,(username, )).fetchone()
            generated_otp = uuid.uuid4().hex[0:8]
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
                    "donotUseMiddleWare": True,
                    'status': True,
                    'name': username
                    }, 200
        except Exception as e:
             app.logger.error(str(e))
             return {'msg': 'OTP generated successfully',
                    'status': False}, 500

    def send_otp_email(self, user, new_otp):
        try:
            app.config.update(
            	DEBUG = True,
            	MAIL_SERVER = mailconfig.MAIL_CONFIG['mail_server'],
            	MAIL_PORT = mailconfig.MAIL_CONFIG['mail_port'],
            	MAIL_USE_SSL = True,
            	MAIL_USERNAME = mailconfig.MAIL_CONFIG['mail_username'],
            	MAIL_PASSWORD = mailconfig.MAIL_CONFIG['mail_password']
            )
            msg = Message('RegOpz!',
                            sender =  'donot-reply@regopz.com',
                            recipients = [user['email']])
            msg.body = "Hi {0},\n" + \
                        "Please use the following OTP to reset password:\n " + \
                        "OTP: {1}".format(user['first_name'],new_otp)
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

            otp = json.loads(recovery['details_data'])
            entered_otp = data['otp'].encode('utf-8')
            saved_otp = otp['OTP'].encode('utf-8')
            if hashpw(entered_otp, saved_otp) != saved_otp:
                return {'msg': 'OTP could not be matched ! Please try again.',
                         "donotUseMiddleWare": True,
                         'status': False,
                         'name': data['name']
                         }, 200
            return {'msg': 'OTP validated successfully',
                    "donotUseMiddleWare": True,
                    'status': True,
                    'name': data['name']
                    }, 200
        except Exception as e:
            app.logger.error(str(e))
            return {'msg' : str(e), 'status': False}, 500




    #     if request.endpoint == 'get_users_security_questions_ep':
    #         return self.get_questions_pwd_recovery(id)
    #
    #     if request.endpoint == 'validate_user_ep':
    #         return self.validate_user(id)
    #
    #     if request.endpoint == 'send_otp_ep':
    #         return self.send_otp(id)
    #
    #     # if request.endpoint == 'match_answers_ep':
    #     #     req_data = request.get_json(forec = True)
    #     #     return self.capture_sec_que_ans(req_data)
    #
    # def post(self):
    #     if request.endpoint == 'capture_password_policy_ep':
    #         print('capturing ')
    #         req_data = request.get_json(force = True)
    #         return self.capture_password_policy(req_data)
    #
    #     if request.endpoint == 'check_answers_ep':
    #         req_data = request.get_json(force = True)
    #         return self.check_answers(req_data)
    #
    #     if request.endpoint == 'capture_security_answers_ep':
    #         req_data = request.get_json(force = True)
    #         return self.capture_sec_que_ans(req_data)
    #
    #     if request.endpoint == 'validate_otp_ep':
    #         req_data = request.get_json(force = True)
    #         return self.validate_otp(req_data)
    #
    # def put(self):
    #     if request.endpoint == 'edit_password_policy_ep':
    #         req_data = request.get_json(force = True)
    #         return self.edit_password_policy(req_data)
    #
    #     if requestself.endpoint ==  'change_pwd_ep':
    #         req_data = request.get_json(force = True)
    #         return self.change_pwd(req_data)
    #
    #
    # def validate_otp(self, red_data):
    #     try:
    #         user_name = red_data['username']
    #         otp = req_data['otp']
    #         sql = "select temp_otp from user_pwd_details where user_id = %s"
    #         sent_otp = self.db.query(sql,(user_name)).fetchone()
    #         if otp == sent_otp:
    #             self.db.transact("update user_pwd_details set temp_otp = ' ' where usera_id = %s",(user_name))
    #             queryString = "SELECT r.role, u.* FROM regopzuser u JOIN (roles r) ON (u.role_id = r.id)\
    #                 WHERE name=%s AND status='Active'"
    #             cur = self.db.query(queryString, (user_id, ))
    #             data = cur.fetchone()
    #             self.db.commit()
    #             return Token().create(data)
    #         else:
    #             return {'O.T.P not matched, Please try again'}, 403
    #     except Exception as e:
    #             app.logger.error(e.__str__())
    #             return (e.__str__())
    #
    #
    # def validate_user(self, id):
    #     try:
    #         sql = "select * from regopzuser where name = %s"
    #         data = self.db.query(sql,(id, )).fetchone()
    #         if data:
    #             return {'msg': 'User validated successfully'}, 200
    #         else :
    #             return {'msg': 'User Name not found'}, 403
    #     except Exception as e:
    #         app.logger.error(e.__str__())
    #         return (e.__str__())
    #
    #
    #
    #
    #
    #
    #
    # def send_otp(self, user_name):
    #     try:
    #
    #         sql = "select email from regopzuser where name = %s"
    #         email = self.db.transact(sql,(user_name, )).fetchone()
    #         otp = str((uuid.uuid4()).int)[0:5]
    #
    #         app.config.update(
    #         	DEBUG = True,
    #         	MAIL_SERVER = mailconfig.MAIL_CONFIG['mail_server'],
    #         	MAIL_PORT = mailconfig.MAIL_CONFIG['mail_port'],
    #         	MAIL_USE_SSL = True,
    #         	MAIL_USERNAME = mailconfig.MAIL_CONFIG['mail_username'],
    #         	MAIL_PASSWORD = mailconfig.MAIL_CONFIG['mail_password']
    #         )
    #         msg = Message('Sending mail from my applcation!',
    #         sender =  'xyz@xyz.com',
    #         recipients = [email])
    #         msg.body = "Enter The following OTP to reset your password '{}'".format(otp)
    #         mail = Mail(app)
    #         mail.send(msg)
    #         app.logger.info('OTP Sent')
    #         sql = "update user_pwd_details set temp_otp = %s where user_id = %s"
    #         self.db.transact(sql,(otp, user_name))
    #         self.db.commit()
    #         return {'msg': 'O.T.P sent successfully'}, 200
    #     except Exception as e:
    #         app.logger.error(e.__str__())
    #         return (e.__str__())
    #
    # # def reset_password_otp(self, req_data):
    # #     try:
    # #         print('WIll do it later')
    # #     except Exception as e:
    # #         app.logger.error(e.__str__())
    # #         return (e.__str__())
    #
    #
    # def edit_password_policy(self, req_data):
    #     try:
    #         #self.dbm = DatabaseHelper()
    #         app.logger.info('capturing password policy')
    #         ## What will be the request format for edit_passsword_policy
    #         ## _____________________TODO_______________________________
    #     except Exception as e:
    #         app.logger.error(e.__str__())
    #         return (e.__str__())
    #
    #
    #
    #
    #
    #
