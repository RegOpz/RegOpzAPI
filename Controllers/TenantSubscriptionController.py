from app import *
from flask_restful import Resource,abort
from flask import Flask, request, redirect, url_for
from jwt import JWT, jwk_from_pem
from Helpers.DatabaseHelper import DatabaseHelper
import json
from datetime import datetime


class TenantSubscirptionController(Resource):
    def __init__(self):
        self.db=DatabaseHelper()

    def get(self, domain_name=None):
        self.userCheck = request.args.get('userCheck')
        if domain_name:
            return self.get_domain_information(domain_name)
        return self.fetch_subscribers()

    def put(self, id=None):
        self.id = id
        data = request.get_json(force=True)
        return self.update_subscriber(data)

    def post(self):
        data = request.get_json(force=True)
        return self.add_subscriber(data)

    def get_domain_information(self,domain_name):
        try:
            app.logger.info("Getting subscriber domain infromation for {} {}".format(domain_name, self.userCheck))
            subscr_info=self.db.query("select * from tenant_subscription_detail where tenant_id=%s",(domain_name,)).fetchone()

            if self.userCheck is None and (not subscr_info or not subscr_info['tenant_conn_details']):
                return {"msg":"Subscriber information not found!","donotUseMiddleWare": True},403
            if self.userCheck is None and (not subscr_info['subscription_start_date'] or not subscr_info['subscription_end_date']):
                return {"msg":"Subscription period not found! Please contact Admin/RegOpz Support","donotUseMiddleWare": True},403
            if self.userCheck is None and (subscr_info['subscription_end_date'] < datetime.now()):
                return {"msg":("Subscription expired on {}! Please contact " + \
                            "Admin/RegOpz Support for assistance.").format(subscr_info['subscription_end_date'].strftime("%d-%b-%Y")),\
                            "donotUseMiddleWare": True},403
            if self.userCheck is None and (subscr_info['subscription_start_date'] > datetime.now()):
                return {"msg":("Subscription will start on {}! Please try to login after subscription start date," + \
                            " alternately contact Admin/RegOpz Support for assistance.").format(subscr_info['subscription_start_date'].strftime("%d-%b-%Y")),\
                            "donotUseMiddleWare": True},403
            if self.userCheck and not subscr_info:
                return {}, 200

            for k,v in subscr_info.items():
                # app.logger.info("k,v of subscr_info items {}".format(k,v))
                if isinstance(subscr_info[k],datetime):
                    subscr_info[k] = subscr_info[k].isoformat()
                    # app.logger.info("{0} {1}".format(d[k], type(d[k])))

            user = {
                'domainInfo': json.dumps(subscr_info)
            }
            jwtObject = JWT()
            with open('private_key', 'rb') as fh:
                salt = jwk_from_pem(fh.read())
            jwt = jwtObject.encode(user, salt, 'RS256')
            subscriptionInfo = {
                                'tenant_id':subscr_info['tenant_id'],
                                'country':subscr_info['country'],
                                'tenant_description':subscr_info['tenant_description'],
                                'subscription_details':subscr_info['subscription_details']
                                }
            # Lets keep token and login_details store data separate. Also ensure that
            # tenant and master connection details are always encrypted and not available in store.
            return {'token':jwt,'subscriptionInfo':subscriptionInfo}

        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500

    def fetch_subscribers(self):
        try:
            app.logger.info("Getting all subscribers infromation.")
            subscr_info=self.db.query("select * from tenant_subscription_detail").fetchall()

            for i,d in enumerate(subscr_info):
                # app.logger.info('Processing index {}'.format(i))
                for k,v in d.items():
                    if isinstance(v,datetime):
                        d[k] = d[k].isoformat()
                        # app.logger.info("{0} {1}".format(d[k], type(d[k])))

            return subscr_info

        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500

    def update_subscriber(self, data = None):
        if data :
            app.logger.info("data for update subscriber : {}".format(data,))
            queryString = "UPDATE tenant_subscription_detail SET " + \
                "master_conn_details=%s,subscription_details=%s," + \
                "subscription_end_date=null,subscription_start_date=null," + \
                "tenant_address=%s,tenant_conn_details=%s,tenant_description=%s," + \
                "tenant_email=%s,tenant_file_system=%s," + \
                "tenant_phone=%s, country=%s," + \
                "subscription_start_date=%s, subscription_end_date=%s " + \
                "WHERE id=%s"
            queryParams = (data['master_conn_details'], data['subscription_details'], \
                data['tenant_address'], data['tenant_conn_details'], data['tenant_description'], \
                data['tenant_email'], data['tenant_file_system'], \
                data['tenant_phone'],data['country'], \
                # Replacing the timezone offset Z as we are assuming no time offset and always show the value as stored.
                data['subscription_start_date'].replace('Z',''),data['subscription_end_date'].replace('Z',''), \
                data['id'])
            try:
                rowId = self.db.transact(queryString, queryParams)
                self.db.commit()
                return { "msg": "Successfully updated subscriber details for {}.".format(data['tenant_id']) },200
            except Exception as e:
                # print(e)
                return { "msg": "Cannot update this subscriber {}, please review the details".format(data['tenant_id']) },400
        return NO_USER_FOUND

    def add_subscriber(self, data = None):
        if data :
            queryString = "insert into  tenant_subscription_detail " + \
                "(tenant_id,tenant_address,tenant_description,tenant_email,tenant_phone)" + \
                "VALUES(%s,%s,%s,%s,%s)"
            queryParams = (data['tenant_id'], data['tenant_address'], \
                data['tenant_description'], data['tenant_email'], \
                data['tenant_phone'])
            try:
                rowId = self.db.transact(queryString, queryParams)
                self.db.commit()
                return { "msg": "Added subscriber {} successfully, please contact Admin to activate".format(data['tenant_id']),
                        "donotUseMiddleWare": True },200
            except Exception as e:
                print(e)
                return { "msg": "Uable to add subscriptionn request, please review the details" },400
        return NO_USER_FOUND
