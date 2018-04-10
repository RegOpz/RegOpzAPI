from app import *
from flask_restful import Resource,abort
from flask import Flask, request, redirect, url_for
from jwt import JWT, jwk_from_pem
from Helpers.DatabaseHelper import DatabaseHelper
import json


class TenantSubscirptionController(Resource):
    def __init__(self):
        self.db=DatabaseHelper()

    def get(self):
        domain_name=request.args.get("domain")
        return self.get_domain_information(domain_name)

    def get_domain_information(self,domain_name):
        try:
            app.logger.info("Getting subscriber domain infromation for {}".format(domain_name))
            subscr_info=self.db.query("select * from tenant_subscription_detail where tenant_id=%s",(domain_name,)).fetchone()

            if not subscr_info:
                return {"msg":"Subscriber information not found!","donotUseMiddleWare": True},403

            user = {
                'domainInfo': json.dumps(subscr_info)
            }
            jwtObject = JWT()
            with open('private_key', 'rb') as fh:
                salt = jwk_from_pem(fh.read())
            jwt = jwtObject.encode(user, salt, 'RS256')
            return jwt

        except Exception as e:
            app.logger.error(str(e))
            return {"msg":str(e)},500
