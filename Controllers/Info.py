from flask_restful import Resource
from Configs import APIConfig
import platform
class Info(Resource):
    def get(self):
        return {
            'context':"RegOpz REST API",
            'author': "RegOpz Team",
            'organization': "RegOpz Pty Ltd",
            "os": platform.linux_distribution(),
            'kernel': platform.system() + " " + platform.release(),
            'version': APIConfig.API['version']
        }
