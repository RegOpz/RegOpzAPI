from flask import Flask
from flask_restful import Resource, Api
from Helpers.DatabaseHelper import *
from Configs import APIConfig
from Controllers.Info import Info
app = Flask(__name__)
api = Api(app)
apiPath = APIConfig.APIPATH
api.add_resource(Info, apiPath + "/info")
if __name__ == '__main__':
    app.run(debug=True, port=APIConfig.API['port'])
