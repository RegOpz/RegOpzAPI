from flask import Flask
from flask_restful import Resource, Api
from Helpers.DatabaseHelper import *
from Configs import APIConfig
from Controllers.Info import Info
from Controllers.DocumentController import DocumentController
from flask_cors import CORS, cross_origin
app = Flask(__name__)
CORS(app)
api = Api(app)
apiPath = APIConfig.APIPATH
api.add_resource(Info, apiPath + "/info")
api.add_resource(DocumentController,
    apiPath + "/document",
    apiPath + "/document/<string:doc_id>"
)
if __name__ == '__main__':
    app.run(debug=True, port=APIConfig.API['port'])
