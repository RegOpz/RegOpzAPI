from flask import Flask
from flask_restful import Resource, Api
from Helpers.DatabaseHelper import *
from Configs import APIConfig
from Controllers.Info import Info
from Controllers.DocumentController import DocumentController
from Controllers.MaintainBusinessRulesController import MaintainBusinessRulesController
from Controllers.ResourceController import ResourceController
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
api.add_resource(MaintainBusinessRulesController,
    apiPath + "/business-rules",
    endpoint="business_rules_ep"
)
api.add_resource(MaintainBusinessRulesController,
    apiPath + "/business-rules/<string:business_rule>",
    endpoint="business_rule_ep"
)

api.add_resource(ResourceController,
    apiPath+"/resource",
    apiPath + "/resource/<int:id>",
    endpoint="resource_ep"
)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=APIConfig.API['port'])
