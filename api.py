from Controllers.Info import Info
from Controllers.DocumentController import DocumentController
from Controllers.MaintainBusinessRulesController import MaintainBusinessRulesController
from Controllers.UserController import UserController
from app import *
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
api.add_resource(UserController,
    apiPath + "/users",
    endpoint="users_ep"
)
api.add_resource(UserController,
    apiPath + "/user/<string:userId>",
    endpoint="user_ep"
)
api.add_resource(UserController,
    apiPath + "/user/login",
    endpoint="user_login_ep"
)
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=APIConfig.API['port'])
#hello