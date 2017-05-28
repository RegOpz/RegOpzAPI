from Controllers.Info import Info
from Controllers.DocumentController import DocumentController
from Controllers.MaintainBusinessRulesController import MaintainBusinessRulesController
from Controllers.UserController import UserController
from Controllers.ViewDataController import ViewDataController
from Controllers.GenerateReportController import GenerateReportController
from app import *
from Controllers.ResourceController import ResourceController
from Controllers.RoleController import RoleController
from Controllers.RoleController import RoleController
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
api.add_resource(DocumentController,
    apiPath + "/document/get-report-list",
    endpoint = "report_list_ep"
)
api.add_resource(DocumentController,
    apiPath + "/document/drill-down",
    endpoint = "drill_down_ep"
)
api.add_resource(DocumentController,
    apiPath + "/document/drill-down-data",
    endpoint = "drill_down_data_ep"
)
api.add_resource(DocumentController,
    apiPath + "/document/get-date-heads-for-report",
    endpoint = "get_date_heads_for_report_ep"
)
api.add_resource(DocumentController,
    apiPath + "/document/get-report-template-suggestion-list",
    endpoint = "get_report_template_suggestion_list_ep"
)
api.add_resource(MaintainBusinessRulesController,
    apiPath + "/business-rules/<string:page>",
    endpoint="business_rules_ep"
)
api.add_resource(MaintainBusinessRulesController,
    apiPath + "/business-rules/<string:page>/orderby/<string:col_name>",
    endpoint="business_rules_ep_ordered"
)
api.add_resource(MaintainBusinessRulesController,
    apiPath + "/business-rules/filtered",
    endpoint="business_rules_ep_filtered"
)
api.add_resource(MaintainBusinessRulesController,
    apiPath + "/business-rules/drill-down-rules",
    endpoint="business_rule_drill_down_rules_ep"
)
api.add_resource(MaintainBusinessRulesController,
    apiPath + "/business-rule/<string:id>",
    endpoint="business_rule_ep"
)
api.add_resource(MaintainBusinessRulesController,
    apiPath + "/business-rule/linkage/<string:business_rule>",
    endpoint="business_rule_linkage_ep"
)
api.add_resource(MaintainBusinessRulesController,
    apiPath + "/business-rule/linkage-multiple",
    endpoint="business_rule_linkage_multiple_ep"
)
api.add_resource(MaintainBusinessRulesController,
    apiPath + "/business-rule/export_to_csv",
    endpoint="business_rule_export_to_csv_ep"
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
api.add_resource(ResourceController,
    apiPath+"/resource",
    apiPath + "/resource/<int:id>",
    endpoint="resource_ep"
)
api.add_resource(RoleController,
    apiPath+"/role",
    apiPath + "/role/<int:id>",
    endpoint="role_ep"
)
api.add_resource(ViewDataController,
    apiPath+"/view-data/get-date-heads",
    endpoint="get_date_heads_ep"
)
api.add_resource(ViewDataController,
    apiPath+"/view-data/report",
    endpoint="report_ep"
)
api.add_resource(ViewDataController,
    apiPath+"/view-data/table-data",
    endpoint="table_data_ep"
)
api.add_resource(ViewDataController,
    apiPath+"/view-data/report/<string:id>",
    endpoint="report_update_ep"
)
api.add_resource(ViewDataController,
    apiPath+"/view-data/report/export-csv",
    endpoint="report_export_csv_ep"
)
api.add_resource(ViewDataController,
    apiPath+"/view-data/get-sources",
    endpoint="get_source_ep"
)
api.add_resource(ViewDataController,
    apiPath+"/view-data/get-report-linkage",
    endpoint="report_linkage_ep"
)
api.add_resource(ViewDataController,
    apiPath+"/view-data/apply-rules",
    endpoint="apply_rules_ep"
)
api.add_resource(GenerateReportController,
    apiPath+"/view-data/generate-report",
    endpoint="generate_report_ep"
)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=APIConfig.API['port'], threaded=True)
#hello
