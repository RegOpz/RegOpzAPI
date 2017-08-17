from Controllers.Info import Info
from Controllers.DocumentController import DocumentController
from Controllers.MaintainBusinessRulesController import MaintainBusinessRulesController
from Controllers.MaintainReportRulesController import MaintainReportRulesController
from Controllers.UserController import UserController
from Controllers.ViewDataController import ViewDataController
from Controllers.GenerateReportController import GenerateReportController
from Controllers.VarianceAnalysisController import VarianceAnalysisController
from app import *
from Controllers.ResourceController import ResourceController
from Controllers.RoleController import RoleController
from Controllers.PermissionController import PermissionController
from Controllers.MaintainSourcesController import MaintainSourcesController
from Controllers.DefChangeController import DefChangeController
from Controllers.DataChangeController import DataChangeController
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
api.add_resource(DocumentController,
    apiPath + "/document/get-report-export-to-excel",
    endpoint = "get_report_export_to_excel_ep"
)
api.add_resource(DocumentController,
    apiPath + "/document/get-report-rule-export-to-excel",
    endpoint = "get_report_rule_export_to_excel_ep"
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
api.add_resource(MaintainBusinessRulesController,
    apiPath + "/business-rule/get-br-source-suggestion-list",
    endpoint="get_br_source_suggestion_list_ep"
)
api.add_resource(MaintainBusinessRulesController,
    apiPath + "/business-rule/get-br-source-column-suggestion-list",
    endpoint="get_br_source_column_suggestion_list_ep"
)

api.add_resource(MaintainBusinessRulesController,
    apiPath+"/business-rule/validate-python-expr",
    endpoint="validate_python_expr_ep"
)

api.add_resource(UserController,
    apiPath + "/users",
    apiPath + "/users/<string:userId>",
    endpoint="users_ep"
)
api.add_resource(RoleController,
    apiPath+"/roles",
    apiPath+"/roles/<string:role>",
    endpoint="role_ep"
)
api.add_resource(PermissionController,
    apiPath+"/permissions",
    endpoint="permission_ep"
)
api.add_resource(ResourceController,
    apiPath+"/resource",
    apiPath + "/resource/<int:id>",
    endpoint="resource_ep"
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

api.add_resource(GenerateReportController,
    apiPath+"/create-report/get-report-list",
    endpoint="get_report_list_ep"
)

api.add_resource(GenerateReportController,
    apiPath+"/create-report/get-country-list",
    endpoint="get_country_list_ep"
)

api.add_resource(GenerateReportController,
    apiPath+"/create-report/generate-report",
    endpoint="create_report_ep"
)

api.add_resource(MaintainReportRulesController,
    apiPath+"/report-rule",
    apiPath+"/report-rule/<int:id>",
    endpoint="report_rule_ep"
)
api.add_resource(MaintainReportRulesController,
    apiPath+"/report-rule/get-business-rules-suggestion-list",
    endpoint="get_business_rules_suggestion_list_ep"
)
api.add_resource(MaintainReportRulesController,
    apiPath+"/report-rule/get-source-suggestion-list",
    endpoint="get_source_suggestion_list_ep"
)
api.add_resource(MaintainReportRulesController,
    apiPath+"/report-rule/get-agg-function-column-suggestion-list",
    endpoint="get_agg_function_column_suggestion_list_ep"
)
api.add_resource(MaintainReportRulesController,
    apiPath+"/report-rule/get-cell-calc-ref-suggestion-list",
    endpoint="get_cell_calc_ref_suggestion_list_ep"
)
api.add_resource(MaintainSourcesController,
    apiPath+"/maintain-sources",
    apiPath+"/maintain-sources/<int:id>",
    endpoint="maintain_sources_ep"
)
api.add_resource(MaintainSourcesController,
    apiPath+"/maintain-sources/get-source-feed-suggestion-list",
    endpoint="get_source_feed_suggestion_list_ep"
)
api.add_resource(MaintainSourcesController,
    apiPath+"/maintain-sources/get-sourcetable-column-suggestion-list",
    endpoint="get_sourcetable_column_suggestion_list_ep"
)

api.add_resource(VarianceAnalysisController,
    apiPath+"/analytics/variance-analysis/get-country-suggestion-list",
    endpoint="get_variance_country_suggestion_list"
)

api.add_resource(VarianceAnalysisController,
    apiPath+"/analytics/variance-analysis/get-report-suggestion-list",
    endpoint="get_variance_report_suggestion_list"
)

api.add_resource(VarianceAnalysisController,
    apiPath+"/analytics/variance-analysis/get-date-suggestion-list",
    endpoint="get_variance_date_suggestion_list"
)

api.add_resource(VarianceAnalysisController,
    apiPath+"/analytics/variance-analysis/get-variance-report",
    endpoint="get_variance_report"
)

api.add_resource(DefChangeController,
    apiPath+"/workflow/def-change/get-audit-list",
    endpoint="get_audit_list"
)

api.add_resource(DefChangeController,
    apiPath+"/workflow/def-change/get-record-detail",
    endpoint="get_record_detail"
)

api.add_resource(DefChangeController,
    apiPath+"/workflow/def-change/audit-decision",
    endpoint="audit_decision"
)

api.add_resource(DataChangeController,
    apiPath+"/workflow/data-change/get-audit-list",
    endpoint="get_data_audit_list"
)

api.add_resource(DataChangeController,
    apiPath+"/workflow/data-change/get-record-detail",
    endpoint="get_data_record_detail"
)

api.add_resource(DefChangeController,
    apiPath+"/workflow/data-change/audit-decision",
    endpoint="data_audit_decision"
)





if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=APIConfig.API['port'], threaded=True)
#hello
