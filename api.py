from Controllers.Info import Info
from Controllers.DocumentController import DocumentController
from Controllers.ReportTemplateController import ReportTemplateController
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
from Controllers.LoadDataController import LoadDataController
from Controllers.ViewReportController import ViewReportController
from Controllers.LoadDataFileController import LoadDataFileController
from Controllers.DynamicReportController import DynamicReportController
from Controllers.TransactionalReportController import TransactionalReportController
from Controllers.TenantSubscriptionController import TenantSubscirptionController
from Controllers.SharedDataController import SharedDataController
from Controllers.ManageMasterBusinessRulesController import ManageMasterBusinessRulesController
from Controllers.ManageMasterReportController import ManageMasterReportController
from Controllers.OperationalLogController import OperationalLogController
from Controllers.PasswordRecoveryController import PasswordRecoveryController
from flask_cors import CORS, cross_origin

api.add_resource(Info, apiPath + "/info")

api.add_resource(ReportTemplateController,
    apiPath + "/document",
    apiPath + "/document/<string:doc_id>"
)

api.add_resource(ViewReportController,
    apiPath + "/view-report/report",
    apiPath + "/view-report/report/<string:report_id>",
    endpoint = "view_report_ep"
)
api.add_resource(ViewReportController,
    apiPath + "/view-report/get-report-list",
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
api.add_resource(ReportTemplateController,
    apiPath + "/document/get-report-template-suggestion-list",
    endpoint = "get_report_template_suggestion_list_ep"
)
api.add_resource(ViewReportController,
    apiPath + "/view-report/get-report-export-to-excel",
    endpoint = "get_report_export_to_excel_ep"
)
api.add_resource(MaintainReportRulesController,
    apiPath + "/report-rule/get-report-rule-export-to-excel",
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
    apiPath + "/business-rules/<string:page>/<string:source_id>",
    endpoint="business_rules_sourceid_ep"
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
    apiPath + "/users/<string:userCheck>/<string:userId>",
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
    apiPath+"/create-report/generate-report"
)

api.add_resource(GenerateReportController,
    apiPath+"/create-report/get-report-list",
    endpoint="get_report_list_ep"
)

api.add_resource(GenerateReportController,
    apiPath+"/create-report/get-country-list",
    endpoint="get_country_list_ep"
)


api.add_resource(MaintainReportRulesController,
    apiPath+"/report-rule",
    apiPath+"/report-rule/<int:id>",
    endpoint="report_rule_ep"
)

api.add_resource(MaintainReportRulesController,
    apiPath+"/report-rule/<string:report>",
    endpoint="report_parameter_ep"
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
api.add_resource(MaintainReportRulesController,
    apiPath+"/report-rule/audit-list",
    endpoint="report_rule_audit_ep"
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

api.add_resource(DataChangeController,
    apiPath+"/workflow/data-change/audit-decision",
    endpoint="data_audit_decision"
)

api.add_resource(LoadDataController,
    apiPath+"/data-feed-management/load-data",
    endpoint="load_data_ep"
)

api.add_resource(LoadDataFileController,
    apiPath + '/view-data-management/load-data',
    endpoint='load_data'
)

api.add_resource(DynamicReportController,
    apiPath+"/create-dynamic-report",
    endpoint="create_dynamic_report_ep"
)

api.add_resource(TransactionalReportController,
    apiPath+"/transactionalReport/<string:report_id>",
    apiPath+"/transactionalReport/<string:report_id>/<string:reporting_date>",
    apiPath+"/transactionalReport/getSection/<string:cell_id>",
    apiPath+"/transactionalReport/getRules/<string:rule_cell_id>",
    apiPath+"/transactionalReport/createTransReport/<string:report_id>",
)
api.add_resource(TransactionalReportController,
    apiPath + "/transactionalReport/trans-report-rule",
    apiPath + "/transactionalReport/trans-report-rule/<int:id>",
    endpoint="trans_report_rule"
)
api.add_resource(TransactionalReportController,
    apiPath + "/transactionalReport/trans-report-rule/bulk-process",
    endpoint = "trans_bulk_process"
)
api.add_resource(TransactionalReportController,
    apiPath + "/transactionalReport/audit-list",
    endpoint = "trans_report_rule_audit_ep"
)
api.add_resource(TransactionalReportController,
    apiPath+"/transactionalReport/captureTemplate",
    endpoint="load_trans_report_template_ep"
)

api.add_resource(TransactionalReportController,
    apiPath + "/transactionalReport/get-transreport-export-to-excel",
    endpoint = "get_transreport_export_to_excel_ep"
)

api.add_resource(TransactionalReportController,
    apiPath+"/transactionalReport/defineSection",
    endpoint="update_trans_section_ep"
)

api.add_resource(TenantSubscirptionController,
    apiPath+"/subscription",
    apiPath+"/subscription/<string:domain_name>",
    apiPath+"/subscription/<int:id>"
)

api.add_resource(SharedDataController,
    apiPath + "/shared-data/countries",
    endpoint = "get_countries_ep"
)

api.add_resource(SharedDataController,
    apiPath + "/shared-data/components",
    endpoint = "get_components_ep"
)

api.add_resource(SharedDataController,
    apiPath + "/shared-data/testconnection",
    endpoint = "test_connection_ep"
)

api.add_resource(ManageMasterBusinessRulesController,
    apiPath + "/business-rules-repo",
    apiPath + "/business-rules-repo/copy-to-tenant/<int:source_id>",
)

api.add_resource(ManageMasterReportController,
    apiPath + "/report-rules-repo",
    apiPath + "/report-rules-repo/<string:country>",
    apiPath + "/report-rules-repo/report/<string:report_id>",
    apiPath + "/report-rules-repo/copy-report-template"
)

api.add_resource(ManageMasterReportController,
    apiPath + "/report-rules-repo/drilldown",
    endpoint = "repository_drill_down_rule_ep"
)
api.add_resource(ManageMasterReportController,
    apiPath + "/report-rules-repo/fetch-report-id",
    endpoint = "fetch-report-id"
)

api.add_resource(ManageMasterReportController,
    apiPath + "/report-rules-repo/audit-list",
    endpoint = "repository_report_rule_audit_ep"
)

api.add_resource(ManageMasterReportController,
    apiPath + "/report-rules-repo/report/report-rule",
    endpoint = "repository_report_rule_ep"
)

api.add_resource(OperationalLogController,
    apiPath + "/fetch-operation-log",
)

api.add_resource(PasswordRecoveryController,
    apiPath + "/pwd-recovery/get-all-security-questions",
    endpoint = "get_all_security_questions_ep"
)

api.add_resource(PasswordRecoveryController,
    apiPath + "/pwd-recovery/get-user-security-questions/<string:username>",
    endpoint = "get_user_security_questions_ep"
)

api.add_resource(PasswordRecoveryController,
    apiPath + "/pwd-recovery/validate-pwd-recovery-answers",
    endpoint = "validate_pwd_recovery_answers"
)
#
# api.add_resource(PasswordRecoveryController,
#     apiPath + "/validate_user/<string:id>",
#     endpoint = "validate_user_ep"
# )
#
# api.add_resource(PasswordRecoveryController,
#     apiPath + "/change_pwd",
#     endpoint = "change_pwd_ep"
# )
#
# api.add_resource(PasswordRecoveryController,
#     apiPath + "/edit_password_policy",
#     endpoint = "edit_password_policy_ep"
# )
#
# api.add_resource(PasswordRecoveryController,
#     apiPath + "/validate_otp",
#     endpoint = "validate_otp_ep"
# )
#
# api.add_resource(PasswordRecoveryController,
#     apiPath + "/capture_security_answers",
#     endpoint = "capture_security_answers_ep"
# )
#
# api.add_resource(PasswordRecoveryController,
#     apiPath + "/check_answers",
#     endpoint = "check_answers_ep"
# )
#
# api.add_resource(PasswordRecoveryController,
#     apiPath + "/capture_password_policy",
#     endpoint = "capture_password_policy_ep"
# )
#
# api.add_resource(PasswordRecoveryController,
#     apiPath + "/send_otp_pwd_recovery/<string:id>",
#     endpoint = "send_otp_ep"
# )

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=APIConfig.API['port'], threaded=True)
#hello
