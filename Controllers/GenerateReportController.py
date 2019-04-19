from flask_restful import Resource
import time
from multiprocessing import Pool,cpu_count
from functools import partial
import re
from datetime import datetime
from app import *
import Helpers.utils as util
from collections import defaultdict
import pandas as pd
from Helpers.DatabaseHelper import DatabaseHelper
from numpy import where
from Helpers.Tree import tree
import json
from Helpers.utils import autheticateTenant
from Helpers.authenticate import *
import math
import Parser.FormulaTranslator as fm_trns
import Parser.FormulaTranslator2 as fm_trns2
from Parser.PandasLib import *
from Parser.ExcelLib import *
from Controllers.OperationalLogController import OperationalLogController

class GenerateReportController(Resource):
    def __init__(self):
        self.domain_info = autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.db=DatabaseHelper(self.tenant_info)
            self.user_id=Token().authenticate()

        self.opsLog = OperationalLogController()
        self.er = {}
        self.log_master_id = None

    @authenticate
    def get(self):
        if(request.endpoint=='get_report_list_ep'):
            country=request.args.get('country') if request.args.get('country') != None else 'ALL'
            return self.get_report_list(country)
        if (request.endpoint == 'get_country_list_ep'):
            return self.get_country_list()

    def post(self):
        report_parameters=request.get_json(force=True)
        return self.create_report(report_parameters)



    def  create_report(self, report_parameters):
        try:
            app.logger.info("Creating report")
            report_id = report_parameters["report_id"]
            reporting_date = report_parameters["reporting_date"]
            self.reporting_currency = report_parameters["reporting_currency"]
            report_version_no=self.create_report_catalog(report_parameters)

            self.update_report_catalog(status='RUNNING'
                                        , report_parameters=report_parameters
                                        , version=report_version_no)
            report_snapshot=self.create_report_detail(report_version_no,**report_parameters)
            self.update_report_catalog(status='SNAPSHOTCREATED'
                                        , report_parameters=report_parameters
                                        , report_snapshot=report_snapshot
                                        , version=report_version_no)
            #print("create_report_summary_by_source")
            self.create_report_summary_by_source(report_version_no,report_snapshot,**report_parameters)
            # print("create_report_summary_final")
            self.create_report_summary_final(report_version_no,report_snapshot,**report_parameters)
            # self.db.commit()
            self.update_report_catalog(status='SUCCESS'
                                        , report_parameters=report_parameters
                                        , report_snapshot=report_snapshot
                                        , version=report_version_no)
            if self.log_master_id:
                self.opsLog.write_log_detail(master_id=self.log_master_id
                    , operation_sub_type='End of Create Report'
                    , operation_status='Complete'
                    , operation_narration="Report generated SUCCESSFULLY for [{0}] Reporting date [{1}].".format(str(report_id),str(reporting_date))
                    )
                self.opsLog.update_master_status(id=self.log_master_id,operation_status="SUCCESS")

            return {"msg": "Report generated SUCCESSFULLY for [{0}] Reporting date [{1}].".format(str(report_id),str(reporting_date))}, 200
        except Exception as e:
            app.logger.error(str(e))
            if self.log_master_id:
                self.opsLog.write_log_detail(master_id=self.log_master_id
                    , operation_sub_type='Error occured while creating report'
                    , operation_status='Failed'
                    , operation_narration="Report not generated  for [{0}] Reporting date [{1}].".format(str(report_id),str(reporting_date))
                    )
                self.opsLog.update_master_status(id=self.log_master_id,operation_status="ERROR")
            return {'msg': str(e)}, 400
            # raise e


    def get_report_list(self,country='ALL'):
        try:
            report_list=self.db.query("select distinct report_id from report_def_catalog where country='"+country+"'").fetchall()
            return report_list
        except Exception as e:
            app.logger.error(str(e))
            return {'msg': str(e)}, 500

    def get_country_list(self):
        try:
            country_list=self.db.query("select distinct country from report_def_catalog").fetchall()
            return country_list
        except Exception as e:
            app.logger.error(str(e))
            return {'msg': str(e)}, 500

    def create_report_catalog(self,report_parameters):
        try:
            report_id = report_parameters["report_id"]
            reporting_date = report_parameters["reporting_date"]
            report_parameters_str=json.dumps(report_parameters)
            report_create_status = 'CREATE'
            report_create_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            as_of_reporting_date = report_parameters["as_of_reporting_date"]

            report_version=self.db.query("select max(version) version from report_catalog where report_id=%s and reporting_date=%s",
                                         (report_id,reporting_date)).fetchone()
            report_version_no=1 if not report_version['version'] else  report_version['version']+1

            sql="insert into report_catalog(report_id,reporting_date,report_create_date," + \
                "report_parameters,report_create_status,as_of_reporting_date,version,report_created_by)" + \
                " values(%s,%s,%s,%s,%s,%s,%s,%s)"
            catalog_id = self.db.transact(sql,(report_id,reporting_date,report_create_date,report_parameters_str,report_create_status,
            as_of_reporting_date,report_version_no,self.user_id))
            self.log_master_id = self.opsLog.write_log_master(operation_type='Create Report'
                , operation_status = 'RUNNING'
                , operation_narration = 'Create report {0} for {1} as on {2}'.format(report_id,reporting_date,as_of_reporting_date)
                , entity_type = 'Report'
                , entity_name = report_id
                , entity_table_name = 'report_catalog'
                , entity_id = catalog_id
                )
            self.db.commit()
            if self.log_master_id:
                self.opsLog.write_log_detail(master_id=self.log_master_id
                    , operation_sub_type='Create catalog entry'
                    , operation_status='Complete'
                    , operation_narration='Report creation started with the parameters : {0}'.format(report_parameters_str,))
            return report_version_no
        except Exception as e:
            app.logger.error(str(e))
            if self.log_master_id:
                self.opsLog.write_log_detail(master_id=self.log_master_id
                    , operation_sub_type='Error occured while creating catalog'
                    , operation_status='Failed'
                    , operation_narration='Report creation Failed with error : {0}'.format(str(e),))
            raise(e)

    def update_report_catalog(self,status,report_parameters,report_snapshot=None,version=0):
        try:
            report_id = report_parameters["report_id"]
            reporting_date = report_parameters["reporting_date"]

            update_clause = "report_create_status='{0}'".format(status,)
            update_clause += ", report_create_date='{0}'".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
            if report_parameters != None:
                report_parameters_str=json.dumps(report_parameters)
                update_clause += ", report_parameters='{0}'".format(report_parameters_str,)
            if report_snapshot !=None:
                update_clause +=", report_snapshot='{0}'".format(report_snapshot)
            sql = "update report_catalog set {0} where report_id='{1}' and reporting_date='{2}' and version={3}".format(update_clause,report_id,reporting_date,version)
            self.db.transact(sql)
            self.db.commit()
            if self.log_master_id:
                self.opsLog.write_log_detail(master_id=self.log_master_id
                    , operation_sub_type='Updated report catalog'
                    , operation_status='Complete'
                    , operation_narration='Report catalog updated with : {0}'.format(update_clause,))
        except Exception as e:
            app.logger.error(str(e))
            if self.log_master_id:
                self.opsLog.write_log_detail(master_id=self.log_master_id
                    , operation_sub_type='Error occured while updating report catalog'
                    , operation_status='Failed'
                    , operation_narration='Report creation Failed with error : {0}'.format(str(e),))
            raise(e)


    def map_data_to_cells(self,list_business_rules,qualified_data):

        try:
            result_set=[]
            for qd in qualified_data:
                trd_rules_list=qd["business_rules"].split(',')
                for rl in list_business_rules:
                    br_rules_list=rl["cell_business_rules"].split(',')
                    if set(br_rules_list).issubset(set(trd_rules_list)):
                       result_set.append((rl["report_id"], rl["sheet_id"],rl["cell_id"],rl["cell_calc_ref"],
                       qd["source_id"],qd["qualifying_key"],qd["reporting_date"]))

            return result_set
        except Exception as e:
            app.logger.error(str(e))
            raise(e)



    def create_report_detail(self,report_version_no,**kwargs):
        try:
            app.logger.info("Creating report details")
            parameter_list=['report_id','reporting_currency','business_date_from','business_date_to','ref_date_rate','rate_type']
            if set(parameter_list).issubset(set(kwargs.keys())):
                report_id=kwargs["report_id"]
                reporting_date=kwargs["business_date_from"]+kwargs["business_date_to"]
                reporting_currency=kwargs["reporting_currency"]
                business_date_from=kwargs["business_date_from"]
                business_date_to=kwargs["business_date_to"]
                ref_date_rate=kwargs["ref_date_rate"]
                rate_type=kwargs["rate_type"]
            else:
                app.logger.info("Please supply parameters: "+str(parameter_list))

            all_sources=self.db.query('select distinct source_id \
                        from report_calc_def where report_id=%s and in_use=\'Y\' and source_id !=0',(report_id,)).fetchall()

            cardf = pd.DataFrame(self.db.query("select id,report_id,sheet_id,cell_id,comp_agg_ref,comp_agg_rule,reporting_scale,rounding_option\
                          from report_comp_agg_def WHERE report_id=%s AND in_use='Y'",(report_id,)).fetchall())
            car_version=self.db.query("select report_id,version,id_list from report_comp_agg_def_vers where report_id=%s\
                       and version=(select max(version) from report_comp_agg_def_vers where report_id=%s)",(report_id,report_id)).fetchone()
            if len(cardf.index) == 0:
                 car_id_list = []
            else:
                cardf['id']=cardf['id'].astype(dtype='int64',errors='ignore')
                car_id_list=list(map(int,cardf['id'].tolist()))
            car_id_list.sort()
            car_id_list_str=",".join(map(str,car_id_list))

            if not car_version:
                car_version_no = 1
            else:
                old_id_list = list(map(int, car_version['id_list'].split(',')))
                car_version_no = car_version['version'] + 1 if set(car_id_list) != set(old_id_list) else car_version['version']

            if not car_version or car_version_no != car_version['version']:
                self.db.transact("insert into report_comp_agg_def_vers(report_id,version,id_list) values(%s,%s,%s)",
                    (report_id, car_version_no, car_id_list_str))
                self.db.commit()

            startsource = time.time()
            comp_agg_rule_version=car_version_no
            report_rule_version={}
            qualified_data_version=defaultdict(dict)

            for source in all_sources:
                if self.log_master_id:
                    self.opsLog.write_log_detail(master_id=self.log_master_id
                        , operation_sub_type='Processing source {}'.format(source['source_id'])
                        , operation_status='Started'
                        , operation_narration="Begining of identifying qualified data for source {}".format(source['source_id'],)
                        )

                resultdf=pd.DataFrame(columns=['report_id','sheet_id' ,'cell_id' ,'cell_calc_ref','source_id','qualifying_key','reporting_date'])
                source_id=source['source_id']

                #Create versioning for REPORT_CALC_DEF
                all_business_rules=self.db.query("select id,report_id,sheet_id,cell_id,cell_calc_ref,cell_business_rules \
                        from report_calc_def where report_id=%s and source_id=%s and in_use='Y'",(report_id,source_id,)).fetchall()

                all_business_rules_df=pd.DataFrame(all_business_rules)
                rr_version=self.db.query("select version,id_list from report_calc_def_vers where source_id=%s and report_id=%s and version=(select\
                            max(version) version from report_calc_def_vers where source_id=%s and report_id=%s)",(source_id,report_id,source_id,report_id)).fetchone()
                all_business_rules_df['id'] = all_business_rules_df['id'].astype(dtype='int64', errors='ignore')
                #print(all_business_rules.dtypes)
                rr_id_list=list(map(int,all_business_rules_df['id'].tolist()))
                rr_id_list.sort()
                rr_id_list_str=",".join(map(str,rr_id_list))

                if not rr_version:
                    rr_version_no=1
                else:
                    old_id_list = list(map(int, rr_version['id_list'].split(',')))
                    rr_version_no = rr_version['version'] + 1 if set(rr_id_list) != set(old_id_list) else rr_version['version']

                if not rr_version or rr_version_no != rr_version['version']:
                    self.db.transact("insert into report_calc_def_vers(report_id,source_id,version,id_list) values(%s,%s,%s,%s)",(report_id,source_id,rr_version_no,rr_id_list_str))
                    self.db.commit()
                report_rule_version[str(source_id)]=rr_version_no


                start = time.time()
                #report_parameter={'_TODAY':'20160930','_YESDAY':'20160929'}
                report_parameter={}
                for k,v in kwargs.items():
                    if k.startswith('_'):
                        report_parameter[k]=v

                app.logger.info('Time taken for converting to dictionary all_business_rules ' + str((time.time() - start) * 1000))

                dbqd=DatabaseHelper(self.tenant_info)
                qdvers=dbqd.query("select a.business_date,a.source_id,a.version,a.id_list,a.br_version from qualified_data_vers a,\
                      (select business_date,source_id,max(version) version from qualified_data_vers where business_date between %s and %s and source_id=%s\
                       group by business_date,source_id) b where a.business_date=b.business_date and a.source_id=b.source_id and a.version=b.version",
                      (business_date_from,business_date_to,source_id)).fetchall()

                id_list_df=pd.DataFrame(columns=['report_id','sheet_id' ,'cell_id' ,'cell_calc_ref','source_id','reporting_date','id_list_no'])
                dbqd.transact("create temporary table tmp_qd_id_list(idlist bigint)")
                startcur = time.time()
                for vers in qdvers:
                    start = time.time()
                    qd_id_list = [(id,) for id in vers['id_list'].split(',')]
                    dbqd.transact("truncate table tmp_qd_id_list")
                    dbqd.transactmany("insert into tmp_qd_id_list(idlist) values (%s)",qd_id_list)
                    app.logger.info('Time taken for populating tmp_qd_id_list ' + str((time.time() - start) * 1000))
                    curdata =dbqd.query("select q.source_id,q.qualifying_key,q.business_rules,q.buy_currency,\
                                q.sell_currency,q.mtm_currency,\
                                %s as reporting_currency,q.business_date,%s as reporting_date,%s as business_date_to, \
                                %s as ref_date_rate from qualified_data q, tmp_qd_id_list qv \
                                where q.source_id=%s and q.business_date=%s \
                                and qv.idlist=q.id" ,
                                (reporting_currency,reporting_date,business_date_to,ref_date_rate,vers['source_id'],
                                 vers['business_date']))
                    while True:
                        start=time.time()
                        all_qualified_trade=curdata.fetchmany(50000)
                        if not all_qualified_trade:
                            break
                        app.logger.info('Time taken for fetching next 50000 qualfied trades '+str((time.time() - start)*1000))
                        if self.log_master_id:
                            self.opsLog.write_log_detail(master_id=self.log_master_id
                                , operation_sub_type='Processing next set of data'
                                , operation_status='Processing'
                                , operation_narration="Processing another {0} records for identification of qualified data for source {1}".format(str(len(all_qualified_trade)),source['source_id'],)
                                )

                        app.logger.info('Before converting to dictionary')
                        start = time.time()
                        all_qual_trd_dict_split = util.split(all_qualified_trade, 1000)
                        app.logger.info('Time taken for converting to dictionary and spliting all_qual_trd '+str((time.time() - start)*1000))

                        #print(exch_rt_dict)
                        start=time.time()

                        # mp=partial(map_data_to_cells,all_bus_rl_dict,exch_rt_dict,reporting_currency)
                        mp = partial(self.map_data_to_cells, all_business_rules)

                        app.logger.info('CPU Count: ' + str(cpu_count()))
                        if cpu_count()>1 :
                            pool=Pool(cpu_count()-1)
                        else:
                            app.logger.info('No of CPU is only 1, ... Inside else....')
                            pool=Pool(1)
                        result_set=pool.map(mp,all_qual_trd_dict_split)
                        pool.close()
                        pool.join()

                        app.logger.info('Time taken by pool processes '+str((time.time() - start)*1000))

                        start=time.time()
                        result_set_flat=util.flatten(result_set)
                        start=time.time()
                        #for result_set_flat in rs:

                        resultdf=resultdf.append(pd.DataFrame(data=result_set_flat,columns=['report_id','sheet_id' ,'cell_id' ,'cell_calc_ref','source_id','qualifying_key','reporting_date']))

                        app.logger.info('Time taken by resultdf.append processes '+str((time.time() - start)*1000))

                    qualified_data_version[str(source_id)][vers['business_date']]= vers['version']

                start=time.time()
                # print(resultdf)
                for idx,grp in resultdf.groupby(['report_id','sheet_id' ,'cell_id' ,'cell_calc_ref','source_id','reporting_date']):
                    #print(grp['report_id'],grp['sheet_id'])
                    grp_report_id=grp['report_id'].unique()[0]
                    grp_sheet_id=grp['sheet_id'].unique()[0]
                    grp_cell_id = grp['cell_id'].unique()[0]
                    grp_cell_calc_ref = grp['cell_calc_ref'].unique()[0]
                    grp_source_id=grp['source_id'].unique()[0]
                    grp_reporting_date = grp['reporting_date'].unique()[0]
                    id_list=grp['qualifying_key'].tolist()

                    id_list_df=id_list_df.append({'report_id':grp_report_id,'sheet_id':grp_sheet_id ,'cell_id':grp_cell_id,
                    'cell_calc_ref':grp_cell_calc_ref,'source_id':grp_source_id,'reporting_date':grp_reporting_date,
                     'id_list_no':id_list},ignore_index=True)
                app.logger.info('Time taken by resultdf.groupby loop for source processes '+str((time.time() - start)*1000))

                id_list_df['version']=report_version_no
                id_list_df[['source_id','reporting_date','version']]=id_list_df[['source_id','reporting_date','version']].astype(dtype='int64',errors='ignore')
                id_list_df['id_list']=id_list_df['id_list_no'].apply(lambda x: ",".join(map(str,list(x))))
                id_list_df=id_list_df.drop(['id_list_no'],axis=1)
                #print(id_list_df)
                columns=",".join(id_list_df.columns)
                placeholders=",".join(['%s']*len(id_list_df.columns))
                id_list_rec=list(id_list_df.itertuples(index=False, name=None))

                self.db.transactmany("insert into report_qualified_data_link ({0}) values({1})".format(columns,placeholders),id_list_rec)
                app.logger.info('Time taken by database inserts '+ str((time.time() - start) * 1000))
                self.db.commit()

                app.logger.info('Time taken by complete loop of qualified data '+ str((time.time() - startcur) * 1000))
                if self.log_master_id:
                    self.opsLog.write_log_detail(master_id=self.log_master_id
                        , operation_sub_type='Finised Processing source {}'.format(source['source_id'])
                        , operation_status='Complete'
                        , operation_narration="Finished identification of qualified data for source {}".format(source['source_id'],)
                        )
            app.logger.info('Time taken by complete loop of all sources '+ str((time.time() - startsource) * 1000))
            report_snapshot=json.dumps({"report_calc_def":report_rule_version,"report_comp_agg_def":comp_agg_rule_version,
                                        "qualified_data":qualified_data_version})
            #print(report_snapshot)
            if self.log_master_id:
                self.opsLog.write_log_detail(master_id=self.log_master_id
                    , operation_sub_type='Finised Processing all sources'
                    , operation_status='Complete'
                    , operation_narration="Finished identification of qualified data for all sources successfully"
                    )
            return report_snapshot
        except Exception as e:
            app.logger.error(str(e))
            if self.log_master_id:
                self.opsLog.write_log_detail(master_id=self.log_master_id
                    , operation_sub_type='Error occured while creating report details'
                    , operation_status='Failed'
                    , operation_narration='Report creation Failed with error : {0}'.format(str(e),))
            raise(e)


    def get_fx_rate(self,dfrow,column):

        try:
            fxrate = float(0)
            referece_rate_date=str(int(dfrow['referece_rate_date']))
            if referece_rate_date in self.er.keys():
                fxrate = float(self.er[referece_rate_date][str(self.reporting_currency)][dfrow[column]])
                # print('Inside if fxrate... {0} {1}'.format(dfrow[column],fxrate))
            return fxrate
        except Exception as e:
            app.logger.error(str(e))
            raise(e)


    def apply_formula_to_frame(self, df, excel_formula,new_field_name):
        try:
            context=fm_trns.Context('df')
            rpn_expr=fm_trns.shunting_yard(excel_formula)
            G,root=fm_trns.build_ast(rpn_expr)
            df_expr=root.emit(G,context=context)
            for col in list(df.columns):
                if col != 'referece_rate_date':
                    df[col]=df[col].astype(dtype='float64',errors='ignore')
            #print(df_expr)
            df[new_field_name]=eval(df_expr)
            return df
        except Exception as e:
            app.logger.error(str(e))
            raise(e)


    def get_list_of_columns_for_dataframe(self,agg_df,table_name):
        try:
            table_def = self.db.query("describe " + table_name).fetchall()

            #Now build the agg column list
            df_agg_column_list=''
            for agg_ref in agg_df['aggregation_ref'].unique():
                df_agg_column_list += '/' + agg_ref

            #check for column in the agg column list search_string
            #if present add to the table column list
            table_col_list=''
            for col in table_def:
                if col['Field'] in df_agg_column_list:
                    if table_col_list == '':
                        table_col_list = col['Field']
                    else:
                        table_col_list += ',' + col['Field']

            #Iftable col list is blank, that means we have to select all the columns of the table
            table_col_list=(table_col_list,'1 as const')[table_col_list=='']
            app.logger.info(table_col_list)
            return table_col_list
        except Exception as e:
            app.logger.error(str(e))
            raise(e)


    def create_report_summary_by_source(self,report_version_no,report_snapshot,**kwargs):

        app.logger.info("Creating report summary by source")
        try:
            parameter_list = ['report_id', 'business_date_from', 'business_date_to','reporting_currency','ref_date_rate','rate_type']

            if set(parameter_list).issubset(set(kwargs.keys())):
                report_id = kwargs["report_id"]
                business_date_from = kwargs["business_date_from"]
                business_date_to = kwargs["business_date_to"]
                reporting_date = business_date_from+business_date_to
                reporting_currency = kwargs['reporting_currency']
                ref_date_rate=kwargs["ref_date_rate"]
                rate_type=kwargs["rate_type"]
                as_of_reporting_date = kwargs["as_of_reporting_date"]

            else:
                app.logger.info("Please supply parameters: " + str(parameter_list))

            self.db.transact("create temporary table tmp_qd_id_list(idlist bigint)")
            if ref_date_rate=='B':
                sql = 'select business_date,from_currency,to_currency,rate from exchange_rate ' \
                        ' where business_date between {0} and {1} ' \
                        ' and rate_type=\'{2}\' and in_use=\'Y\''.format(business_date_from,business_date_to,rate_type)
            else:
                sql = 'select business_date,from_currency,to_currency,rate from exchange_rate ' \
                        ' where business_date={0} ' \
                        ' and rate_type=\'{1}\' and in_use=\'Y\''.format(as_of_reporting_date,rate_type)

            ercur=self.db.query(sql).fetchall()
            for rate in ercur:
                if str(rate['business_date']) in self.er.keys():
                    if str(rate['to_currency']) in self.er[str(rate['business_date'])].keys():
                        self.er[str(rate['business_date'])][str(rate['to_currency'])].update({str(rate['from_currency']): rate['rate']})
                    else:
                        self.er[str(rate['business_date'])].update({str(rate['to_currency']):{str(rate['from_currency']): rate['rate']}})
                else:
                    self.er.update({str(rate['business_date']): {str(rate['to_currency']):{str(rate['from_currency']): rate['rate']}}})

            parameters = pd.DataFrame([kwargs])
            app.logger.info("Parameter DataFrame {}".format(parameters,))
            result_set=[]
            # for static parameters value.
            sql = "select * from report_calc_def where \
                    report_id='{0}' and source_id=0 and in_use='Y'".format(report_id)
            all_agg_param = self.db.query(sql).fetchall()
            app.logger.info("Parameter DataFrame {}".format(all_agg_param,))
            for calc_rule in all_agg_param:
                agg_ref = calc_rule["aggregation_ref"]
                sheet_id = calc_rule["sheet_id"]
                cell_id = calc_rule["cell_id"]
                cell_calc_ref = calc_rule["cell_calc_ref"]
                agg_func = calc_rule["aggregation_func"]

                source_data_trans = self.apply_formula_to_frame(parameters, agg_ref, 'static_value')
                source_data_trans['static_value']=source_data_trans['static_value'].astype(dtype='float64',errors='ignore')
                summary = eval("source_data_trans['static_value']." + agg_func + "()")
                # app.logger.info("summary {0} and data type: {1}".format(summary,type(summary)))
                # summary=0 if math.isnan(summary) else (summary)
                summary=summary.tolist()[0]
                # app.logger.info("summary2 {0} and data type: {1}".format(summary,type(summary)))
                result_set.append({'source_id': '0', 'report_id': report_id, 'sheet_id': sheet_id, 'cell_id': cell_id,
                                   'cell_calc_ref': cell_calc_ref, 'reporting_date': reporting_date, 'cell_summary': summary, 'version':report_version_no})

            report_snapshot=json.loads(report_snapshot)
            report_calc_def_vers=report_snapshot["report_calc_def"]
            srcs=report_calc_def_vers.keys()

            for src in srcs:
                app.logger.info("Processing source {}".format(src,report_calc_def_vers[src]))
                if self.log_master_id:
                    self.opsLog.write_log_detail(master_id=self.log_master_id
                        , operation_sub_type='Aggregation for source {0}'.format(src)
                        , operation_status='Aggregation'
                        , operation_narration="Processing aggegation summary by source for source {0}".format(src,)
                        )
                sql = "select a.sheet_id,a.cell_id,a.aggregation_ref,a.cell_calc_ref,a.aggregation_func from report_calc_def a,\
                      (select id_list from report_calc_def_vers where report_id='{0}' and source_id={1} and version={2}) c \
                      where a.report_id='{0}' and a.source_id={1} and instr(concat(',',c.id_list,','),concat(',',a.id,','))".format(report_id,src,
                      report_calc_def_vers[src])

                all_agg_cls = pd.DataFrame(self.db.query(sql).fetchall())
                #Convert to float where possible to reduce memory usage
                all_agg_cls=all_agg_cls.astype(dtype='float64',errors='ignore')

                source_table_name=self.db.query("select source_table_name from data_source_information where \
                                  source_id=%s",(src,)).fetchone()['source_table_name']
                key_column= 'id' #util.get_keycolumn(self.db._cursor(), source_table_name)

                col_list = self.get_list_of_columns_for_dataframe(all_agg_cls, source_table_name)
                if key_column not in col_list:
                    col_list = key_column + ',' + col_list
                col_list_df = col_list + ',referece_rate_date'
                if ref_date_rate=='B':
                    col_list += ',business_date as referece_rate_date '
                else:
                    col_list += ',' + str(as_of_reporting_date) + ' as referece_rate_date '

                report_qualified_data_link=self.db.query("select sheet_id,cell_id,cell_calc_ref,id_list from report_qualified_data_link \
                                           a where report_id=%s and reporting_date=%s and version=%s\
                                           and a.source_id=%s",(report_id,reporting_date,report_version_no,src)).fetchall()

                for row in report_qualified_data_link:
                    # app.logger.info("Processing report qualified data link loop {}".format(row['cell_calc_ref'],))
                    source_data=pd.DataFrame(columns=list(col_list_df.split(',')))
                    start = time.time()
                    qd_id_list = [(id,) for id in row['id_list'].split(',')]
                    self.db.transact("truncate table tmp_qd_id_list")
                    self.db.transactmany("insert into tmp_qd_id_list(idlist) values (%s)",qd_id_list)
                    app.logger.info('Time taken for populating tmp_qd_id_list ' + str((time.time() - start) * 1000))
                    curdata=self.db.query("select {2} from {0}, tmp_qd_id_list  where {1}=idlist".format(source_table_name,key_column,col_list))

                    startcur = time.time()
                    while True:
                        start=time.time()
                        all_qualified_trade=curdata.fetchmany(50000)
                        app.logger.info('Time taken for fetching next 50000 report qualfied data link '+str((time.time() - start)*1000))
                        if not all_qualified_trade:
                            break
                        source_data=source_data.append(pd.DataFrame(all_qualified_trade))
                    app.logger.info('Time taken for fetching source data .. ' + str((time.time() - startcur) * 1000))
                    source_data.fillna(0.0,inplace=True)
                    for col in list(source_data.columns):
                        source_data[col]=source_data[col].astype(dtype='float64',errors='ignore')

                    agg_ref=all_agg_cls.loc[(all_agg_cls['sheet_id']==row['sheet_id']) & (all_agg_cls['cell_id']==row['cell_id']) &
                                            (all_agg_cls['cell_calc_ref'] == row['cell_calc_ref'])]['aggregation_ref'].reset_index(drop=True).at[0]
                    agg_func=all_agg_cls.loc[(all_agg_cls['sheet_id']==row['sheet_id']) & (all_agg_cls['cell_id']==row['cell_id']) &
                                            (all_agg_cls['cell_calc_ref'] == row['cell_calc_ref'])]['aggregation_func'].reset_index(drop=True).at[0]

                    source_data_trans = self.apply_formula_to_frame(source_data, agg_ref, 'reporting_value')
                    source_data_trans['reporting_value']=source_data_trans['reporting_value'].astype(dtype='float64',errors='ignore')
                    summary = eval("source_data_trans['reporting_value']." + agg_func + "()")
                    # summary=0 if math.isnan(summary) else (summary)
                    app.logger.info("Summary by source for ...{0} {1}".format(row['sheet_id'],row['cell_id'],))

                    result_set.append({'source_id':src,'report_id':report_id,'sheet_id':row['sheet_id'],'cell_id':row['cell_id'],
                                       'cell_calc_ref':row['cell_calc_ref'],'reporting_date':reporting_date,'cell_summary':summary,'version':report_version_no})


            if len(result_set) > 0 :
                result_df=pd.DataFrame(result_set)
                columns=",".join(result_df.columns)
                placeholders=",".join(['%s']*len(result_df.columns))
                data=list(result_df.itertuples(index=False,name=None))

                sql = "insert into report_summary_by_source({0}) values({1})".format(columns,placeholders)
                self.db.transactmany(sql,data)
                self.db.commit()
        except Exception as e:
            app.logger.error(str(e))
            if self.log_master_id:
                self.opsLog.write_log_detail(master_id=self.log_master_id
                    , operation_sub_type='Error occured while creating report summary by source'
                    , operation_status='Failed'
                    , operation_narration='Report creation Failed with error : {0}'.format(str(e),))
            raise(e)
        #return

    def create_report_summary_final(self,report_version_no,report_snapshot,populate_summary = True,cell_format_yn = 'N',**kwargs):
        try:
            app.logger.info("Creating final Report-summary")
            if self.log_master_id:
                self.opsLog.write_log_detail(master_id=self.log_master_id
                    , operation_sub_type='Final Aggregation for report'
                    , operation_status='Aggregation'
                    , operation_narration="Processing aggegation summary for the entire report"
                    )
            parameter_list = ['report_id', 'business_date_from', 'business_date_to']

            if set(parameter_list).issubset(set(kwargs.keys())):
                report_id = kwargs["report_id"]
                business_date_from = kwargs["business_date_from"]
                business_date_to = kwargs["business_date_to"]
                reporting_date = business_date_from + business_date_to
            else:
                app.logger.info("Please supply parameters: " + str(parameter_list))

            report_snapshot=json.loads(report_snapshot)
            comp_agg_rule_version=report_snapshot['report_comp_agg_def']

            contributors = self.db.query("SELECT * FROM report_summary_by_source WHERE reporting_date=%s AND report_id=%s and version=%s",
                (reporting_date, report_id,report_version_no)).fetchall()

            comp_agg_cls = self.db.query("SELECT a.* FROM report_comp_agg_def a,(select id_list from report_comp_agg_def_vers\
                          where report_id=%s and version=%s) b where instr(concat(',',b.id_list,','),concat(',',a.id,','))",
                          (report_id,comp_agg_rule_version)).fetchall()

            formula_set = {}
            for element in contributors:
                formula_set[element['cell_calc_ref']] = {
                    'formula': '"'+element['cell_summary']+'"' if isinstance(eval(element['cell_summary']),str) else element['cell_summary'],
                    'reporting_scale': "NONE" if isinstance(eval(element['cell_summary']),str) else 1,
                    'rounding_option': "NONE"
                }

            for cls in comp_agg_cls:
                ref = cls['comp_agg_ref']
                formula_set[ref] = {'formula': cls['comp_agg_rule'],
                                    'reporting_scale': cls['reporting_scale'] if cls['reporting_scale'] else "NONE",
                                    'rounding_option': cls['rounding_option'] if cls['rounding_option'] else "NONE"
                                    }

            #print(formula_set)
            summary_set = fm_trns2.tree(formula_set, format_flag=cell_format_yn)
            # print(summary_set)

            result_set = []
            for cls in comp_agg_cls:
                result_set.append({'report_id':cls['report_id'],'sheet_id':cls['sheet_id'],'cell_id':cls['cell_id'],
                'cell_summary':eval(summary_set[cls['comp_agg_ref']]),'reporting_date':reporting_date,'version':report_version_no})


            if populate_summary:
                try:
                    if len(result_set) > 0 :
                        result_df = pd.DataFrame(result_set)
                        columns = ",".join(result_df.columns)
                        placeholders = ",".join(['%s'] * len(result_df.columns))
                        data = list(result_df.itertuples(index=False, name=None))
                        rowId = self.db.transactmany("INSERT INTO report_summary({0}) values({1})".format(columns,placeholders),data)
                        self.db.commit()
                        return rowId
                except Exception as e:
                    self.db.rollback()
                    app.logger.info("Transaction Failed:", e)
                    raise(e)
            else:
                return result_set
        except Exception as e:
            app.logger.error(str(e))
            if self.log_master_id:
                self.opsLog.write_log_detail(master_id=self.log_master_id
                    , operation_sub_type='Error occured while creating final report summary'
                    , operation_status='Failed'
                    , operation_narration='Report creation Failed with error : {0}'.format(str(e),))
            raise(e)
