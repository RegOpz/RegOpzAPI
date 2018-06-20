from flask_restful import Resource
import time
from multiprocessing import Pool,cpu_count
from functools import partial
import re
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
from Parser.PandasLib import *

class GenerateReportController(Resource):
    def __init__(self):
        self.domain_info = autheticateTenant()
        if self.domain_info:
            tenant_info = json.loads(self.domain_info)
            self.tenant_info = json.loads(tenant_info['tenant_conn_details'])
            self.db=DatabaseHelper(self.tenant_info)
        self.er = {}

    @authenticate
    def get(self):
        if(request.endpoint=='get_report_list_ep'):
            country=request.args.get('country') if request.args.get('country') != None else 'ALL'
            return self.get_report_list(country)
        if (request.endpoint == 'get_country_list_ep'):
            return self.get_country_list()

    def post(self):
        if (request.endpoint=='create_report_ep'):
            report_info=request.get_json(force=True)
            self.reporting_currency = report_info["reporting_currency"]
            report_id=report_info['report_id']
            reporting_date=report_info['reporting_date']
            as_of_reporting_date=report_info['as_of_reporting_date']
            report_create_date=report_info['report_create_date']
            report_create_status=report_info['report_create_status']

            report_parameters = "'business_date_from':'" + report_info["business_date_from"] + "'," + \
                                "'business_date_to':'" + report_info["business_date_to"] + "'," + \
                                "'reporting_currency':'" + report_info["reporting_currency"] + "'," + \
                                "'ref_date_rate':'" + report_info["ref_date_rate"] + "'," + \
                                "'rate_type':'" + report_info["rate_type"] +"'"
            if(report_info["report_parameters"]):
                report_parameters=report_parameters+","+report_info["report_parameters"]

            report_kwargs=eval("{"+"'report_id':'" + report_id + "',"+ report_parameters + "}")

            report_version_no=self.create_report_catalog(report_id,reporting_date,report_create_date,
                                       report_parameters,report_create_status,as_of_reporting_date)
            self.update_report_catalog(status='RUNNING', report_id=report_id, reporting_date=reporting_date,
                                       report_create_date=report_create_date, version=report_version_no)
            report_snapshot=self.create_report_detail(report_version_no,**report_kwargs)
            #print("create_report_summary_by_source")
            self.create_report_summary_by_source(report_version_no,report_snapshot,**report_kwargs)
            # print("create_report_summary_final")
            self.create_report_summary_final(report_version_no,report_snapshot,**report_kwargs)
            # self.db.commit()
            self.update_report_catalog(status='SUCCESS', report_id=report_id, reporting_date=reporting_date,
                                       report_create_date=report_create_date,report_snapshot=report_snapshot,
                                       version=report_version_no)

            return {"msg": "Report generated SUCCESSFULLY for ["+str(report_id)+"] Reporting date ["+str(reporting_date)+"]."}, 200



        if(request.endpoint == 'generate_report_ep'):
            report_info = request.get_json(force=True)
            report_id = report_info['report_id']
            report_create_date=report_info['report_create_date']
            report_parameters = report_info['report_parameters']
            reporting_date = report_info['reporting_date']
            as_of_reporting_date=report_info['as_of_reporting_date']
            report_create_status='CREATE'
            report_kwargs = eval("{'report_id':'" + report_id + "' ," + report_parameters.replace('"',"'") + "}")
            self.reporting_currency = report_kwargs["reporting_currency"]
            # self.update_report_catalog(status='RUNNING',report_id=report_id,reporting_date=reporting_date,report_parameters=report_parameters,report_create_date=report_create_date)
            # self.create_report_detail(**report_kwargs)
            # print("create_report_summary_by_source")
            # self.create_report_summary_by_source(**report_kwargs)
            # print("create_report_summary_final")
            # self.create_report_summary_final(**report_kwargs)
            # self.db.commit()
            # self.update_report_catalog(status='SUCCESS',report_id=report_id,reporting_date=reporting_date,report_create_date=report_create_date)
            report_version_no=self.create_report_catalog(report_id,reporting_date,report_create_date,
                                       report_parameters,report_create_status,as_of_reporting_date)
            self.update_report_catalog(status='RUNNING', report_id=report_id, reporting_date=reporting_date,
                                       report_create_date=report_create_date, version=report_version_no)
            report_snapshot=self.create_report_detail(report_version_no,**report_kwargs)
            #print("create_report_summary_by_source")
            self.create_report_summary_by_source(report_version_no,report_snapshot,**report_kwargs)
            # print("create_report_summary_final")
            self.create_report_summary_final(report_version_no,report_snapshot,**report_kwargs)
            # self.db.commit()
            self.update_report_catalog(status='SUCCESS', report_id=report_id, reporting_date=reporting_date,
                                       report_create_date=report_create_date,report_snapshot=report_snapshot,
                                       version=report_version_no)
            return {"msg": "Report generated SUCCESSFULLY for ["+str(report_id)+"] Reporting date ["+str(reporting_date)+"]."}, 200

    def get_report_list(self,country='ALL'):
        report_list=self.db.query("select distinct report_id from report_def_catalog where country='"+country+"'").fetchall()
        return report_list

    def get_country_list(self):
        country_list=self.db.query("select distinct country from report_def_catalog").fetchall()
        return country_list

    def create_report_catalog(self,report_id,reporting_date,report_create_date,
                              report_parameters,report_create_status,as_of_reporting_date):
        report_version=self.db.query("select max(version) version from report_catalog where report_id=%s and reporting_date=%s",
                                     (report_id,reporting_date)).fetchone()
        report_version_no=1 if not report_version['version'] else  report_version['version']+1

        sql="insert into report_catalog(report_id,reporting_date,report_create_date,\
            report_parameters,report_create_status,as_of_reporting_date,version) values(%s,%s,%s,%s,%s,%s,%s)"
        self.db.transact(sql,(report_id,reporting_date,report_create_date,report_parameters,report_create_status,
        as_of_reporting_date,report_version_no))
        self.db.commit()
        return report_version_no

    def update_report_catalog(self,status,report_id,reporting_date,report_parameters=None,report_create_date=None,report_snapshot=None,version=0):
        update_clause = "report_create_status='{0}'".format(status,)
        if report_parameters != None:
            # Replace all singlequotes(') with double quote(") as update sql requires all enclosed in ''
            update_clause += ", report_parameters='{0}'".format(report_parameters.replace("'",'"'),)
        if report_create_date != None:
            # Replace all singlequotes(') with double quote(") as update sql requires all enclosed in ''
            update_clause += ", report_create_date='{0}'".format(report_create_date.replace("'",'"'),)
        if report_snapshot !=None:
            update_clause +=", report_snapshot='{0}'".format(report_snapshot)
        sql = "update report_catalog set {0} where report_id='{1}' and reporting_date='{2}' and version={3}".format(update_clause,report_id,reporting_date,version)
        self.db.transact(sql)
        self.db.commit()


    def map_data_to_cells(self,list_business_rules,qualified_data):

        result_set=[]
        for qd in qualified_data:
            trd_rules_list=qd["business_rules"].split(',')
            for rl in list_business_rules:
                br_rules_list=rl["cell_business_rules"].split(',')
                if set(br_rules_list).issubset(set(trd_rules_list)):
                   result_set.append((rl["report_id"], rl["sheet_id"],rl["cell_id"],rl["cell_calc_ref"],
                   qd["source_id"],qd["qualifying_key"],qd["reporting_date"]))

        return result_set



    def create_report_detail(self,report_version_no,**kwargs):
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
            print("Please supply parameters: "+str(parameter_list))

        all_sources=self.db.query('select distinct source_id \
                    from report_calc_def where report_id=%s and in_use=\'Y\'',(report_id,)).fetchall()
            #Changes required for incoporating exchange rates

        #Clean the link table before populating for same reporting date
        # print('Before clean_table report_qualified_data_link')
        # start = time.time()
        # util.clean_table(self.db._cursor(), 'report_qualified_data_link', '', reporting_date,'report_id=\''+ report_id + '\'')
        # print('Time taken for clean_table report_qualified_data_link ' + str((time.time() - start) * 1000))

        #Create versionning for REPORT_COMP_AGG_DEF
        cardf = pd.DataFrame(self.db.query("select id,report_id,sheet_id,cell_id,comp_agg_ref,comp_agg_rule,reporting_scale,rounding_option\
                      from report_comp_agg_def WHERE report_id=%s AND in_use='Y'",(report_id,)).fetchall())
        car_version=self.db.query("select report_id,version,id_list from report_comp_agg_def_vers where report_id=%s\
                   and version=(select max(version) from report_comp_agg_def_vers where report_id=%s)",(report_id,report_id)).fetchone()
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

            # for i,rl in all_business_rules.iterrows():
            #     #check for possible report parameter token replacement
            #     for key, value in report_parameter.items():
            #         all_business_rules.iloc[i]['cell_business_rules'] = all_business_rules.iloc[i]['cell_business_rules'].replace(key, key + ':' + value)

            #print('All business rules after report parameter', all_business_rules)
            print('Time taken for converting to dictionary all_business_rules ' + str((time.time() - start) * 1000))

            dbqd=DatabaseHelper(self.tenant_info)
            qdvers=dbqd.query("select a.business_date,a.source_id,a.version,a.id_list,a.br_version from qualified_data_vers a,\
                  (select business_date,source_id,max(version) version from qualified_data_vers where business_date between %s and %s and source_id=%s\
                   group by business_date,source_id) b where a.business_date=b.business_date and a.source_id=b.source_id and a.version=b.version",
                  (business_date_from,business_date_to,source_id)).fetchall()

            id_list_df=pd.DataFrame(columns=['report_id','sheet_id' ,'cell_id' ,'cell_calc_ref','source_id','reporting_date','id_list_no'])
            dbqd.transact("create temporary table tmp_qd_id_list(idlist bigint)")
            for vers in qdvers:
                start = time.time()
                qd_id_list = [(id,) for id in vers['id_list'].split(',')]
                dbqd.transact("truncate table tmp_qd_id_list")
                dbqd.transactmany("insert into tmp_qd_id_list(idlist) values (%s)",qd_id_list)
                print('Time taken for populating tmp_qd_id_list ' + str((time.time() - start) * 1000))
                curdata =dbqd.query("select q.source_id,q.qualifying_key,q.business_rules,q.buy_currency,\
                            q.sell_currency,q.mtm_currency,\
                            %s as reporting_currency,q.business_date,%s as reporting_date,%s as business_date_to, \
                            %s as ref_date_rate from qualified_data q, tmp_qd_id_list qv \
                            where q.source_id=%s and q.business_date=%s \
                            and qv.idlist=q.id" ,
                            (reporting_currency,reporting_date,business_date_to,ref_date_rate,vers['source_id'],
                             vers['business_date']))
                startcur = time.time()
                while True:
                    start=time.time()
                    all_qualified_trade=curdata.fetchmany(50000)
                    print('Time taken for fetching next 50000 qualfied trades '+str((time.time() - start)*1000))
                    if not all_qualified_trade:
                        break

                    print('Before converting to dictionary')
                    start = time.time()
                    all_qual_trd_dict_split = util.split(all_qualified_trade, 1000)
                    print('Time taken for converting to dictionary and spliting all_qual_trd '+str((time.time() - start)*1000))

                    #print(exch_rt_dict)
                    start=time.time()

                    # mp=partial(map_data_to_cells,all_bus_rl_dict,exch_rt_dict,reporting_currency)
                    mp = partial(self.map_data_to_cells, all_business_rules)

                    print('CPU Count: ' + str(cpu_count()))
                    if cpu_count()>1 :
                        pool=Pool(cpu_count()-1)
                    else:
                        print('No of CPU is only 1, ... Inside else....')
                        pool=Pool(1)
                    result_set=pool.map(mp,all_qual_trd_dict_split)
                    pool.close()
                    pool.join()

                    print('Time taken by pool processes '+str((time.time() - start)*1000))

                    start=time.time()
                    result_set_flat=util.flatten(result_set)
                    start=time.time()
                    #for result_set_flat in rs:

                    resultdf=resultdf.append(pd.DataFrame(data=result_set_flat,columns=['report_id','sheet_id' ,'cell_id' ,'cell_calc_ref','source_id','qualifying_key','reporting_date']))

                    print('Time taken by resultdf.append processes '+str((time.time() - start)*1000))

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
            print('Time taken by resultdf.groupby loop for source processes '+str((time.time() - start)*1000))

            id_list_df['version']=report_version_no
            id_list_df[['source_id','reporting_date','version']]=id_list_df[['source_id','reporting_date','version']].astype(dtype='int64',errors='ignore')
            id_list_df['id_list']=id_list_df['id_list_no'].apply(lambda x: ",".join(map(str,list(x))))
            id_list_df=id_list_df.drop(['id_list_no'],axis=1)
            #print(id_list_df)
            columns=",".join(id_list_df.columns)
            placeholders=",".join(['%s']*len(id_list_df.columns))
            id_list_rec=list(id_list_df.itertuples(index=False, name=None))

            self.db.transactmany("insert into report_qualified_data_link ({0}) values({1})".format(columns,placeholders),id_list_rec)
            print('Time taken by database inserts '+ str((time.time() - start) * 1000))
            self.db.commit()

            print('Time taken by complete loop of qualified data '+ str((time.time() - startcur) * 1000))
        print('Time taken by complete loop of all sources '+ str((time.time() - startsource) * 1000))
        report_snapshot=json.dumps({"report_calc_def":report_rule_version,"report_comp_agg_def":comp_agg_rule_version,
                                    "qualified_data":qualified_data_version})
        #print(report_snapshot)
        return report_snapshot

    def get_fx_rate(self,dfrow,column):
        # print(dfrow)
        fxrate = float(0)
        if str(dfrow['referece_rate_date']) in self.er.keys():
            fxrate = float(self.er[str(dfrow['referece_rate_date'])][str(self.reporting_currency)][dfrow[column]])
        return fxrate

    def apply_formula_to_frame(self, df, excel_formula,new_field_name):
        context=fm_trns.Context('df')
        rpn_expr=fm_trns.shunting_yard(excel_formula)
        G,root=fm_trns.build_ast(rpn_expr)
        df_expr=root.emit(G,context=context)
        for col in list(df.columns):
            if col != 'referece_rate_date':
                df[col]=df[col].astype(dtype='float64',errors='ignore')
        df[new_field_name]=eval(df_expr)
        return df

    def get_list_of_columns_for_dataframe(self,agg_df,table_name):
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
        print(table_col_list)
        return table_col_list



    def create_report_summary_by_source(self,report_version_no,report_snapshot,**kwargs):
        parameter_list = ['report_id', 'business_date_from', 'business_date_to','reporting_currency','ref_date_rate','rate_type']

        if set(parameter_list).issubset(set(kwargs.keys())):
            report_id = kwargs["report_id"]
            business_date_from = kwargs["business_date_from"]
            business_date_to = kwargs["business_date_to"]
            reporting_date = business_date_from+business_date_to
            reporting_currency = kwargs['reporting_currency']
            ref_date_rate=kwargs["ref_date_rate"]
            rate_type=kwargs["rate_type"]

        else:
            print("Please supply parameters: " + str(parameter_list))

        self.db.transact("create temporary table tmp_qd_id_list(idlist bigint)")
        if ref_date_rate=='B':
            sql = 'select business_date,from_currency,to_currency,rate from exchange_rate ' \
                    ' where business_date between {0} and {1} ' \
                    ' and rate_type=\'{2}\' and in_use=\'Y\''.format(business_date_from,business_date_to,rate_type)
        else:
            sql = 'select business_date,from_currency,to_currency,rate from exchange_rate ' \
                    ' where business_date={0} ' \
                    ' and rate_type=\'{1}\' and in_use=\'Y\''.format(business_date_to,rate_type)

        ercur=self.db.query(sql).fetchall()
        for rate in ercur:
            if str(rate['business_date']) in self.er.keys():
                if str(rate['to_currency']) in self.er[str(rate['business_date'])].keys():
                    self.er[str(rate['business_date'])][str(rate['to_currency'])].update({str(rate['from_currency']): rate['rate']})
                else:
                    self.er[str(rate['business_date'])].update({str(rate['to_currency']):{str(rate['from_currency']): rate['rate']}})
            else:
                self.er.update({str(rate['business_date']): {str(rate['to_currency']):{str(rate['from_currency']): rate['rate']}}})

        report_snapshot=json.loads(report_snapshot)
        report_calc_def_vers=report_snapshot["report_calc_def"]
        srcs=report_calc_def_vers.keys()

        result_set=[]
        for src in srcs:
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
                col_list += ',' + str(business_date_to) + ' as referece_rate_date '

            report_qualified_data_link=self.db.query("select sheet_id,cell_id,cell_calc_ref,id_list from report_qualified_data_link \
                                       a where report_id=%s and reporting_date=%s and version=%s\
                                       and a.source_id=%s",(report_id,reporting_date,report_version_no,src)).fetchall()

            for row in report_qualified_data_link:
                source_data=pd.DataFrame(columns=list(col_list_df.split(',')))
                start = time.time()
                qd_id_list = [(id,) for id in row['id_list'].split(',')]
                self.db.transact("truncate table tmp_qd_id_list")
                self.db.transactmany("insert into tmp_qd_id_list(idlist) values (%s)",qd_id_list)
                print('Time taken for populating tmp_qd_id_list ' + str((time.time() - start) * 1000))
                curdata=self.db.query("select {2} from {0}, tmp_qd_id_list  where {1}=idlist".format(source_table_name,key_column,col_list))

                startcur = time.time()
                while True:
                    start=time.time()
                    all_qualified_trade=curdata.fetchmany(50000)
                    print('Time taken for fetching next 50000 report qualfied data link '+str((time.time() - start)*1000))
                    if not all_qualified_trade:
                        break
                    source_data=source_data.append(pd.DataFrame(all_qualified_trade))
                print('Time taken for fetching source data .. ' + str((time.time() - startcur) * 1000))
                source_data.fillna(0.0,inplace=True)
                for col in list(source_data.columns):
                    source_data[col]=source_data[col].astype(dtype='float64',errors='ignore')
                # print(source_data.info())
                #hack to go around exchange rate
                # source_data['buy_reporting_rate']=1
                # source_data['sell_reporting_rate']=1

                agg_ref=all_agg_cls.loc[(all_agg_cls['sheet_id']==row['sheet_id']) & (all_agg_cls['cell_id']==row['cell_id']) &
                                        (all_agg_cls['cell_calc_ref'] == row['cell_calc_ref'])]['aggregation_ref'].reset_index(drop=True).at[0]
                agg_func=all_agg_cls.loc[(all_agg_cls['sheet_id']==row['sheet_id']) & (all_agg_cls['cell_id']==row['cell_id']) &
                                        (all_agg_cls['cell_calc_ref'] == row['cell_calc_ref'])]['aggregation_func'].reset_index(drop=True).at[0]

                source_data_trans = self.apply_formula_to_frame(source_data, agg_ref, 'reporting_value')
                source_data_trans['reporting_value']=source_data_trans['reporting_value'].astype(dtype='float64',errors='ignore')
                summary = eval("source_data_trans['reporting_value']." + agg_func + "()")
                summary=0 if math.isnan(float(summary)) else float(summary)
                print("Summary by source for ...{0} {1}".format(row['sheet_id'],row['cell_id'],))

                result_set.append({'source_id':src,'report_id':report_id,'sheet_id':row['sheet_id'],'cell_id':row['cell_id'],
                                   'cell_calc_ref':row['cell_calc_ref'],'reporting_date':reporting_date,'cell_summary':summary,'version':report_version_no})


        result_df=pd.DataFrame(result_set)
        columns=",".join(result_df.columns)
        placeholders=",".join(['%s']*len(result_df.columns))
        data=list(result_df.itertuples(index=False,name=None))

        self.db.transactmany("insert into report_summary_by_source({0}) values({1})".format(columns,placeholders),data)
        self.db.commit()
        #return

    def create_report_summary_final(self,report_version_no,report_snapshot,populate_summary = True,cell_format_yn = 'N',**kwargs):
        parameter_list = ['report_id', 'business_date_from', 'business_date_to']

        if set(parameter_list).issubset(set(kwargs.keys())):
            report_id = kwargs["report_id"]
            business_date_from = kwargs["business_date_from"]
            business_date_to = kwargs["business_date_to"]
            reporting_date = business_date_from + business_date_to
        else:
            print("Please supply parameters: " + str(parameter_list))

        report_snapshot=json.loads(report_snapshot)
        comp_agg_rule_version=report_snapshot['report_comp_agg_def']
        # if populate_summary:
        #     util.clean_table(self.db._cursor(), 'report_summary', '', reporting_date, 'report_id=\''+ report_id + '\'')

        contributors = self.db.query("SELECT * FROM report_summary_by_source WHERE reporting_date=%s AND report_id=%s and version=%s",
            (reporting_date, report_id,report_version_no)).fetchall()

        comp_agg_cls = self.db.query("SELECT a.* FROM report_comp_agg_def a,(select id_list from report_comp_agg_def_vers\
                      where report_id=%s and version=%s) b where instr(concat(',',b.id_list,','),concat(',',a.id,','))",
                      (report_id,comp_agg_rule_version)).fetchall()

        formula_set = {}
        for element in contributors:
            formula_set[element['cell_calc_ref']] = {
                'formula': element['cell_summary'],
                'reporting_scale': 1,
                'rounding_option': "NONE"
            }

        for cls in comp_agg_cls:
            ref = cls['comp_agg_ref']
            formula_set[ref] = {'formula': cls['comp_agg_rule'],
                                'reporting_scale': cls['reporting_scale'] if cls['reporting_scale'] else 1,
                                'rounding_option': cls['rounding_option'] if cls['rounding_option'] else "NONE"
                                }

        #print(formula_set)
        summary_set = tree(formula_set, format_flag=cell_format_yn)
        print(summary_set)

        result_set = []
        for cls in comp_agg_cls:
            result_set.append({'report_id':cls['report_id'],'sheet_id':cls['sheet_id'],'cell_id':cls['cell_id'],
            'cell_summary':summary_set[cls['comp_agg_ref']],'reporting_date':reporting_date,'version':report_version_no})

        # print(result_set)

        if populate_summary:
            try:
                result_df = pd.DataFrame(result_set)
                columns = ",".join(result_df.columns)
                placeholders = ",".join(['%s'] * len(result_df.columns))
                data = list(result_df.itertuples(index=False, name=None))
                rowId = self.db.transactmany("INSERT INTO report_summary({0}) values({1})".format(columns,placeholders),data)
                self.db.commit()
                return rowId
            except Exception as e:
                self.db.rollback()
                print("Transaction Failed:", e)
        else:
            return result_set
