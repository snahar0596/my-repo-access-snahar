import importlib
import logging
import os
import os.path
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd
import boto3
import json
import re
import time
import datetime
import difflib
import pickle
from whoosh import index
from whoosh import qparser
from whoosh.qparser.plugins import FuzzyTermPlugin, PrefixPlugin
from whoosh.query import SpanNear, SpanOr, SpanNear2, FuzzyTerm
from whoosh.qparser import QueryParser
from whoosh import scoring
from whoosh.fields import Schema, TEXT, ID
from sklearn.linear_model import LogisticRegression
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from botocore.config import Config
import json

# workflow_module = importlib.import_module("etl-plugin-scripts.helper.get_workflow")
args = getResolvedOptions(sys.argv,['manifestBucket'])
# Specify the S3 bucket and key where your boto_config.json is stored
s3_bucket = args['manifestBucket']
s3_key = 'services/boto_config.json'

# Create an S3 client
s3_client = boto3.client('s3')

# Get the JSON file from S3
response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
config_data = json.loads(response['Body'].read().decode('utf-8'))
boto_config=config_data.get('config',{})
# Create a Config object using the loaded configuration
config = Config(**boto_config)
# workflow_prop = workflow_module.GlueWorkflowProperties(config)


log = logging.getLogger('ETL::')
log.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)

glue_client = boto3.client("glue", config=config)

params = [
    'inputBucket', 'manifestBucket', 'manifestPath', 'masterIndexPath', 'notificationJobName', 'crawlerJobName',
    'outputBucket', 'JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID', 'domain_model_path','mongoBucket'
]

args = getResolvedOptions(
    sys.argv, params
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job_name = args['JOB_NAME']
log.debug("Job name is : %s", args['JOB_NAME'])
inputBucket = args['inputBucket']
log.debug('inputBucket %s', inputBucket)
outputBucket = args['outputBucket']
log.debug('outputBucket %s', outputBucket)
mongo_bucket = args['mongoBucket']
log.debug('mongo_bucket %s', mongo_bucket)
workflow_name = args['WORKFLOW_NAME']
log.debug("WORKFLOW_NAME: %s", workflow_name)
workflow_run_id = args['WORKFLOW_RUN_ID']
log.debug("WORKFLOW_RUN_ID : %s", workflow_run_id)
workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)["RunProperties"]
crawlerjob = args['crawlerJobName']
log.debug('ETL: %s', crawlerjob)
file_key = workflow_params['file_key']
source = workflow_params['source']
transaction_id = workflow_params['transaction_id']
log.debug('file_key %s source %s transaction_id is: %s', file_key, source, transaction_id)
inputfile = os.path.basename(file_key)
log.debug('inputfile %s', inputfile)
inputfile_name = os.path.splitext(inputfile)
log.debug('inputfile_name %s', inputfile_name)

# Reading Manifest.json file
manifestBucketName = args['manifestBucket']
manifestFilePath = args['manifestPath']
notification_job = args['notificationJobName']
domain_model_path = args['domain_model_path']
log.info(
    "The manifestBucketName is: %s The manifestFilePath is: %s  The notification_job is: %s The Domain model Path is: %s",
    manifestBucketName, manifestFilePath, notification_job, domain_model_path)
ms_source_name = source.lower()

s3 = boto3.resource('s3')
obj = s3.Object(manifestBucketName, manifestFilePath)
mf_data = json.load(obj.get()['Body'])
log.info("Manifest data: %s", mf_data)


# customerid = mf_data[ms_source_name]['customers'][0]['customerId']
# log.debug('customerid', customerid)

# logger = logging.getLogger(__name__)

def send_to_timestream_events(transaction_id, manifest_bucket_name, domain_model_path, inputBucketName,
                              file_path, spark, state, PipelineModule, measure_name):
    # Send an event along with prepared arguments to Timestream with the specified parameters. This function calls the nvi_timestream module to create a CSV file and store the data in the Timestream database.
    if source == "nvi":
        TIMESTREAM = "timeStream"
        timestream_script_name = f"{source}_{TIMESTREAM}.py"
        timestream_plug = module_import(timestream_script_name, TIMESTREAM)
        log.info("TIMESTREAM TRIGGERED STARTS - %s %s", PipelineModule, state)
        timestream_plug.time_stream_operation_main(state, PipelineModule, measure_name, transaction_id,
                                                    manifest_bucket_name, domain_model_path, inputBucketName,
                                                    file_path, outputBucket, spark)
        log.info("TIMESTREAM TRIGGERED ENDS - %s %s", PipelineModule, state)


"""def trigger_glue_job(file_key, notification_job, bucket_name, transaction_id, error_code, status):
    log.debug("This is going to trigger a glue job: %s", notification_job)
    glue_Job_Name_Arg = notification_job
    bucket_name_Arg = bucket_name
    file_key_Arg = file_key
    filepath_Arg = 's3://' + bucket_name_Arg + '/' + file_key_Arg

    glue_client = boto3.client('glue', config=config)
    # glue_client.update_workflow(Name=WFName, DefaultRunProperties={'filekey': file_key, 'session_id': session_id})
    # glue_client.start_workflow_run(Name=WFName)

    response = glue_client.start_job_run(JobName=notification_job, Arguments={
        '--processed_csv_path': filepath_Arg, '--module': job_name, '--transaction_id': transaction_id,
        '--error_code': str(error_code), '--status': status
    })
    log.info('## STARTED GLUE JOB: %s', glue_Job_Name_Arg)
    log.info('## GLUE JOB RUN ID: %s', response['JobRunId'])
    return response"""


def match_with_v2(row, confidencescore, v1_match_dict):
    # ['machine_DOB_simm', 'machine_FirstName_simm', 'machine_LastName_simm']
    row_df = pd.DataFrame(row).T
    row_df.reset_index(inplace=True)
    # row_df.fillna('', inplace=True)
    indpVars = ['NORMALIZED_DOB', 'FirstName', 'LastName']
    s3_model_index_path = "model-repository/auto-index/base/v3/"
    model_index_path = "./tmp/model-repository/auto-index/base/v3/"

    if not os.path.exists(model_index_path):
        s3_image_dir = f"s3://{outputBucket}/{s3_model_index_path}"
        s3downloadprefix = os.path.dirname(s3_image_dir)
        download_froms3(s3downloadprefix, './tmp', '.model')

    row_df = discover_field_manualmatch(row_df, v1_match_dict, 0, indpVars)

    row_df.at[0, 'Prev_ConfidenceScore'] = confidencescore
    
    pred = model_predict_single_reinforce(row_df, model_index_path, 0)
    predValue = (pred > 0.89)
    # try:
    #    predValue = model_predict_single_reinforce(row_df, model_index_v2_path, 0 )
    # except Exception as e:
    #    predValue = False
    #    log.debug(f'ERROR IN MODEL V2 MATCHING - MIGHT BE DATA ISSUE {e}')

    log.info(f' MACHINE MATCH IS {predValue} WITH MATCHSCORE OF {pred}')
    isRequiredMatch=False
    #if(isSimilarFuzzy(row['NORMALIZED_DOB'],v1_match_dict['NORMALIZED_DOB']) and isSimilarFuzzy(row['FirstName'],v1_match_dict['FirstName'])):
    #    isRequiredMatch = True
    if(isSimilarFuzzy(row['NORMALIZED_DOB'],v1_match_dict['NORMALIZED_DOB'])):
        isRequiredMatch = True
    
    #row_df_check = normalize_ml_data(row_df, ['machine_NORMALIZED_DOB_simm', 'machine_FirstName_simm'])
    #isRequiredMatch = (int(row_df_check.at[0, 'RequiredMatch']) == 1)
    #isRequiredMatch = True
    log.info(f' IS REQUIRED MATCH {isRequiredMatch}')
    
    model_response = dict()
    model_response['ismatch'] = (bool(predValue) and isRequiredMatch)
    model_response['matchscore'] = pred
    
    
    #arresponse = [int(bool(predValue) and isRequiredMatch),]

    return model_response


def model_predict_single_reinforce(df, modelrepodir, idx):
    log.debug(f'EXECUTING MODEL PREDICTION for {idx}')
    log.debug(f' THE VALUES {df.head()}')
    filepath = f'{modelrepodir}autoindex-re.model'
    lstNames_x = ['machine_NORMALIZED_DOB_simm', 'machine_FirstName_simm', 'machine_LastName_simm',
                  'Prev_ConfidenceScore', 'RequiredMatch', 'Customer ID #_Exists']
    # df.rename(columns={' DOB_match': 'DOB_match', ' Phone1_match': 'Phone1_match'}, inplace=True)
    # df['machine_NORMALIZED_DOB_simm'] = df['machine_NORMALIZED_DOB_simm'].astype(int)
    # df['machine_FirstName_simm'] = df['machine_FirstName_simm'].astype(int)
    # df['machine_LastName_simm'] = df['machine_LastName_simm'].astype(int)
    df = prep_exists_ml_data(df, ['Customer ID #'])
    df = normalize_ml_data(df, ['machine_NORMALIZED_DOB_simm','machine_FirstName_simm', 'machine_LastName_simm'])
    log.debug(f' THE VALUES {df.iloc[idx][lstNames_x].values}')
    row = df.iloc[idx]
    row_df = pd.DataFrame(row).T
    X = row_df[lstNames_x].values

    if (os.path.exists(filepath)):
        model = pickle.load(open(filepath, 'rb'))
        log.debug(f'MODEL EXISTS FOR {filepath}')
    else:
        log.debug(f' MODEL DOES NOT EXIST {filepath}')

    # list of files in model directory
    contents = os.listdir(f'{modelrepodir}')

    for item in contents:
        print(item)

    pred = custom_predict(model,X)

    return pred

def custom_predict(model, X, threshold=0):
    probs = model.predict_proba(X) 
    return (probs[:, 1])

def prep_exists_ml_data(df, lstFieldNames):
    for index, row in df.iterrows():
        fieldvalexists = False
        #currfieldname = ''
        for fieldname in list(lstFieldNames):
            currfieldname = f'{fieldname}_Exists'
            #if(fieldname == 'Match'): continue
            
            try:
                fieldval = int(df.at[index, fieldname])
                if(fieldval > 0): fieldvalexists = True
                #else: fieldvalexists = False
                
            except Exception as e:
                #fieldvalexists = False
                logging.info(f"EXCEPTION FOR FIELDNAME: {fieldname} : {e}")
                df.at[index, fieldname] = 0
                logging.info(f"UPDATE AND RETRIEVE LATEST FOR FIELDNAME: {fieldname} : {df.at[index, fieldname]}")
                
        if(fieldvalexists): df.at[index, currfieldname] = 1
        else: df.at[index, currfieldname] = 0
    return df


def normalize_ml_data(df, requiredFieldNames):
    for index, row in df.iterrows():
        requiredmatch = True
        for fieldname in list(requiredFieldNames):

            if (not (int(df.at[index, fieldname]) == 1)): requiredmatch = False
        if (requiredmatch):
            df.at[index, 'RequiredMatch'] = 1
        else:
            df.at[index, 'RequiredMatch'] = 0
    return df


def download_froms3(s3prefix, localdir, file_format):
    log.info('Inside mergeimages_froms3 function')
    s3endpoint = f'{s3prefix}'
    log.debug(f' THE FULL S3 ENDPOINT IS {s3endpoint}/')
    download_s3_directory(s3endpoint, f'{localdir}', file_format)
    return


def download_s3_directory(bucketurl, local_directory, file_format=''):
    bucketstr_arr = bucketurl.replace('s3://', '').split('/')
    bucket_name = bucketstr_arr[0]
    bucketstr_arr_length = len(bucketstr_arr)
    s3_directory = '/'.join(bucketstr_arr[1:bucketstr_arr_length])
    my_session = boto3.Session(region_name="us-east-2")

    s3_client = my_session.client('s3')
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    # s3 = boto3.client('s3')
    try:
        for obj in bucket.objects.filter(Prefix=s3_directory):
            if not str(obj.key).startswith(s3_directory): continue
            if (file_format):
                if (not str(obj.key).endswith(file_format)): continue

            local_file_path = os.path.join(local_directory, obj.key)
            log.debug(f'CREATING LOCAL DIRECTORY FOR {local_file_path}')
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            s3_client.download_file(bucket_name, obj.key, f'{local_file_path}')
    except Exception as e:
        log.debug(f'DID NOT SUCCESSFULLY DOWNLOAD FROM S3 WITH MESSAGE {str(e)}')


def discover_field_manualmatch(df, match_dict, idx, lstIndpFields):
    # return df

    # customer_code = getCustomerCode(df)
    # configkey = f'SUPERFIELD_MAPPING_{customer_code}'

    # configval = os.environ.get(configkey)

    log.debug(df.info())

    log.debug(f' EVAL INPUTS {match_dict}')

    for fieldname in lstIndpFields:
        # fieldname = item.split(':')[0]
        # alias = item.split(':')[1]

        for evalfieldname, evalfieldvalue in dict(match_dict).items():
            if (str(evalfieldname) == str(fieldname)):

                if fieldname in df.columns:
                    log.debug(f'EVALUATING MANUAL FIELD NAME {fieldname}')
                    machine_entry_field = f'machine_{fieldname}'
                    # machine_alias_entry_field = f'machine_{alias}'
                    machine_entry_field_diff = f'machine_{fieldname}_diff'
                    # machine_alias_entry_field_diff = f'machine_{alias}_diff'
                    machine_entry_field_simm = f'machine_{fieldname}_simm'
                    # machine_alias_entry_field_simm = f'machine_{alias}_simm'
                    if not machine_entry_field in df.columns:
                        df[machine_entry_field] = ''
                        # df[machine_alias_entry_field] = ''
                        df[machine_entry_field_diff] = int(0)
                        # df[machine_alias_entry_field_diff] = 0
                        df[machine_entry_field_simm] = int(0)
                        # df[machine_alias_entry_field_simm] = 0

                # if not
                if fieldname in df.columns:

                    # seq = difflib.SequenceMatcher()
                    # seq.set_seqs(str(df.at[idx, fieldname]).lower(), str(evalfieldvalue).lower())
                    # simm = int(seq.ratio() * 100)
                    fieldValClean = str(df.at[idx, fieldname]).replace('.0', '')
                    simm = isSimilarValue(str(fieldValClean).lower(), str(evalfieldvalue).lower())
                    df.at[idx, machine_entry_field_simm] = int(simm)

                    if (fieldValClean != str(evalfieldvalue)):
                        diff_value = abs(len(str(df.at[idx, fieldname])) - len(str(evalfieldvalue)))

                        log.debug(
                            f'ADDING MACHINE FIELD NAME {fieldname} WITH DIFF VALUE {diff_value} FieldValue {fieldValClean} EvalValue {evalfieldvalue} and SIM {int(simm)}')
                    if (str(fieldValClean) != str(evalfieldvalue)):
                        df.at[idx, machine_entry_field_diff] = diff_value
                        df.at[idx, machine_entry_field] = fieldValClean
                        # df.at[idx, fieldname] = str(evalfieldvalue)

                    if (isSimilarValue(fieldValClean, evalfieldvalue) and str(fieldValClean) != str(evalfieldvalue)):
                        log.debug(
                            f'{fieldname} : {df.at[idx, fieldname]} IS SIMILAR TO {evalfieldname} : {evalfieldvalue}')
                        df.at[idx, fieldname] = str(evalfieldvalue)

    return df


def isSimilarValue(val1, val2) -> int:
    if (not str(val1)): return 0
    if (not str(val2)): return 0
    seq = difflib.SequenceMatcher()
    seq.set_seqs(str(val1).lower(), str(val2).lower())
    sim_ratio = int(seq.ratio() * 100)
    if (sim_ratio > 61): return 1
    return 0

def isSimilarExact(val1, val2) -> int:
    if (not str(val1)): return 0
    if (not str(val2)): return 0
    seq = difflib.SequenceMatcher()
    seq.set_seqs(str(val1).lower(), str(val2).lower())
    sim_ratio = int(seq.ratio() * 100)
    if (sim_ratio >= 90): return 1
    return 0
    
def isSimilarFuzzy(val1, val2) -> int:
    if (not str(val1)): return 0
    if (not str(val2)): return 0
    seq = difflib.SequenceMatcher()
    seq.set_seqs(str(val1).lower(), str(val2).lower())
    sim_ratio = int(seq.ratio() * 100)
    if (sim_ratio > 66): return 1
    return 0


def isSimilarValueActual(val1, val2) -> int:
    if (not str(val1)): return 0
    if (not str(val2)): return 0
    seq = difflib.SequenceMatcher()
    seq.set_seqs(str(val1).lower(), str(val2).lower())
    sim_ratio = int(seq.ratio() * 100)
    return sim_ratio


def phone_normalization(currphone):
    phone_work = re.sub('[^0-9]', '', currphone)
    # log.debug("Type phone number %s", type(phone_work))
    return phone_work


def normalize_name(row):
    row['FirstName'] = str(row['FirstName']).strip().replace(" ", "").lower()

    row['LastName'] = str(row['LastName']).strip().replace(" ", "").lower()

    row['PrevLastName'] = str(row['PrevLastName']).strip().replace(" ", "").lower()
    return row


def normalize_string(val):
    return str(val).strip().replace(" ", "").lower()


def date_normalization(currdob):
    normalized_date = str(currdob).replace('/', '')
    # log.debug('normalized_date %s', normalized_date)
    return normalized_date


def run_glue_job(job_name, arguments):
    log.debug("inside run_glue_job function for %s", job_name)
    session = boto3.session.Session()
    glue_client = session.client('glue', config=config)
    try:
        job_run_id = glue_client.start_job_run(JobName=job_name, Arguments=arguments)
        return job_run_id
    except ClientError as e:
        raise Exception("boto3 client error in run_glue_job: %s", e.__str__())
    except Exception as e:
        raise Exception("Unexpected error in run_glue_job: %s", e.__str__())


def module_import(jsonObject, timestream=False):
    if timestream:
        TIMESTREAM = "timeStream"
        scriptName = f"{ms_source_name}_{TIMESTREAM}.py"
        module_name = f"etl-plugin-scripts.{TIMESTREAM}"
    else:
        scriptName = jsonObject[ms_source_name]['module']['index']
        module_name = "etl-plugin-scripts.indexBase"

    scriptfile_name = os.path.splitext(scriptName)
    log.info('ETL:: scriptname is %s', scriptfile_name)
    scriptfilename = scriptfile_name[0]
    module_name = f"{module_name}.{scriptfilename}"
    log.info('modulename is %s', module_name)
    module = importlib.import_module(module_name, package=None)
    return module


def crawler_job_status(crawler_job, crawler_job_id):
    log.debug("Inside crawler Job Status")

    try:
        response = glue_client.get_job_run(
            JobName=crawler_job,
            RunId=crawler_job_id
        )

        crawler_job_run_state = response['JobRun']['JobRunState']
        log.debug('Crawler job Status : %s', crawler_job_run_state)
        if crawler_job_run_state == 'SUCCEEDED' or crawler_job_run_state == 'FAILED':
            return crawler_job_run_state
        else:
            time.sleep(5)  # retry after every 5 seconds
            return crawler_job_status(crawler_job, crawler_job_id)
    except glue_client.exceptions.EntityNotFoundException as not_found:
        log.debug('MS:: Exception is %s', not_found)
    except glue_client.exceptions.OperationTimeoutException as not_found:
        log.debug('MS:: Exception is %s', not_found)


def error_code(row, vp_fields, master_data):
    error = ''
    for i in range(0, len(vp_fields) - 3):
        log.debug("Checking value for %s = %s in master data", [vp_fields[i]['normalized_ocr_field']],
                  row[vp_fields[i]['normalized_ocr_field']])
        if row[vp_fields[i]['normalized_ocr_field']] != master_data[vp_fields[i]['normalized_index_field']]:
            error += str(vp_fields[i]['ocr_field']) + ', '  # Something that we are showing to the user
    return error[0: len(error) - 2]


def index_search(dirname, search_fields, search_query, lstResults, rscoreb, ix):
    #ix = index.open_dir(dirname, indexname="customer-master-index")
    schema = ix.schema


    # Actual searcher, log.debugs top 10 hits, Currently limiting to 1
    # results = []
    #with ix.searcher(weighting=scoring.TF_IDF()) as s:
        # Create query parser that looks through designated fields in index
    s = ix
    og = qparser.OrGroup.factory(0.9)
    #mp = qparser.MultifieldParser(search_fields, schema, group=og)
    mp = QueryParser('document', schema, group=og)
    mp.remove_plugin_class(qparser.WildcardPlugin)
    mp.add_plugin(PrefixPlugin())
    mp.default_boost = 3.0
    #mp.add_plugin(FuzzyTermPlugin())
    
    lst_str = []
    parseLst = []
    required_q = ''
    q1Lst2 = []
    max_count = 2
    int_itr = 0
    for token in search_query.split(' '):
        if(not token): 
            int_itr += 1
            continue
        if(int_itr < max_count):
            tokenclean = str(token).replace('.','')
            if(int_itr == 1 and len(tokenclean) > 1 and not tokenclean[0].isdigit() ):
                #lst_str.append(f'{token}~2/2')
                query = FuzzyTerm("document", f'{tokenclean}', boost=1.0, maxdist=1, prefixlength=1, constantscore=True)
                required_q = query
                #parseLst.append(mp.parse(f'{token}~2/2'))
                lst_str.append(f'{tokenclean}*^2.0')
                #parseLst.append(f'{token}*^2.0')
            elif(len(tokenclean) > 1 and not tokenclean[0].isdigit()): 
                lst_str.append(f'{tokenclean}*^2.0')
        else:
            lst_str.append(f'{token}')
            #parseLst.append(mp.parse(f'{token}'))
        int_itr += 1
        
    new_search_string = ' '.join(lst_str)
    
    q = mp.parse(new_search_string)
    q_original = mp.parse(search_query)
    
    log.debug(f' THE NEW SEARCHSTRING IS {new_search_string} AND required q {required_q}')
    
    if(required_q):
        q_parse  = q
        #q_parse = SpanNear(q, required_q, slop=2, ordered=False)
    else: q_parse = q_original
    # This is the user query

    #results = s.search(q, limit=50)
    try:
        results = s.search(q_parse, limit=210)
    except Exception as e:
        log.debug(f' CAUGHT A MATCH EXCEPTION {e}')
        q_parse = q_original
        results = s.search(q_parse, limit=210)
        
    log.debug('-----------------------results-------------------------------- %s', results)
    for r in results:
        rscoreb.append(r.score)
        log.debug('score %s', r.score)
    lstResults = results[0:210]
    log.debug('ETL: %s', lstResults[0:210])
    return lstResults[0:210]


def query_merge_results(row, df_prep_store_index, search_index_store, vp_fields, ix):
    searchstring = ""
    for i in range(0, len(vp_fields)):
        if vp_fields[i]['isSearchable'] == "True":
            searchstring += row[vp_fields[i]['normalized_ocr_field']] + " "

    searchstring = str(searchstring.strip()).replace("nan", " ")
    log.debug('searchstring : %s', searchstring)

    lResults = []
    rscoreb = []
    lstResults2 = index_search(search_index_store, ['document'], searchstring, lResults, rscoreb, ix)

    row['ClientErrorCheck'] = False
    base_error_code = mf_data[ms_source_name]['customers'][0]['BaseError'][0]['ErrorCode']
    error = ''
    if len(lstResults2) > 0 and len(rscoreb) > 0:
        # case1 : No fields mismatched
        idx = 0
        for res in lstResults2:
            if float(rscoreb[idx]) > 50.0:
                result = lstResults2[idx]['document'].split(' ')
                master_data = dict()
                log.debug('Result %s,', result)
                if len(result) == len(vp_fields):
                    for i in range(0, len(vp_fields)):
                        master_data[vp_fields[i]['normalized_index_field']] = result[i]
                        row[vp_fields[i]['ocr_field']] = result[i]
                    log.debug('master_date %s', master_data)
                    error = error_code(row, vp_fields, master_data)
    
                    log.debug('Error %s', error.strip())
                    row['Error'] = error.strip()
                    row['ConfidenceScore'] = 100
                    row['MatchFound'] = 'Yes'
                else:
                    row['Error'] = 'Column mismatched'
                    row['ClientErrorCheck'] = True
                    row['ConfidenceScore'] = 0
                    row['MatchFound'] = 'No'
                    row['PatientID'] = ''
                    

                return row

            # Case 2: when one or more than one fields mismatched
            elif float(rscoreb[idx]) < 50.0:
                result = lstResults2[idx]['document'].split(' ')
                master_data = dict()
                log.debug('The result is: %s ', result)
                if len(result) == len(vp_fields):
                    for i in range(0, len(vp_fields)):
                        log.debug('i value is: %s ', i)
                        log.debug('%s ', vp_fields[i]['normalized_index_field'])
                        master_data[vp_fields[i]['normalized_index_field']] = result[i]
                    log.debug('master_date %s', master_data)
    
                    confidencescore = int((float(rscoreb[idx]) / 5.5) * 100)
                    if(confidencescore > 100): confidencescore = 100
                    log.debug(' RUNNING MODEL V2 EVALUATION')
                    matchresponse = match_with_v2(row, confidencescore, master_data)
                    isV2Match = bool(matchresponse['ismatch'])
                    
                    is_true_nomatch = (float(matchresponse['matchscore'])<float(0.57))
                    if is_true_nomatch:
                        confidencescore = 99
                        log.debug('TRUE NO MATCH FOUND')
                        # row['ClientErrorCheck'] = True
                        # row['Error'] = error.strip()
                        # row['ConfidenceScore'] = 99
                        # row['MatchFound'] = 'Yes'
                        # row['v2Match'] = 'No'
                        
                    if (isV2Match):
                        log.debug('V2 MATCH FOUND')
                        row['ClientErrorCheck'] = False
                        row['Error'] = error.strip()
                        row['ConfidenceScore'] = 100
                        row['MatchFound'] = 'Yes'
                        row['v2Match'] = 'Yes'
                        for i in range(0, len(vp_fields)):
                            row[vp_fields[i]['ocr_field']] = master_data[vp_fields[i]['normalized_index_field']]
                        return row
                
                    if(isSimilarExact(row['NORMALIZED_PHONE'],master_data['NORMALIZED_PHONE']) and isSimilarExact(row['FirstName'],master_data['FirstName'])):
                        
                        log.debug('PHONE MATCH FOUND')
                        row['ClientErrorCheck'] = True
                        row['Error'] = error.strip()
                        row['ConfidenceScore'] = confidencescore
                        row['MatchFound'] = 'Yes'
                        row['v2Match'] = 'No'      
                        for i in range(0, len(vp_fields)):
                            row[vp_fields[i]['ocr_field']] = master_data[vp_fields[i]['normalized_index_field']]
                        return row
                    # PLN doesn't exist.
                    if row['PrevLastName'] == 'nan' and master_data['PrevLastName'] == 'nan':
                        if (
                                (row['FirstName'] == master_data['FirstName'] and row['LastName'] == master_data[
                                    'LastName']) and
                                (row['NORMALIZED_DOB'] == master_data['NORMALIZED_DOB'] or row['NORMALIZED_PHONE'] ==
                                 master_data[
                                     'NORMALIZED_PHONE'])
                        ):
                            row['ClientErrorCheck'] = True
                            row['ConfidenceScore'] = confidencescore
                            row['Error'] = error.strip()
                            row['MatchFound'] = 'Yes'
    
                            for i in range(0, len(vp_fields)):
                                row[vp_fields[i]['ocr_field']] = master_data[vp_fields[i]['normalized_index_field']]
    
                            return row
    
                        else:
                            row['ClientErrorCheck'] = True
                            row['ConfidenceScore'] = confidencescore
                            row['Error'] = error.strip()
                            row['MatchFound'] = 'No'
                            row['PatientID'] = ''
                            # for i in range(0, len(vp_fields)):
                            #     row[vp_fields[i]['ocr_field']] = ''
    
    
    
                    elif (
                            (row['FirstName'] == master_data['FirstName']) and
                            (row['LastName'] == master_data['LastName'] or row['PrevLastName'] == master_data[
                                'PrevLastName']) and
                            (row['NORMALIZED_DOB'] == master_data['NORMALIZED_DOB'] or row['NORMALIZED_PHONE'] ==
                             master_data[
                                 'NORMALIZED_PHONE'])
                    ):
                        row['ClientErrorCheck'] = True
                        row['ConfidenceScore'] = confidencescore
                        row['Error'] = error.strip()
                        row['MatchFound'] = 'Yes'
                        for i in range(0, len(vp_fields)):
                            row[vp_fields[i]['ocr_field']] = master_data[vp_fields[i]['normalized_index_field']]
    
                        return row
    
                    else:
                        row['ClientErrorCheck'] = True
                        row['ConfidenceScore'] = int((float(rscoreb[idx]) / 13.5) * 100)
                        row['MatchFound'] = 'No'
                        row['PatientID'] = ''
                        # for i in range(0, len(vp_fields)):
                        #     row[vp_fields[i]['ocr_field']] = ''
    
                    error = error_code(row, vp_fields, master_data)
                    log.debug('Error %s', error)
                    row['Error'] = str(base_error_code) + ': ' + str(error.strip())
                else:
                    row['Error'] = 'Column mismatched'
                    row['ClientErrorCheck'] = True
                    row['ConfidenceScore'] = 0
                    row['MatchFound'] = 'No'
                    row['PatientID'] = ''
                    return row
                idx += 1
    # case3: No hit to mater data
    elif len(lstResults2) == 0:
        for i in range(0, len(vp_fields) - 1):
            error += str(vp_fields[i]['ocr_field']) + ', '
        error = error[0: len(error) - 2]
        row['Error'] = str(base_error_code) + ': ' + error
        row['ClientErrorCheck'] = True
        row['ConfidenceScore'] = 0
        row['MatchFound'] = 'No'
        row['PatientID'] = ''
        # for i in range(0, len(vp_fields)):
        #     row[vp_fields[i]['ocr_field']] = ''

    return row


def clean_record(row):
    # FirstName cleanup
    row['FirstName'] = str(row['FirstName']).replace('#', '').replace(':', '').replace('>', '').replace('<',
                                                                                                        '').replace(
        '$', '') \
        .replace('\"', '').replace('\"', '').replace('^', '').replace('=', '').replace('(', '').replace(')', '') \
        .lower().replace('types', '').replace('noclub', '').replace('club', '').strip().replace(" ", "").lower()

    # LastName cleanup
    row['LastName'] = str(row['LastName']).replace('#', '').replace(':', '').replace('>', '').replace('<',
                                                                                                      '').replace('$',
                                                                                                                  '') \
        .replace('\"', '').replace('\"', '').replace('^', '').replace('=', '').replace('(', '').replace(')', '') \
        .lower().replace('types', '').replace('noclub', '').replace('club', '').strip().replace(" ", "").lower()
    
    row['PrevLastName'] = ' '
    #row['PrevLastName'] = str(row['PrevLastName']).replace('#', '').replace(':', '').replace('>',
    #                                                                                        '').replace(
    #    '<', '').replace('$', '') \
    #    .replace('\"', '').replace('\"', '').replace('^', '').replace('=', '').replace('(', '').replace(')', '') \
     #   .lower().replace('types', '').replace('noclub', '').replace('club', '').strip().replace(" ", "").lower()

    # phone cleanup
    row['NORMALIZED_PHONE'] = phone_normalization(str(row['Phone1']))

    # date cleanup
    try:
        row['NORMALIZED_DOB'] = pd.to_datetime(row['DOB'])
        row['NORMALIZED_DOB'] = row['NORMALIZED_DOB'].strftime('%m/%d/%y')
        row['NORMALIZED_DOB'] = date_normalization(row['NORMALIZED_DOB'])
    except:
        row['NORMALIZED_DOB'] = date_normalization(row['DOB'])

    return row


def runbox_processing_calculate_and_send(ocr_csp_path, df_prep_store_idx, customerid, i_search_index, vp_fields, separator=","):
    ### LOAD THE RESULTS
    df2_rekog_data_calc = pd.read_csv(ocr_csp_path, dtype='str', sep=separator)
    log.debug('The df2_rekog_data_calc read from s3 in pandas %s', df2_rekog_data_calc.head(5))

    ### CLEAN UP RESULTS
    df2_rekog_data_calc = df2_rekog_data_calc.apply(clean_record, axis=1)
    log.debug('The df2_rekog_data_calc after cleanup %s', df2_rekog_data_calc.head(5))
    
    #og = qparser.OrGroup.factory(0.4)
    m_setup = index.open_dir(i_search_index, indexname="customer-master-index")
    ix = m_setup.searcher(weighting=scoring.TF_IDF())
    df2_rekog_data_calc = df2_rekog_data_calc.apply(
        lambda x: query_merge_results(x, df_prep_store_idx, i_search_index, vp_fields, ix), axis=1)
    log.debug('The df2_rekog_data_calc after query merge %s', df2_rekog_data_calc.head(10))
    df2_rekog_data_calc = df2_rekog_data_calc.reset_index(drop=True)
    log.debug('The df2_rekog_data_calc after reset index %s', df2_rekog_data_calc.head(10))

    df_results = df2_rekog_data_calc.drop(['NORMALIZED_DOB', 'NORMALIZED_PHONE'], axis=1)

    ## SEND RESULTS
    log.debug(f"SAVING THE RESULTS FOR {ocr_csp_path}")
    index_file_key = 'index/processed/' + customerid + '/' + inputfile_name[0] + '_index.csv'
    index_result_path = 's3://' + outputBucket + '/' + index_file_key
    df_results.to_csv(index_result_path, sep=separator, index=False)
    log.debug("------- DataFrame AI ML------ %s", df_results.head(5))
    return True

    # # Calling Create Crawler JOB
    # crawler_args = {'--source': ms_source_name, '--preprocess_filepath': index_file_key,
    #                 '--etlProcessDB': 'etl_index_db'}
    # crawler_job_id = run_glue_job(crawlerjob, crawler_args)
    # log.debug('crawler job run id %s', crawler_job_id['JobRunId'])

    # response = crawler_job_status(crawlerjob, crawler_job_id['JobRunId'])

    # log.debug('Response: %s', response)
    # if response == 'SUCCEEDED':
    #     return True
    # else:
    #     log.debug("Crawler Job Need to be Investigated")
    #     return True


def add_stores(i, dataframe, writer, schema, vp_fields):
    # log.debug('Inside add stores...')
    schema_field = schema.names()
    document_fields = ''
    # document_fields = str(dataframe.loc[i, 'NORMALIZED_NAME']) + ' ' + str(dataframe.loc[i, 'NORMALIZED_PHONE'])
    for j in vp_fields:
        document_fields += str(dataframe.loc[i, j['normalized_index_field']]).lower() + ' '

    # log.debug('document_fields %s', document_fields.strip())
    writer.update_document(document=document_fields.strip())


# def add_stores(i, dataframe, writer, schema, vp_fields):
#     log.debug('Inside add stores...')
#     schema_field = schema.names()
#     document_fields = dataframe.loc[i, [field['normalized_index_field'] for field in vp_fields]].apply(lambda x: str(x).lower() + ' ')
#     document_fields = document_fields.str.cat()

#     log.debug('document_fields %s', document_fields.strip())
#     writer.update_document(document=document_fields.strip())


# create and populate index
def populate_index(dirname, dataframe, schema, vp_fields):
    # Checks for existing index path and creates one if not present
    log.debug('Inside populate index')
    if not os.path.exists(dirname):
        os.mkdir(dirname)  # create directory to store indexing
    log.debug("Creating the Index")

    ix = index.create_in(dirname, schema, indexname="customer-master-index")
    log.debug('Index got created: %s', ix.indexname)
    log.debug('Index got storage: %s', ix.storage)
    log.debug('Schema of the index is: %s', ix.schema)
    with ix.writer(limitmb=1000) as writer:
        # Imports stories from pandas df
        log.debug("Populating the Index")
        for i in dataframe.index:
            add_stores(i, dataframe, writer, schema, vp_fields)

def clean_df(df):

    df_prep_store_index = df.drop_duplicates()
    df_prep_store_index = df_prep_store_index.fillna('')
    df_prep_store_index['Email'] = ''
    log.debug('master index content: %s', df_prep_store_index.head(15))

    df_prep_store_index['NORMALIZED_DOB'] = pd.to_datetime(df_prep_store_index.DOB)
    log.debug('The formatted date frame %s', df_prep_store_index.head(5))
    df_prep_store_index['NORMALIZED_DOB'] = df_prep_store_index['NORMALIZED_DOB'].dt.strftime(
        '%m/%d/%y')

    log.debug('I am here... 1')
    df_prep_store_index['NORMALIZED_DOB'] = df_prep_store_index['NORMALIZED_DOB'].apply(
        lambda x: date_normalization(x))

    # log.debug('Normalize Name')
    # df_prep_store_index = df_prep_store_index.apply(normalize_name, axis=1)

    log.debug('Normalize Name')
    # normalize_string
    df_prep_store_index['FirstName'] = df_prep_store_index['FirstName'].apply(
        lambda x: normalize_string(x))

    log.debug('Normalize LastName')
    df_prep_store_index['LastName'] = df_prep_store_index['LastName'].apply(
        lambda x: normalize_string(x))

    log.debug('Normalize PrevLastName')
    df_prep_store_index['PrevLastName'] = df_prep_store_index['PrevLastName'].apply(
        lambda x: normalize_string(x))

    log.debug('Normalize phone number')
    df_prep_store_index['NORMALIZED_PHONE'] = df_prep_store_index['Phone1'].apply(
        lambda x: phone_normalization(str(x)))

    # df_prep_store_index = df_prep_store_index.apply(documents, axis=1)
    log.debug('The normalized dataframe is: %s', df_prep_store_index.head(5))

    log.debug('I am here... 2')
    df_store_index = df_prep_store_index

    return df_store_index

def get_since_date(outputBucketName,file_path):
    s3 = boto3.client('s3')
    #outputBucketName='prod-csp-s3-etl-adhoc'
    #file_path = "model-repository/auto-index/base/Model_v1_update_time/blank.csv"
    log.info("outputBucketName %s, file_path %s",outputBucketName,file_path)
    response = s3.head_object(Bucket=outputBucketName, Key=file_path)
    SinceDate = response['LastModified']
    log.info('since date is %s',SinceDate)
    return SinceDate

def update_index(i_search_index,schema,vp_fields,mongo_bucket,version):
    try:
        s3_client = boto3.client('s3')
        now_datetime = datetime.datetime.utcnow()
        a_str = now_datetime.strftime('%Y-%m-%d')
        file_key=f"{source}/delta-index/{a_str}/masterindex.csv"
        log.info("mongo_bucket %s",mongo_bucket)
        log.info("filekey %s",file_key)
        s3_client.head_object(Bucket=mongo_bucket, Key=file_key)
        try:
            file_path = "model-repository/auto-index/base/Model_v1_update_time/blank.csv"
            SinceDate=get_since_date(mongo_bucket,file_path)
            log.info("sincedate is %s",SinceDate)
            output_sincedate=SinceDate.date()
            
            log.info('a_str %s',a_str)
            log.info(' output_sincedate %s',output_sincedate)
            #output_sincedate = SinceDate.split()[0]
            #output_sincedate=SinceDate[:10]
            if output_sincedate < datetime.datetime.strptime(a_str, '%Y-%m-%d').date():
                log.info("Inside if updating v1.")
                # If the file exists, read its contents into a DataFrame
                delta_path = f's3://{mongo_bucket}/{source}/delta-index/{a_str}/masterindex.csv'
                dataframe = pd.read_csv(delta_path)
                ###################### clean df #########################
                dataframe=clean_df(dataframe)
                ix = index.open_dir(i_search_index, indexname="customer-master-index")
                log.info("ix done ")
                with ix.writer(limitmb=1000) as writer:
                # Imports stories from pandas df
                    log.debug("updating the Index")
                    for i in dataframe.index:
                        add_stores(i, dataframe, writer, schema, vp_fields)
                log.info("out from write index")
                #for path, dirs, files in os.walk('./tmp/model-repository/auto-index/base/v1'):
                for path, dirs, files in os.walk(i_search_index):
                    log.info("inside customer-master-index")
                    client = boto3.client('s3')
                    for file in files:
                        log.debug('The file is: %s', file)
                        file_s3 = "model-repository/auto-index/base/" + version + "/" + file
                        file_local = os.path.join(path, file)
                        log.debug("Upload:%s to target: %s", file_local, file_s3)
                        client.upload_file(file_local, outputBucket, file_s3)
                #Blank csv to hold Model_v1_update_time        
                
                #file_path
                s3_client.put_object(Bucket=mongo_bucket,Key=file_path,Body="Blank csv to hold Model_v1_update_time")
                log.info("blank csv created to hold Model_v1_update_time")
            else:
                log.info("V1 already updated")
        except Exception as e:
                    log.info(f"Error occurred: {e}")
                    #log.info("No new data to update to existing index")
    except Exception as e:
        log.info(f"Error occurred: {e}")
        log.info("No new data to update to existing index")

def populate_master_index_s3(schema, vp_fields, df_megamaster,mongo_bucket,transaction_id, version=None):
    log.info('Inside populate model repository')
    for store_number in distinct_store:
        client = boto3.client('s3')
        # file_s3 = "model-repository/auto-index/" + customerid + "/" + str(store_number) + "/" + version + "/"
        file_s3 = "model-repository/auto-index/base/" + version + "/"
        result = client.list_objects(Bucket=outputBucket, Prefix=file_s3)
        log.info('The list result for s3 object list: %s', result)
        s3_model_index_v1_path = "model-repository/auto-index/base/" + version + "/"
        # s3_model_index_v1_path = "model-repository/auto-index/" + customerid + "/" + str(
        #            store_number) + "/" + version + "/" + file
        # s3_model_index_v1_path = "model-repository/auto-index/" + customerid + "/" + version + "/"
        model_index_v1_path = "./tmp/model-repository/auto-index/base/" + version + "/"
        if 'Contents' in result:
            # return True
            # if not os.path.exists('../tmp/' + str(store_number) + "_index" + '/'):
            # model_index_v1_path = "./tmp/model-repository/auto-index/base/v2/"

            if not os.path.exists(model_index_v1_path):
                s3_image_dir = f"s3://{outputBucket}/{s3_model_index_v1_path}"
                s3downloadprefix = os.path.dirname(s3_image_dir)
                download_froms3(s3downloadprefix, './tmp', '')
            log.info("going to update_index")
            update_index(model_index_v1_path,schema,vp_fields,outputBucket,version)
        else:

            df_prep_store_index = df_megamaster
            log.debug('The normalized dataframe is: %s', df_prep_store_index.head(5))
            populate_index("Store_index", df_prep_store_index, schema, vp_fields)
            # populate_index(str(store_number) + "_index", df_prep_store_index, schema, vp_fields)
            log.info('The list of dire from root %s', os.listdir('.'))
            log.info('The list of dir from stor %s', os.listdir('../tmp/' + "Store_index"))
            # log.debug('The list of dir from stor %s', os.listdir('../tmp/' + str(store_number) + "_index"))
            # log.debug('The file is: %s', file_s3)
            # file_s3 = "model-repository/auto-index/" + customerid + "/" + str(
            #            store_number) + "/" + version + "/" + file
            for path, dirs, files in os.walk('../tmp/' + "Store_index"):
                for file in files:
                    # file_s3 = os.path.normpath(path + '/' + file)
                    # s3://bucket/model_repo/auto-index/accountNo/store_no/v1/*files
                    # s3://bucket/model_repo/accountNo/store_no/v2/*files
                    log.debug('The file is: %s', file)
                    file_s3 = "model-repository/auto-index/base/" + version + "/" + file
                    file_local = os.path.join(path, file)
                    log.debug("Upload:%s to target: %s", file_local, file_s3)
                    client.upload_file(file_local, outputBucket, file_s3)


if __name__ == '__main__':
    log.info("The execution starts here")
    try:
        if ms_source_name in mf_data:
            log.info('source name exist in manifest json')
            """
            if ms_source_name == "nvi":
                x = trigger_glue_job(file_key, notification_job, inputBucket, transaction_id, 2, 'Failed')
                log.debug('ETL:: Trigger Glue Job Exception Result: %s', x)
                exit("Failing Index Job as V1 model is generating")
            """
            
            if 'index' in mf_data[ms_source_name]['module'].keys():
                
                send_to_timestream_events(transaction_id, manifestBucketName, domain_model_path, inputBucket,
                                  file_key, spark, state="Ready", PipelineModule="AI/ML",
                                  measure_name="InAutoIndexingWF")
                log.debug('index module exist in %s', ms_source_name)
                customerid = re.findall('{}(.+?)input.*'.format(ms_source_name), inputfile_name[0])[0]
                separator = mf_data[ms_source_name]["csv-seperator"]["etl_index_db"]
                if customerid in mf_data[ms_source_name]["customers"][0]["customerId"]:
                    
                    log.debug('customerid exist in manifest')
                    incoming_csv_filepath = 's3://' + inputBucket + '/' + file_key
                    incoming_csv_df = pd.read_csv(incoming_csv_filepath)
                    # incoming_csv_df=createPatientRecord(incoming_csv_df)
                    distinct_store = incoming_csv_df['Store'].unique()
                    log.debug('Distinct store number: %s', distinct_store)

                    # create empty dataframe to merge all dataframe
                    pd.set_option('display.max_columns', None)
                    mega_folder_name = "mega-MasterIndex"
                    nvi_master_index_bucket_path = "s3://" + outputBucket + "/" + ms_source_name + "/master-index/" + mega_folder_name + "/processed/MasterIndex.csv"
                    log.info(f"master in dex path is {nvi_master_index_bucket_path}")
                    df_store_index = pd.read_csv(nvi_master_index_bucket_path)

                    # Drop duplicates from master_index_df
                    pd.set_option('display.max_columns', None)

                    #apply data cleansing to master data frame
                    ############################## Normalization on Master ###############################
                    # df_store_index = clean_df(master_index_df)

                    vp_keys = mf_data[ms_source_name]['customers'][0]['validation_profile']
                    log.debug("Validation profile keys: %s", vp_keys)

                    for i in range(0, len(vp_keys)):
                        vp_fields = vp_keys[i]['fields']
                        schema = Schema()
                        for j in range(0, len(vp_fields)):
                            if vp_fields[j]['DataType'] == 'TEXT':
                                schema.add(vp_fields[j]['index_field'],
                                           TEXT(stored=True, chars=True, spelling=True, phrase=True))
                            else:
                                schema.add(vp_fields[j]['index_field'], ID(stored=True, unique=False))

                        log.debug("Schema field in for loop %s", schema.names)
                        schema.add('document', TEXT(stored=True, chars=True, spelling=True, phrase=True))
                        # populate_index("Store_Index", df_store_index, schema, vp_fields)
                        ############################## master index normalization ######################
                        populate_master_index_s3(schema, vp_fields, df_store_index,mongo_bucket,transaction_id, "v1")
                        # populate_master_index_s3(schema, vp_fields, "v1")
                        
                        
                        log.debug('Populate index is complete:%s', df_store_index.head(5))
                        log.debug('customerid %s', customerid)

                        ocr_file_key = 'ocr/processed/' + customerid + '/' + inputfile_name[0] + '_ocr.csv'
                        ocr_csv = "s3://" + outputBucket + "/" + ocr_file_key
                        runbox_processing_calculate_and_send(ocr_csv, df_store_index, customerid, './tmp/model-repository/auto-index/base/v1/',
                                                             vp_fields, separator)
                    
                    send_to_timestream_events(transaction_id, manifestBucketName, domain_model_path, inputBucket,
                                  file_key, spark, state="Complete", PipelineModule="AI/ML",
                                  measure_name="InAutoIndexingWF")
                else:
                    log.debug('CustomerId does not exist in manifest %s', customerid)

            else:
                log.debug('index module does not exist in manifest %s', ms_source_name)
                log.debug('Skipping index module')
        else:
            log.debug('%s source name does not exist in manifest', ms_source_name)
            log.debug('Skipping index function')
        # x = trigger_glue_job(file_key, notification_job, inputBucket, transaction_id, 0, 'Success')
        # log.info('ETL:: Trigger Glue Job Result: %s', x)
    except Exception as e:
        send_to_timestream_events(transaction_id, manifestBucketName, domain_model_path, inputBucket,
                                  file_key, spark, state="Failed", PipelineModule="AI/ML",
                                  measure_name="InAutoIndexingWF")
        log.error('Pre-Process:: Generic Exception is %s', e.__cause__)
        log.error('ETL: %s', traceback.print_exception(type(e), e, e.__traceback__))
        # x = trigger_glue_job(file_key, notification_job, inputBucket, transaction_id, 2, 'Failed')
        # log.debug('ETL:: Trigger Glue Job Exception Result: %s', x)
        exit("Index Job failed")

job.commit()