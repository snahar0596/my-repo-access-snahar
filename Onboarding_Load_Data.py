import importlib
import logging
from typing import List, Dict
import os
from operator import truediv
import sys
import ast
import requests
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from io import StringIO
from pyspark.sql.functions import when, col, upper, lit, split, from_json, current_timestamp, date_format, create_map, udf, expr, regexp_extract, substring, element_at, concat_ws, collect_list, first
from functools import reduce
import re
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, IntegerType, DateType
from itertools import chain
from pyspark.sql import Row
import pandas as pd
import numpy as np
import boto3
import json
import base64
import traceback
import requests
from botocore.exceptions import ClientError
from datetime import datetime, date, timedelta
import time
from botocore.config import Config
import pymongo
from bson import ObjectId
import Onboarding_Log_Manager
import Onboarding_Timestream_Manager
from Onboarding_Configuration_Template import ConfigurationTemplate, S3Location, GlueCatalog, Transaction, ConfigurationNotFound, CollectionNotFound, GlueCatalogException

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 200)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

pipeline_mod = 'LoadData'
log = Onboarding_Log_Manager.get_module_logger(pipeline_mod)
log.info('OnboardingLoadData module')

step_name = "Load_Data"
execution_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
next_step = "There is no next step"

log.info(f"""
Step Name: {step_name}
Execution Date: {execution_date}
Next Step: {next_step}
""")

df = pd.DataFrame({
    'Step': [step_name],
    'Timestamp': [execution_date],
    'Next': [next_step]
})

log.info("Sample DataFrame:\n"
         f"{df}")

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

secret_manager = importlib.import_module("etl-plugin-scripts.helper.ssm_services")
refresh_oauth = importlib.import_module("etl-plugin-scripts.helper.api_auth_token")


# Keeping global variable
source, user, password, db_name, authdatabase, hosts = (None, None, None, None, None, [])
manifest_json_data = dict()
all_columns_with_types = []
no_of_core_partition = 1  # number of partitions to use
s3_client = boto3.client('s3')
base_api = ""
ms_oauth_token = ""
tr_mask_columns_list = []
unify_mask_columns_list = []

type_mapping = {
    'string': StringType(),
    'date': StringType(),
    'integer': IntegerType()
}


def create_connection(user, password, database, authdatabase, hosts):
    mongo_url_python = "mongodb://{User}:{Password}@{host}/{db}"
    _client = None
    error_message = "ETL Unable to connect to MongoDB"
    for host in hosts:
        try:
            _client = pymongo.MongoClient(mongo_url_python.format(host=host, User=user, Password=password, db=database),
                                          tls=True, authSource=authdatabase, authMechanism='SCRAM-SHA-1')
            _client.server_info()
            log.info(f"ETL MongoDB connection is successful.")
            # If the connection is successful, do not continue the loop.
            return _client
        except Exception as e:
            error_message = f"ETL The Exception has occurred while connecting to mongo {e.__cause__}"
            log.error(error_message)

    # Raise exception if we're not able to connect mongo with all the available hosts
    if not _client:
        raise Exception(e)


def get_one_item(database: str, collection: str, search_pair: dict):
    client = create_connection(user, password, db_name, authdatabase, hosts)
    log.info(f"db is {database}")
    db = client[database]
    log.info(f"collection is {collection}")
    collection_obj = db[collection]  # Customer is the name of the collection in MongoDB
    log.info(f"collection is {collection_obj}")
    result = collection_obj.find_one(search_pair)  # Fetching record from mongo with specific cust id
    # log.info(f"result is {result}")
    # Close the connection
    client.close()
    return result


def find_item(database: str, collection: str, projection: Dict, extra_args: Dict):
    log.info(f"inside find_item")
    client = create_connection(extra_args["user"], extra_args["password"], extra_args["db_name"],
                               extra_args["authdatabase"], extra_args["hosts"])
    log.info(f"db is {database}")
    db = client[database]
    log.info(f"collection is {collection}")
    collection_obj = db[collection]  # Customer is the name of the collection in MongoDB
    log.info(f"collection is {collection_obj}")
    result = collection_obj.find(projection)  # Fetching record from mongo with specific cust id
    # log.info(f"result is {result}")
    return result.to_list()
    # Close the connection
    client.close()


def get_mongo_details_from_secrets(secrets: Dict):
    server, user, password, replicaset, dnsserver, rtk, authdatabase, db_name = ('', '', '', '', '', '', '', '')
    if secrets["Server"] and secrets["Server"] != "":
        server = secrets["Server"]
    if secrets["User"] and secrets["User"] != "":
        user = secrets["User"]
    if secrets["Password"] and secrets["Password"] != "":
        password = secrets["Password"]
    if secrets["ReplicaSet"] and secrets["ReplicaSet"] != "":
        replicaset = secrets["ReplicaSet"]
    if secrets["DNSServer"] and secrets["DNSServer"] != "":
        dnsserver = secrets["DNSServer"]
    if secrets["RTK"] and secrets["RTK"] != "":
        rtk = secrets["RTK"]
    if secrets["AuthDatabase"] and secrets["AuthDatabase"] != "":
        authdatabase = secrets["AuthDatabase"]
    if secrets["dbname"] and secrets["dbname"] != "":
        db_name = secrets["dbname"]
    hosts = server.split(",")
    log.info('ETL:: The length of server list is: %s', len(hosts))

    return server, user, password, replicaset, dnsserver, rtk, authdatabase, db_name, hosts


def get_tr_details_from_secrets(secrets):
    grant_type, username, tr_password = ('', '', '')
    if secrets["grant_type"] and secrets["grant_type"] != "":
        grant_type = secrets["grant_type"]
    if secrets["username"] and secrets["username"] != "":
        username = secrets["username"]
    if secrets["password"] and secrets["password"] != "":
        tr_password = secrets["password"]

    return grant_type, username, tr_password


def get_api_secret(secret_keys):
    client_id = ''
    client_secret = ''
    grant_type = ''
    username = ''
    api_password = ''
    # extract_mongo_secret(secret_keys)
    if secret_keys["client_id"] is not None and secret_keys["client_id"] != "":
        client_id = secret_keys["client_id"]
    if secret_keys["client_secret"] is not None and secret_keys["client_secret"] != "":
        client_secret = secret_keys["client_secret"]
    if secret_keys["grant_type"] is not None and secret_keys["grant_type"] != "":
        grant_type = secret_keys["grant_type"]
    if secret_keys["username"] is not None and secret_keys["username"] != "":
        username = secret_keys["username"]
    if secret_keys["password"] is not None and secret_keys["password"] != "":
        api_password = secret_keys["password"]
    # log.info("ETL:: client_id " + client_id + " client_secret " + client_secret + " grant_type "
    #       + grant_type + " username " + username + " password " + api_password)
    return client_id, client_secret, grant_type, username, api_password


def get_catalog_data(catalog_db, incoming_catalog_table, batch_name, transaction_id):
    """Function to query the AWS Glue Data Catalog, given the 'catalog_db' and 'incoming_catalog_table' to retrieve the needed information to process a.k.a. image paths"""

    try:
        query = f"""
        SELECT
            *
        FROM
            `{catalog_db}`.`{incoming_catalog_table}` AS _incoming_table
        WHERE
            _incoming_table.batch_name = '{batch_name}' and
            _incoming_table.transaction_id = '{transaction_id}'
        """

        log.info(f"The query to be executed is:\n\n{query}\n\n")
        query_result = spark.sql(query)

        log.info("Success! Data was read from Glue Catalog.")
        return query_result

    except Exception as e:
        log.info(f"Critical error in the 'get_image_paths_from_catalog' function querying the table '{catalog_db}.{incoming_catalog_table}':\n{str(e)}\n")
        log.info(f"Error type: {type(e).__name__}")
        exit("OCR - Critical error")
        # Timestream call
        # raise RuntimeError("Failed to read from Glue Catalog.") from e


def explode_key_value_pairs(df, json_schema, field_list):
    expanded_columns = [
        from_json(col("key_value_pairs"), json_schema).getField(field.name).alias(field.name)
        for field in json_schema.fields
    ]
    df_final = df.select(
        *df.columns,
        *expanded_columns)

    df_final = rename_column_with_label(df_final, field_list)

    return df_final


def get_customer_config(incoming_bucket, cust_name):
    file_key = f'Onb_Customer_Conf/{cust_name}.json'
    response = s3_client.get_object(Bucket=incoming_bucket, Key=file_key)
    json_content = json.loads(response['Body'].read().decode('utf-8'))
    return json_content


def fetch_collection_config(customer_config, project_name):
    for project_dict in customer_config["collections"]:
        if project_dict["collection"] == project_name:
            return project_dict


def create_json_schema_from_field_list(field_list):
    json_schema = [
        StructField(
            field['FieldName'],
            type_mapping.get(field['Type'], StringType()),
            True
        )
        for field in field_list
    ]
    return json_schema


def get_box_guid(filter_box, unify_customer_id):
    ms_item_api_url = 'https://' + base_api + '/v1/items'
    ms_item_api_header = {'Authorization': 'Bearer ' + ms_oauth_token,
                          'customerId': unify_customer_id, 'Content-Type': 'application/json'}
    ms_item_api_params = {'itemCode': filter_box}
    ms_api_session = requests.Session()
    try:
        item_api_response = ms_api_session.get(ms_item_api_url, headers=ms_item_api_header, params=ms_item_api_params)
        log.info('ETL:: item_api_response statuscode  %s', item_api_response.status_code)
        log.info('ETL:: item_api_response message%s', item_api_response.reason)
        if item_api_response.status_code == 200:
            data = item_api_response.text
            log.info("ETL:: data %s", data)
            json_data = json.loads(data)
            box_guid = json_data[0]['id']
            log.info("ETL:: boxGUID is %s", box_guid)
        else:
            log.info("ETL:: Some error occured in getting Item data, API status and response is %s",
                      item_api_response.status_code)
            log.info("ETL:: Some error occured in getting Item data, API status and response is %s",
                      item_api_response.reason)
        return box_guid
    except Exception as e:
        log.error('ETL:: API has an Exception  %s', e.__cause__)
        # x = trigger_glue_job(source,item_csv_path, customer_file_key, transaction_id, notifyjob, 204, 'Failed')
        # log.info('ETL::  %s', x)
        log.error('ETL:: %s', traceback.print_exception(type(e), e, e.__traceback__))
        return False


def rename_column_with_label(df, field_list):
    field_label_mapping = {field['FieldName']: field['FieldLabel'] for field in field_list}
    for field_name, field_label in field_label_mapping.items():
        if field_name in df.columns:
            df = df.withColumnRenamed(field_name, field_label)

    return df


def create_opsiq_df(filter_parameters, transaction_id, batch_name, field_list):
    data = []
    filter_parameters = ast.literal_eval(filter_parameters)
    for parent_key, child_dict in filter_parameters.items():
        row = {
            "file_name_opsiq": parent_key,
            "transaction_id_opsiq": transaction_id,
            "batch_name_opsiq": batch_name
        }
        row.update(child_dict)
        data.append(row)
    log.info("Transformed Data:")
    for row in data:
        log.info(row)

    json_struct = create_json_schema_from_field_list(field_list)
    json_struct.extend(
        [StructField("file_name_opsiq", StringType(), True), StructField("transaction_id_opsiq", StringType(), True),
         StructField("batch_name_opsiq", StringType(), True)])

    log.info(json_struct)
    json_schema = StructType(json_struct)

    opsiq_df = spark.createDataFrame(data, json_schema)

    opsiq_df = rename_column_with_label(opsiq_df, field_list)

    log.info("Dataframe from OpsIq")
    opsiq_df.show(2, truncate=False)
    return opsiq_df


def get_tr_unify_mapping(template_data, content_type):
    entities = template_data['itemHierarchy']['entities']
    field_mapping = []
    for i in range(len(entities)):
        if entities[i]['entity'].lower() == content_type.lower():
            field_mapping = template_data['itemHierarchy']['entities'][i]['fields']
    return field_mapping


def fetch_columns_to_mask(tr_mapping_dict):
    for key, value in tr_mapping_dict.items():
        if value[0:3].lower() == "ref":  # Check if the last character of the value is a digit
            tr_mask_columns_list.append(value)
            unify_mask_columns_list.append(key)
    return  tr_mask_columns_list, unify_mask_columns_list


def mask_pii(data):
    return data[:2] + "***" + data[-2:] if data else "BLANK"


def create_access_token(token_api_url, client_id, client_secret, grant_type, username, api_password):
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": grant_type,
        "audience": "",
        "username": username,
        "password": api_password,
        "realm": ""
    }

    api_res = requests.post(token_api_url, data)
    return api_res.json()['accessToken']


def create_tr_access_token(tr_grant_type, tr_username, tr_password):
    create_access_token_api = f"https://{tr_base_api}/Token"
    headers = {"Content-Type": "application/json"}
    token_payload = {"grant_type": tr_grant_type, "username": tr_username, "password": tr_password}
    log.info(create_access_token_api)
    res = requests.post(create_access_token_api, data=token_payload, headers=headers)
    log.info(res.text)
    if res.status_code == 200:
        tr_access_token = res.json()['access_token']

    return tr_access_token


def get_tr_box_parent_id(tr_customer_id, box_barcode):
    get_parent_id_api = f"https://{tr_base_api}/api/v1.0/Items/{tr_customer_id}/{box_barcode}/GetItemByItemCode"
    headers = {"Authorization": f"Bearer {tr_access_token}", "Content-Type": "application/json"}

    res = requests.get(get_parent_id_api, headers=headers)
    if res.status_code == 200:
        tr_parent_id = res.json()['trItemID']

    return tr_parent_id


def create_tr_work_order(tr_customer_id, api_user_id):
    create_work_order_api = f"https://{tr_base_api}/api/v1.0/WorkOrders/CreateWorkOrderHeader"
    headers = {"Authorization": f"Bearer {tr_access_token}", "Content-Type": "application/json"}
    create_workorder_payload = {
        "CustRef": "",
        "DueDate": str(date.today() + timedelta(days=2)),
        "DepartmentID": 0,
        "Descr": "",
        "Priority": "OTHR",
        "InvoiceComments": "",
        "RequestedBy": "SYSTEM",
        "SiteID": 0,
        "RouteID": 0,
        "CenterID": 0,
        "CustomerID": tr_customer_id,
        "UserID": api_user_id
    }

    res = requests.post(create_work_order_api, json=create_workorder_payload, headers=headers)
    if res.status_code == 200:
        tr_work_order_id = res.json()

    return tr_work_order_id


def create_itemcode(row, tr_customer_id, tr_work_order_id, tr_type_id, tr_storage_code, tr_parent_id):
    create_itemcode_api = f"https://{tr_base_api}/api/v1.0/Items/CreateItem"
    headers = {"Authorization": f"Bearer {tr_access_token}", "Content-Type": "application/json"}
    non_indexed_item_payload = {
    "WorkOrderID": tr_work_order_id,
    "CustomerID": tr_customer_id,
    "TypeID": tr_type_id,
    "StorageCode": tr_storage_code,
    "ParentID": tr_parent_id,
    "InvCategory": "Service"
    }

    try:
        res = requests.post(create_itemcode_api, json=non_indexed_item_payload, headers=headers)
        if res.status_code == 200:
            data = res.json()
            return pd.Series({
                'ItemCode': data.get('ItemCode'),
                'ItemCodeID': data.get('trItemID')
            })
        else:
            return pd.Series({
                'ItemCode': f"Error: {res.status_code}",
                'ItemCodeID': f"Error: {res.status_code}"
            })
    except Exception as e:
        return f"Exception: {e}"


def update_item_refs(row):
    update_item_ref_api = f"https://{tr_base_api}/api/v1.0/Items/UpdateItemRef"
    headers = {"Authorization": f"Bearer {tr_access_token}", "Content-Type": "application/json"}
    update_item_ref_payload = {
    "ItemID": int(row['ItemCodeID'])
    }
    ref_dict = {col: row[col] for col in row.index if re.match(r'REF\d+$', col)}
    update_item_ref_payload.update(ref_dict)
    log.info(update_item_ref_payload)
    try:
        res = requests.put(update_item_ref_api, json=update_item_ref_payload, headers=headers)
        if res.status_code == 200:
            return "Ref Fields Updated"
        else:
            return f"Error: {res.status_code}"
    except Exception as e:
        return f"Exception: {e}"


def get_item_refs(row):
    item_id = row['ItemCodeID']
    tr_ocr_column = row['tr_ocr_column']
    get_item_ref_api = f"https://{tr_base_api}/api/v1.0/Items/{item_id}/GetItem"
    log.info(get_item_ref_api)
    headers = {"Authorization": f"Bearer {tr_access_token}", "Content-Type": "application/json"}
    try:
        res = requests.get(get_item_ref_api, headers=headers)
        if res.status_code == 200:
            return res.json()[tr_ocr_column]
        else:
            return f"Error: {res.status_code}"
    except Exception as e:
        return f"Exception: {e}"


def main(**kwargs):
    global user, password, db_name, authdatabase, hosts,  source, all_columns_with_types, base_api, ms_oauth_token, ms_api_session, ms_tritem_service_obj, ms_workorder_api, tr_base_api, tr_access_token, batch_name
    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME',
                               'WORKFLOW_NAME',
                               'WORKFLOW_RUN_ID',
                               'CatalogDB',
                               'IncomingCatalogTable',
                               'EnrichmentCatalogTable',
                               'BaseApi',
                               'SecretManagerName',
                               'RegionName',
                               'TimestreamDB',
                               'TimestreamTable',
                               'TimestreamSQSQueue',
                               'CustomBucket',
                               'BaseTRApi',
                               'TRSecretManagerName',
                               'APIUserID'])

    log.info(f"arguments are {args}")
    try:
        job_name = args['JOB_NAME']
        glue_client = boto3.client('glue')
        workflow_name = args['WORKFLOW_NAME']
        log.info(f"WORKFLOW_NAME: {workflow_name}", )
        workflow_run_id = args['WORKFLOW_RUN_ID']
        log.info(f"WORKFLOW_RUN_ID : {workflow_run_id}", )
        catalog_db = args['CatalogDB']
        incoming_catalog_table = args['IncomingCatalogTable']
        ocr_catalog_table = args['EnrichmentCatalogTable']
        base_api = args['BaseApi']
        secretManagerName = args["SecretManagerName"]
        regionName = args["RegionName"]
        customBucketName = args['CustomBucket']
        tr_base_api = args['BaseTRApi']
        trSecretManagerName = args["TRSecretManagerName"]
        api_user_id = int(args["APIUserID"])

        job.init(job_name, args)

        workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)["RunProperties"]

        transaction_id = workflow_params['transaction_id']
        source = workflow_params['source']
        zip_key = workflow_params['zip_key']
        incoming_bucket = workflow_params['incoming_bucket']
        source_device = workflow_params['source_device'] # Just for logging purpose

        batch_name = zip_key.rsplit('/', 1)[-1].rsplit('.', 1)[0]
        log.info(f"batch_name: '{batch_name}'")

        log.info(f"transaction_id: '{transaction_id}'\n"
                 f"source: '{source}'\n"
                 f"zip_key: '{zip_key}'\n"
                 f"incoming_bucket: '{incoming_bucket}'\n"
                 f"source_device: '{source_device}'\n")        

        # Secrets & Connections
        secret_keys = secret_manager.AWSSecretsManager(secretManagerName, regionName).get_secret_value()
        server, user, password, replicaset, dnsserver, rtk, authdatabase, db_name, hosts = (get_mongo_details_from_secrets(secret_keys))
        log.info(f"mongo details are {user}, {db_name}, {authdatabase}, {hosts}")

        tr_secret_keys = secret_manager.AWSSecretsManager(trSecretManagerName, regionName).get_secret_value()
        tr_grant_type, tr_username, tr_password = (get_tr_details_from_secrets(tr_secret_keys))
        log.info("Fetched TR Credentials to create Token")
        log.info("Creating Token to call TR APIs")
        tr_access_token = create_tr_access_token(tr_grant_type, tr_username, tr_password)
        log.info("Access Token Created")        


        # 1. Fetch Config via Utility
        log.info("Fetching Configuration from Catalog...")
        location = S3Location(incoming_bucket, "Onb_Customer_Conf/", "")
        incoming_catalog_obj = GlueCatalog(catalog_db, incoming_catalog_table)
        transaction_obj = Transaction(transaction_id, batch_name)
        
        tr_account_name = '-'
        tr_customer_id = None
        collection_config = None
        config_present = False
        load_to_system_of_records = False

        try:
            tr_account_name, collection_config = ConfigurationTemplate.get_collection_config_from_catalog(
                location, incoming_catalog_obj, transaction_obj, spark, log
            )
            config_present = True
            log.info(f"Configuration Found for {tr_account_name}")
            if collection_config:
                load_to_system_of_records = collection_config.get("load_to_system_of_records", False)
                tr_customer_id = collection_config.get("tr_customer_id", '-')
                log.info(f"The TR Customer ID is: {tr_customer_id}")
        except (ConfigurationNotFound, CollectionNotFound, ClientError) as e:
            log.warning(f"Configuration NOT Found: {e}")
            config_present = False

        # 2. Get Dataframes
        # Get dataframe using utility which handles renaming standardize

        try:
            incoming_catalog_df = ConfigurationTemplate.get_catalog_data_from_batchname_transaction(spark, incoming_catalog_obj, transaction_obj)
            incoming_catalog_df = incoming_catalog_df.withColumnRenamed("image_path", "image_path_scanner") \
                                .withColumnRenamed("transaction_id", "transaction_id_scanner")
        except GlueCatalogException as e:
            log.info(f"Critical error in the 'get_catalog_data_from_batchname_transaction' function querying the table '{catalog_db}.{incoming_catalog_table}':\n{str(e)}\n")
            log.info(f"Error type: {type(e).__name__}")
            exit("OCR - Critical error")

        # Clean up Incoming DF (remove REFs)
        cols_to_drop = [c for c in incoming_catalog_df.columns if re.match(r'ref\d+$', c) and c != 'ref1']
        incoming_catalog_df = incoming_catalog_df.drop(*cols_to_drop)
        log.info("Load File Records Fetched from the Glue Catalog...")

        incoming_catalog_df.cache()
        log.info("Cached the incoming_catalog_df")

        ocr_catalog_df = get_catalog_data(catalog_db, ocr_catalog_table, batch_name, transaction_id)
        log.info("OCR Records Fetched from the Glue Catalog...")

        ocr_catalog_df.cache()
        log.info("Cached the ocr_catalog_df")        
        
        # Fallback for tr_account_name if config was missing
        if not tr_account_name:
             try:
                tr_account_name = incoming_catalog_df.select("collection").first()[0]
                log.info(f"TR Account Name is: {tr_account_name}")
             except:
                tr_account_name = "-"
                log.info(f"TR Account Name is: {tr_account_name}")



        # Check for ItemCode and ItemCodeID in Incoming DF --> if present we can add use updateitemref api
        has_item_code = "item_code" in incoming_catalog_df.columns
        has_item_code_id = "item_code_id" in incoming_catalog_df.columns
        
        # Optimization: check one row to see if they are populated
        is_existing_item_batch = False
        if has_item_code and has_item_code_id:
            try:
                first_row = incoming_catalog_df.select("item_code", "item_code_id").first()
                if first_row and first_row["item_code"] and first_row["item_code_id"]:
                    is_existing_item_batch = True
            except Exception:
                is_existing_item_batch = False  

        log.info(f"Batch Analysis: Existing Item Batch = {is_existing_item_batch}, Config Present = {config_present}")

        if not is_existing_item_batch and config_present:
            if workflow_name.split('-')[-1] == 'retrigger':
                total_record_count  = str(len(ast.literal_eval(workflow_params['filter_parameters'])))
            else:
                total_record_count = str(ocr_catalog_df.filter(ocr_catalog_df["data_is_valid"] == "True").count())

            log.info(f"Number of Images in the Batch to Load to TR/Unify: {total_record_count}")

            Onboarding_Timestream_Manager.timestream_insert_data(
                db=args["TimestreamDB"],
                table=args["TimestreamTable"],
                timestream_queue=args["TimestreamSQSQueue"],
                measure_name=f'OnboardingWF_{pipeline_mod}',
                content=Onboarding_Timestream_Manager.create_timestream_content(
                    source=source,
                    source_device=source_device,
                    transaction_id=transaction_id,
                    batch_name=batch_name,
                    pipeline_mod=pipeline_mod,
                    state="Ready",
                    no_of_asset=total_record_count,
                    tr_account_name='-'
                )
            )

            customer_data = get_one_item(db_name, "Customer", {"extCustomerId.extId": str(tr_customer_id)})
            try:
                unify_customer_id = str(customer_data['_id'])
                tr_customer_id = customer_data["extCustomerId"]["extId"]
                tr_account_name = customer_data["name"]
                customer_industry_id = customer_data['industry']
                log.info(f"Unify Customer ID is: {unify_customer_id}")
                log.info(f"TR Customer ID is: {tr_customer_id}")
                log.info(f"Unify Customer Industry ID is: {customer_industry_id}")
            except TypeError as e:
                log.error(f"TypeError occurred: {e}")
                error = f"{tr_account_name} TR Customer not found in Unify Collection"
                log.error(error)
                Onboarding_Timestream_Manager.timestream_insert_data(
                    db=args["TimestreamDB"],
                    table=args["TimestreamTable"],
                    timestream_queue=args["TimestreamSQSQueue"],
                    measure_name=f'OnboardingWF_{pipeline_mod}',
                    content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                        source=source,
                        source_device=source_device,
                        transaction_id=transaction_id,
                        batch_name=batch_name,
                        pipeline_mod=pipeline_mod,
                        record_count="0",
                        error=error,
                        error_code="41",
                        tr_account_name=tr_account_name if tr_account_name is not None else '-'
                    )
                )
                exit(f"Unify Customer not found for: {tr_account_name}")

            industry_data = get_one_item(db_name, "Industry", {"_id": ObjectId(customer_industry_id)})
            try:
                industry_name = str(industry_data['name'])
                log.info(f"Unify Customer {tr_account_name}'s indsutry is: {industry_name}")
            except TypeError as e:
                log.error(f"TypeError occurred: {e}")
                error = f"{tr_account_name} TR Customer not found in Unify Collection"
                log.error(error)
                Onboarding_Timestream_Manager.timestream_insert_data(
                    db=args["TimestreamDB"],
                    table=args["TimestreamTable"],
                    timestream_queue=args["TimestreamSQSQueue"],
                    measure_name=f'OnboardingWF_{pipeline_mod}',
                    content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                        source=source,
                        source_device=source_device,
                        transaction_id=transaction_id,
                        batch_name=batch_name,
                        pipeline_mod=pipeline_mod,
                        record_count="0",
                        error=error,
                        error_code="43",
                        tr_account_name=tr_account_name if tr_account_name is not None else '-'
                    )
                )
                exit(f"Unify Customer not found for: {tr_account_name}")

            try:
                # We already have incoming_catalog_df and tr_account_name, but we use the utility method to get the config
                # and it will internally also fetch the dataframe again, but this is the requested design.
                # We ignore the returned tr_account_name as we already have it and verified it matches (implicit).
                # We get the collection object (config).
                _, collection_config = ConfigurationTemplate.get_collection_config_from_catalog(
                    location, incoming_catalog_obj, transaction_obj, spark, log
                )
            except (ClientError, ConfigurationNotFound) as e:
                log.error(f"ClientError/ConfigurationNotFound occurred: {e}")
                error = f"Customer Configuration not accessible for: {tr_account_name}"
                log.error(error)
                Onboarding_Timestream_Manager.timestream_insert_data(
                    db=args["TimestreamDB"],
                    table=args["TimestreamTable"],
                    timestream_queue=args["TimestreamSQSQueue"],
                    measure_name=f'OnboardingWF_{pipeline_mod}',
                    content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                        source=source,
                        source_device=source_device,
                        transaction_id=transaction_id,
                        batch_name=batch_name,
                        pipeline_mod=pipeline_mod,
                        record_count="0",
                        error=error,
                        error_code="42",
                        tr_account_name=tr_account_name if tr_account_name is not None else '-'
                    )
                )
                exit(f"Customer Configuration not found for: {tr_account_name}")
            except CollectionNotFound as e:
                log.error(f"CollectionNotFound occurred: {e}")
                error = f"Collection Configuration not found inside template for: {tr_account_name}"
                log.error(error)
                Onboarding_Timestream_Manager.timestream_insert_data(
                    db=args["TimestreamDB"],
                    table=args["TimestreamTable"],
                    timestream_queue=args["TimestreamSQSQueue"],
                    measure_name=f'OnboardingWF_{pipeline_mod}',
                    content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                        source=source,
                        source_device=source_device,
                        transaction_id=transaction_id,
                        batch_name=batch_name,
                        pipeline_mod=pipeline_mod,
                        record_count="0",
                        error=error,
                        error_code="42",
                        tr_account_name=tr_account_name if tr_account_name is not None else '-'
                    )
                )
                exit(f"Collection Configuration not found for: {tr_account_name}")
            except GlueCatalogException as e:
                log.error(f"GlueCatalogException occurred: {e}")
                error = f"Glue/Spark Error: {e}"
                log.error(error)
                Onboarding_Timestream_Manager.timestream_insert_data(
                    db=args["TimestreamDB"],
                    table=args["TimestreamTable"],
                    timestream_queue=args["TimestreamSQSQueue"],
                    measure_name=f'OnboardingWF_{pipeline_mod}',
                    content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                        source=source,
                        source_device=source_device,
                        transaction_id=transaction_id,
                        batch_name=batch_name,
                        pipeline_mod=pipeline_mod,
                        record_count="0",
                        error=error,
                        error_code="47",
                        tr_account_name=tr_account_name if tr_account_name is not None else '-'
                    )
                )
                exit(f"Glue/Spark Error for: {tr_account_name}")

            field_list = collection_config["field_list"]
            log.info(f"Project's Field Schema is: {field_list}")

            load_to_system_of_records = collection_config["load_to_system_of_records"]
            log.info(f"Fetching the Project's load_to_system_of_records flag. Value is {load_to_system_of_records}")

            json_schema = StructType(create_json_schema_from_field_list(field_list))
            log.info(f"Inferred Schema: {json_schema.simpleString()}")

            if workflow_name.split('-')[-1] == 'retrigger':
                ocr_df = ocr_catalog_df.withColumn("file_name", regexp_extract(col("image_path"), ".*/(.*)$", 1))
                opsiq_df = create_opsiq_df(workflow_params["filter_parameters"], transaction_id, batch_name, field_list)
                ocr_df = opsiq_df.join(ocr_df, (opsiq_df.transaction_id_opsiq == ocr_df.transaction_id) & (
                            opsiq_df.batch_name_opsiq == ocr_df.batch_name) & (
                                                    opsiq_df.file_name_opsiq == ocr_df.file_name)) \
                    .drop("transaction_id_opsiq", "batch_name_opsiq", "file_name_opsiq")
            else:
                ocr_df = ocr_catalog_df.filter(ocr_catalog_df["data_is_valid"] == "True")
                ocr_df = explode_key_value_pairs(ocr_df, json_schema, field_list)

            log.info("OCR DF is: ")
            ocr_df.show(2, vertical=True, truncate=False)

            ocr_df_count = ocr_df.count()
            if ocr_df_count == 0:
                error = "Empty Filtered OCR Dataframe Created. No Data present to Load."
                log.error(error)
                # Using content helper to preserve "NoOp" state, but appending Error/ErrorCode manually
                content = Onboarding_Timestream_Manager.create_timestream_content(
                    source=source,
                    source_device=source_device,
                    transaction_id=transaction_id,
                    batch_name=batch_name,
                    pipeline_mod=pipeline_mod,
                    state="NoOp",
                    no_of_asset="0",
                    tr_account_name=tr_account_name if tr_account_name is not None else '-'
                )
                content["Dimensions"].append({"Error": error, "Type": "VARCHAR"})
                content["Dimensions"].append({"ErrorCode": "44", "Type": "VARCHAR"})
                
                Onboarding_Timestream_Manager.timestream_insert_data(
                    db=args["TimestreamDB"],
                    table=args["TimestreamTable"],
                    timestream_queue=args["TimestreamSQSQueue"],
                    measure_name=f'OnboardingWF_{pipeline_mod}',
                    content=content
                )

            else:
                ocr_df.cache()
                log.info("Cached the ocr_df")

                log.info("Removing ref fields except ref1 from incoming_catalog_df")
                ref_cols_to_drop = [col for col in incoming_catalog_df.columns if re.match(r'ref\d+$', col) and col != 'ref1']

                incoming_catalog_df = incoming_catalog_df.drop(*ref_cols_to_drop)
                log.info("Removed the ref fields from incoming_catalog_df")

                merged_df = ocr_df.join(incoming_catalog_df, (ocr_df["transaction_id"] == incoming_catalog_df["transaction_id_scanner"]) & (
                            ocr_df["image_path"] == incoming_catalog_df["image_path_scanner"])) \
                            .drop("image_path_scanner", "transaction_id_scanner") \
                            .withColumnRenamed("session_notes", "work_order")

                log.info("Joined dataframe of OCR and Incoming Dataframes:")
                merged_df.show(2, vertical=True, truncate=False)

                merged_df.cache()
                log.info("Cached the merged_df")

                distinct_box_df = merged_df.select("box_barcode").distinct()
                distinct_box_list = [row["box_barcode"] for row in distinct_box_df.collect()]

                log.info(f"List of distinct Boxes: {distinct_box_list}")

                token_api_url = f'https://{base_api}/v1/oauth/token'
                client_id, client_secret, grant_type, username, api_password = get_api_secret(secret_keys)
                ms_api_session = requests.Session()
                ms_oauth_token = create_access_token(token_api_url, client_id, client_secret, grant_type, username, api_password)

                log.info('ETL::ms_oauth_token %s', ms_oauth_token)
                box_guid_mapping = [(box_barcode, get_box_guid(box_barcode, unify_customer_id)) for box_barcode in distinct_box_list]

                box_df = spark.createDataFrame(box_guid_mapping, ["box", "Box_GUID"])

                final_df = merged_df.join(box_df, merged_df.box_barcode == box_df.box).drop("box")

                final_df = final_df.withColumn("Customer File Barcode", lit("")) \
                            .withColumn("customerFileBarcode", lit("")) \
                            .withColumn("customerSchemaType", upper(split("image_filename", "-")[0])) \
                            .withColumn("itemType", upper(split("image_filename", "-")[0])) \
                            .withColumn("customerId", lit(unify_customer_id)) \
                            .withColumn("parentId", col("Box_GUID")) \
                            .withColumn("parentItemCode", col("box_barcode")) \
                            .withColumn("Access Box Barcode", col("box_barcode")) \
                            .withColumn("sourcePath", col("image_path")) \
                            .withColumn("locationId", lit("")) \
                            .withColumn("departmentId", lit("")) \
                            .withColumn("action", lit("4")) \
                            .withColumn("attachment", lit("false")) \
                            .withColumn("REF10", substring(col("ocr_liquid_text"), 1, 900)) \
                            .withColumn("AFS Index Date", date_format(current_timestamp(), "yyyy-MM-dd HH:mm")) \
                            .withColumn("REF30", date_format(current_timestamp(), "yyyy-MM-dd HH:mm")) \
                            .withColumn("Department", lit("")) \
                            .withColumn("TR File Barcode", lit(""))

                log.info("Final Dataframe containing all the fields: ")
                log.info(final_df.show(2))

                final_df.cache()
                log.info("Cached the final_df")

                content_type = final_df.select("customerSchemaType").first()[0]

                log.info(f"Item type of the batch is : {content_type}")

                log.info(f"Fetching the TR Unify Mapping from Template Collection for the customer with Entity Type: {content_type}")
                template_data = get_one_item(db_name, "Template", {"customerId": unify_customer_id, "isActive": True})
                tr_unify_mapping = get_tr_unify_mapping(template_data, content_type)

                tr_unify_field_mapping = [(i["name"], f"{i['ref']}" if 'ref' in i else None) for i in tr_unify_mapping]

                tr_unify_field_mapping_df = spark.createDataFrame(tr_unify_field_mapping, ["unify_field", "tr_field"])

                log.info(f"TR Unify Field Mapping is: {tr_unify_field_mapping_df.show()}")
                log.info("Filtering not null fields from TR Unify Field Mapping for creating TR dataframe")
                tr_field_mapping_df = tr_unify_field_mapping_df.filter(tr_unify_field_mapping_df["tr_field"].isNotNull())

                tr_mapping_dict = {row["unify_field"]: row["tr_field"] for row in tr_field_mapping_df.collect()}

                add_tr_columns = [(col_name, tr_mapping_dict[col_name]) for col_name in final_df.columns if
                                col_name in tr_mapping_dict]
                # renamed_columns.extend(
                #     [("customerId", "customerId"), ("departmentId", "departmentId"), ("itemType", "itemType"),
                #      ("action", "action")])
                for old_col, new_col in add_tr_columns:
                    if 'REF' in new_col:
                        final_df = final_df.withColumn(new_col, col(old_col))

                add_tr_columns.extend(
                    [("customerId", "customerId"), ("departmentId", "departmentId"), ("itemType", "itemType"),
                    ("action", "action")])
                tr_df = final_df.select(
                    [final_df[col_name].alias(new_col_name) for col_name, new_col_name in add_tr_columns]
                )

                tr_df = tr_df.withColumn(
                    "itemType",
                    when(col("itemType") == "FILE", "3")
                    .when(col("itemType") == "BOX", "2")
                    .otherwise("1")) \
                    .withColumn("itemCode", lit(""))

                tr_mask_columns_list, unify_mask_columns_list = fetch_columns_to_mask(tr_mapping_dict)

                log.info("Registering the masking UDF")
                mask_pii_udf = udf(mask_pii, StringType())

                tr_mask_df = tr_df
                for column in tr_mask_columns_list:
                    tr_mask_df = tr_mask_df.withColumn(column, mask_pii_udf(col(column)))

                log.info("TR Dataframe created:")
                tr_df.show(2, vertical=True, truncate=False)
                tr_mask_df.show(2, vertical=True, truncate=False)

                log.info("Selecting the columns required for Unify Item v2")
                metadata_column_names = [row["unify_field"] for row in tr_unify_field_mapping_df.select("unify_field").collect()]
                log.info(f"Columns are: {metadata_column_names}")
                col_length = len(metadata_column_names)
                log.info(f"Number of columns for itemV2 is: {col_length}")
                # For metaData field
                # final_df = final_df.withColumn("metaData", create_map(list(chain(*(
                #     (lit(metadata_column_names[i]), col(metadata_column_names[i])) for i in range(0, col_length))))))

                unify_item_df = final_df.select("customerId", "locationId",
                                                "parentId", "attachment", "customerSchemaType",
                                                "itemType", "departmentId", "sourcePath",
                                                "REF10", "REF30")

                # spark.udf.register("mask_pii_udf", mask_pii)

                unify_item_mask_df = final_df
                unify_item_mask_df = unify_item_mask_df.withColumn("REF10", mask_pii_udf(col("REF10")))

                unify_item_mask_df = unify_item_mask_df.select("customerId", "locationId",
                                                "parentId", "attachment", "customerSchemaType",
                                                "itemType", "departmentId", "sourcePath",
                                                "REF10", "REF30")

                log.info("Unify Item v2 Dataframe created:")
                unify_item_df.show(2, vertical=True, truncate=False)
                unify_item_mask_df.show(2, vertical=True, truncate=False)

                #todo Add timestream event to capture the start of the module

                if not load_to_system_of_records:
                    log.info("Logging the details identical to what it will write to TR and Unify")
                    log.info("The data that will be written through TR API")
                    tr_mask_df.show(vertical=True, truncate=False)
                    tr_record_count = str(tr_mask_df.count())
                    log.info(f"Number of records to be loaded to TR System of Records: {tr_record_count}")
                    rows = final_df.sort("image_path").collect()
                    for tr_row in rows:
                        Onboarding_Timestream_Manager.timestream_insert_data(
                            db=args["TimestreamDB"],
                            table=args["TimestreamTable"],
                            timestream_queue=args["TimestreamSQSQueue"],
                            measure_name=f'OnboardingWF_{pipeline_mod}',
                            content=Onboarding_Timestream_Manager.create_timestream_content(
                                source=source,
                                source_device=source_device,
                                transaction_id=transaction_id,
                                batch_name=batch_name,
                                pipeline_mod=pipeline_mod,
                                state="Complete",
                                no_of_asset="1",
                                file_path=tr_row['image_path'],
                                event_name="TRLoadLog",
                                tr_account_name=tr_account_name if tr_account_name is not None else '-'
                            )
                        )

                    final_record_count = final_df.count()
                    log.info(f"Records in the LoadData Module that got loaded: {final_record_count}")
                    Onboarding_Timestream_Manager.timestream_insert_data(
                        db=args["TimestreamDB"],
                        table=args["TimestreamTable"],
                        timestream_queue=args["TimestreamSQSQueue"],
                        measure_name=f'OnboardingWF_{pipeline_mod}',
                        content=Onboarding_Timestream_Manager.create_timestream_content(
                            source=source,
                            source_device=source_device,
                            transaction_id=transaction_id,
                            batch_name=batch_name,
                            pipeline_mod=pipeline_mod,
                            state="Complete",
                            no_of_asset=str(final_record_count),
                            tr_account_name=tr_account_name if tr_account_name is not None else '-'
                        )
                    )
                    log.info("Logged to Timestream")
                else:
                    log.info("Writing to TR API")
                    log.info("Load to TR Starting")
                    log.info("Creating Token to call TR APIs")
                    tr_access_token = create_tr_access_token(tr_grant_type, tr_username, tr_password)
                    log.info("Access Token Created")

                    #todo Confirm the flow if a batch could have multiple boxes
                    box_barcode = distinct_box_list[0]

                    log.info(f"Getting Parent Id of Box Barcode {box_barcode} from TR")
                    tr_parent_id = get_tr_box_parent_id(tr_customer_id, box_barcode)
                    log.info(f"TR Item ID for Box {box_barcode} is {tr_parent_id}")

                    log.info(f"Creating Work Order for Box {box_barcode}")
                    tr_work_order_id = create_tr_work_order(tr_customer_id, api_user_id)
                    log.info(f"Created Work Order with ID {tr_work_order_id}")

                    #todo Fetch these 2 values dynamically
                    tr_type_id = 720899 if customBucketName.split('-')[0] == "prod" else 115704
                    tr_storage_code = "FILES"
                    log.info("Converting the Pyspark Dataframe to Pandas Dataframe")
                    final_pdf = final_df.toPandas()

                    log.info("Creating Items in TR")
                    item_codes = final_pdf.apply(create_itemcode, axis=1, args=(tr_customer_id, tr_work_order_id, tr_type_id, tr_storage_code, tr_parent_id))
                    final_pdf = pd.concat([final_pdf, item_codes], axis=1)
                    final_pdf.head()

                    if (final_pdf['ItemCode'] == 'Error: 500').all():
                        log.error("All values in the ItemCode column are Error: 500.")
                        error = f"{tr_base_api}/api/v1.0/Items/CreateItem API call failed to create Items in TR"
                        log.error(error)
                        Onboarding_Timestream_Manager.timestream_insert_data(
                            db=args["TimestreamDB"],
                            table=args["TimestreamTable"],
                            timestream_queue=args["TimestreamSQSQueue"],
                            measure_name=f'OnboardingWF_{pipeline_mod}',
                            content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                                source=source,
                                source_device=source_device,
                                transaction_id=transaction_id,
                                batch_name=batch_name,
                                pipeline_mod=pipeline_mod,
                                record_count="0",
                                error=error,
                                error_code="45",
                                event_name="TRLoad",
                                tr_account_name=tr_account_name if tr_account_name is not None else '-'
                            )
                        )
                        exit(error)
                    log.info("Created the Items")

                    log.info("Populating the Department Name")
                    final_pdf['TR File Barcode'] = final_pdf['ItemCode']
                    final_pdf['Customer File Barcode'] = final_pdf['ItemCode']
                    final_pdf['REF1'] = final_pdf['ItemCode']
                    final_pdf['customerFileBarcode'] = final_pdf['ItemCode']

                    log.info("Updating Ref Fields in TR")

                    log.info(final_pdf.head())

                    final_pdf['tr_load_status'] = final_pdf.apply(update_item_refs, axis=1)
                    if (final_pdf['tr_load_status'] == 'Error: 500').all():
                        log.error("All values in the ItemCode column are Error: 500.")
                        error = f"{tr_base_api}/api/v1.0/Items/UpdateItemRef API call failed to Update Items in TR"
                        log.error(error)
                        Onboarding_Timestream_Manager.timestream_insert_data(
                            db=args["TimestreamDB"],
                            table=args["TimestreamTable"],
                            timestream_queue=args["TimestreamSQSQueue"],
                            measure_name=f'OnboardingWF_{pipeline_mod}',
                            content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                                source=source,
                                source_device=source_device,
                                transaction_id=transaction_id,
                                batch_name=batch_name,
                                pipeline_mod=pipeline_mod,
                                record_count="0",
                                error=error,
                                error_code="46",
                                tr_account_name=tr_account_name if tr_account_name is not None else '-'
                            )
                        )
                        exit(error)
                    log.info("Updated the Item Refs for the Images")

                    final_pdf_sorted = final_pdf.sort_values(by='image_filename')
                    for tr_row in final_pdf_sorted.itertuples(index=True):
                        if tr_row.tr_load_status == "Ref Fields Updated":
                            Onboarding_Timestream_Manager.timestream_insert_data(
                                db=args["TimestreamDB"],
                                table=args["TimestreamTable"],
                                timestream_queue=args["TimestreamSQSQueue"],
                                measure_name=f'OnboardingWF_{pipeline_mod}',
                                content=Onboarding_Timestream_Manager.create_timestream_content(
                                    source=source,
                                    source_device=source_device,
                                    transaction_id=transaction_id,
                                    batch_name=batch_name,
                                    pipeline_mod=pipeline_mod,
                                    state="Complete",
                                    no_of_asset="1",
                                    item_code=tr_row.ItemCode,
                                    file_path=tr_row.image_path,
                                    tr_account_name=tr_account_name if tr_account_name is not None else '-',
                                    event_name="TRLoad"
                                )
                            )
                        else:
                            Onboarding_Timestream_Manager.timestream_insert_data(
                                db=args["TimestreamDB"],
                                table=args["TimestreamTable"],
                                timestream_queue=args["TimestreamSQSQueue"],
                                measure_name=f'OnboardingWF_{pipeline_mod}',
                                content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                                source=source,
                                source_device=source_device,
                                transaction_id=transaction_id,
                                batch_name=batch_name,
                                pipeline_mod=pipeline_mod,
                                record_count="1",
                                error=tr_row.tr_load_status,
                                tr_account_name=tr_account_name if tr_account_name is not None else '-',
                                event_name="TRLoad",
                                file_path=tr_row.image_path
                             )
                            )

                    log.info("TR Load: Complete")

                    final_record_count = len(final_pdf[final_pdf.tr_load_status == "Ref Fields Updated"])
                    log.info(f"Records in the LoadData Module that got loaded: {final_record_count}")
                    Onboarding_Timestream_Manager.timestream_insert_data(
                        db=args["TimestreamDB"],
                        table=args["TimestreamTable"],
                        timestream_queue=args["TimestreamSQSQueue"],
                        measure_name=f'OnboardingWF_{pipeline_mod}',
                        content=Onboarding_Timestream_Manager.create_timestream_content(
                            source=source,
                            source_device=source_device,
                            transaction_id=transaction_id,
                            batch_name=batch_name,
                            pipeline_mod=pipeline_mod,
                            state="Complete",
                            no_of_asset=str(final_record_count),
                            tr_account_name=tr_account_name if tr_account_name is not None else '-'
                        )
                    )
                    log.info("Logged to Timestream")
        elif is_existing_item_batch and config_present:
            if workflow_name.split('-')[-1] == 'retrigger':
                total_record_count = str(len(ast.literal_eval(workflow_params['filter_parameters'])))
            else:
                total_record_count = str(ocr_catalog_df.filter(ocr_catalog_df["data_is_valid"] == "True").count())

            log.info(f"Number of Images in the Batch to Load to TR/Unify: {total_record_count}")

            Onboarding_Timestream_Manager.timestream_insert_data(
                db=args["TimestreamDB"],
                table=args["TimestreamTable"],
                timestream_queue=args["TimestreamSQSQueue"],
                measure_name=f'OnboardingWF_{pipeline_mod}',
                content=Onboarding_Timestream_Manager.create_timestream_content(
                    source=source,
                    source_device=source_device,
                    transaction_id=transaction_id,
                    batch_name=batch_name,
                    pipeline_mod=pipeline_mod,
                    state="Ready",
                    no_of_asset=total_record_count,
                    tr_account_name='-'
                )
            )

            customer_data = get_one_item(db_name, "Customer", {"extCustomerId.extId": str(tr_customer_id)})
            try:
                unify_customer_id = str(customer_data['_id'])
                tr_customer_id = customer_data["extCustomerId"]["extId"]
                tr_account_name = customer_data["name"]
                customer_industry_id = customer_data['industry']
                log.info(f"Unify Customer ID is: {unify_customer_id}")
                log.info(f"TR Customer ID is: {tr_customer_id}")
                log.info(f"Unify Customer Industry ID is: {customer_industry_id}")
            except TypeError as e:
                log.error(f"TypeError occurred: {e}")
                error = f"{tr_account_name} TR Customer not found in Unify Collection"
                log.error(error)
                Onboarding_Timestream_Manager.timestream_insert_data(
                    db=args["TimestreamDB"],
                    table=args["TimestreamTable"],
                    timestream_queue=args["TimestreamSQSQueue"],
                    measure_name=f'OnboardingWF_{pipeline_mod}',
                    content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                        source=source,
                        source_device=source_device,
                        transaction_id=transaction_id,
                        batch_name=batch_name,
                        pipeline_mod=pipeline_mod,
                        record_count="0",
                        error=error,
                        error_code="41",
                        tr_account_name=tr_account_name if tr_account_name is not None else '-'
                    )
                )
                exit(f"Unify Customer not found for: {tr_account_name}")

            try:
                # We already have incoming_catalog_df and tr_account_name, but we use the utility method to get the config
                # and it will internally also fetch the dataframe again, but this is the requested design.
                # We ignore the returned tr_account_name as we already have it and verified it matches (implicit).
                # We get the collection object (config).
                _, collection_config = ConfigurationTemplate.get_collection_config_from_catalog(
                    location, incoming_catalog_obj, transaction_obj, spark, log
                )
            except (ClientError, ConfigurationNotFound) as e:
                log.error(f"ClientError/ConfigurationNotFound occurred: {e}")
                error = f"Customer Configuration not accessible for: {tr_account_name}"
                log.error(error)
                Onboarding_Timestream_Manager.timestream_insert_data(
                    db=args["TimestreamDB"],
                    table=args["TimestreamTable"],
                    timestream_queue=args["TimestreamSQSQueue"],
                    measure_name=f'OnboardingWF_{pipeline_mod}',
                    content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                        source=source,
                        source_device=source_device,
                        transaction_id=transaction_id,
                        batch_name=batch_name,
                        pipeline_mod=pipeline_mod,
                        record_count="0",
                        error=error,
                        error_code="42",
                        tr_account_name=tr_account_name if tr_account_name is not None else '-'
                    )
                )
                exit(f"Customer Configuration not found for: {tr_account_name}")
            except CollectionNotFound as e:
                log.error(f"CollectionNotFound occurred: {e}")
                error = f"Collection Configuration not found inside template for: {tr_account_name}"
                log.error(error)
                Onboarding_Timestream_Manager.timestream_insert_data(
                    db=args["TimestreamDB"],
                    table=args["TimestreamTable"],
                    timestream_queue=args["TimestreamSQSQueue"],
                    measure_name=f'OnboardingWF_{pipeline_mod}',
                    content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                        source=source,
                        source_device=source_device,
                        transaction_id=transaction_id,
                        batch_name=batch_name,
                        pipeline_mod=pipeline_mod,
                        record_count="0",
                        error=error,
                        error_code="42",
                        tr_account_name=tr_account_name if tr_account_name is not None else '-'
                    )
                )
                exit(f"Collection Configuration not found for: {tr_account_name}")
            except GlueCatalogException as e:
                log.error(f"GlueCatalogException occurred: {e}")
                error = f"Glue/Spark Error: {e}"
                log.error(error)
                Onboarding_Timestream_Manager.timestream_insert_data(
                    db=args["TimestreamDB"],
                    table=args["TimestreamTable"],
                    timestream_queue=args["TimestreamSQSQueue"],
                    measure_name=f'OnboardingWF_{pipeline_mod}',
                    content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                        source=source,
                        source_device=source_device,
                        transaction_id=transaction_id,
                        batch_name=batch_name,
                        pipeline_mod=pipeline_mod,
                        record_count="0",
                        error=error,
                        error_code="47",
                        tr_account_name=tr_account_name if tr_account_name is not None else '-'
                    )
                )
                exit(f"Glue/Spark Error for: {tr_account_name}")

            field_list = collection_config["field_list"]
            log.info(f"Project's Field Schema is: {field_list}")

            load_to_system_of_records = collection_config["load_to_system_of_records"]
            log.info(f"Fetching the Project's load_to_system_of_records flag. Value is {load_to_system_of_records}")

            json_schema = StructType(create_json_schema_from_field_list(field_list))
            log.info(f"Inferred Schema: {json_schema.simpleString()}")

            if workflow_name.split('-')[-1] == 'retrigger':
                ocr_df = ocr_catalog_df.withColumn("file_name", regexp_extract(col("image_path"), ".*/(.*)$", 1))
                opsiq_df = create_opsiq_df(workflow_params["filter_parameters"], transaction_id, batch_name, field_list)
                ocr_df = opsiq_df.join(ocr_df, (opsiq_df.transaction_id_opsiq == ocr_df.transaction_id) & (
                        opsiq_df.batch_name_opsiq == ocr_df.batch_name) & (
                                               opsiq_df.file_name_opsiq == ocr_df.file_name)) \
                    .drop("transaction_id_opsiq", "batch_name_opsiq", "file_name_opsiq")
            else:
                ocr_df = ocr_catalog_df.filter(ocr_catalog_df["data_is_valid"] == "True")
                ocr_df = explode_key_value_pairs(ocr_df, json_schema, field_list)

            log.info("OCR DF is: ")
            ocr_df.show(2, vertical=True, truncate=False)

            ocr_df_count = ocr_df.count()
            if ocr_df_count == 0:
                error = "Empty Filtered OCR Dataframe Created. No Data present to Load."
                log.error(error)
                # Using content helper to preserve "NoOp" state, but appending Error/ErrorCode manually
                content = Onboarding_Timestream_Manager.create_timestream_content(
                    source=source,
                    source_device=source_device,
                    transaction_id=transaction_id,
                    batch_name=batch_name,
                    pipeline_mod=pipeline_mod,
                    state="NoOp",
                    no_of_asset="0",
                    tr_account_name=tr_account_name if tr_account_name is not None else '-'
                )
                content["Dimensions"].append({"Error": error, "Type": "VARCHAR"})
                content["Dimensions"].append({"ErrorCode": "44", "Type": "VARCHAR"})

                Onboarding_Timestream_Manager.timestream_insert_data(
                    db=args["TimestreamDB"],
                    table=args["TimestreamTable"],
                    timestream_queue=args["TimestreamSQSQueue"],
                    measure_name=f'OnboardingWF_{pipeline_mod}',
                    content=content
                )

            else:
                ocr_df.cache()
                log.info("Cached the ocr_df")

                log.info("Removing ref fields except ref1 from incoming_catalog_df")
                ref_cols_to_drop = [col for col in incoming_catalog_df.columns if
                                    re.match(r'ref\d+$', col) and col != 'ref1']

                incoming_catalog_df = incoming_catalog_df.drop(*ref_cols_to_drop)
                log.info("Removed the ref fields from incoming_catalog_df")

                merged_df = ocr_df.join(incoming_catalog_df,
                                        (ocr_df["transaction_id"] == incoming_catalog_df["transaction_id_scanner"]) & (
                                                ocr_df["image_path"] == incoming_catalog_df["image_path_scanner"])) \
                    .drop("image_path_scanner", "transaction_id_scanner") \
                    .withColumnRenamed("session_notes", "work_order")

                log.info("Joined dataframe of OCR and Incoming Dataframes:")
                merged_df.show(2, vertical=True, truncate=False)

                merged_df.cache()
                log.info("Cached the merged_df")

                # distinct_box_df = merged_df.select("box_barcode").distinct()
                # distinct_box_list = [row["box_barcode"] for row in distinct_box_df.collect()]
                #
                # log.info(f"List of distinct Boxes: {distinct_box_list}")
                #
                # token_api_url = f'https://{base_api}/v1/oauth/token'
                # client_id, client_secret, grant_type, username, api_password = get_api_secret(secret_keys)
                # ms_api_session = requests.Session()
                # ms_oauth_token = create_access_token(token_api_url, client_id, client_secret, grant_type, username,
                #                                      api_password)
                #
                # log.info('ETL::ms_oauth_token %s', ms_oauth_token)
                # box_guid_mapping = [(box_barcode, get_box_guid(box_barcode, unify_customer_id)) for box_barcode in
                #                     distinct_box_list]
                #
                # box_df = spark.createDataFrame(box_guid_mapping, ["box", "Box_GUID"])
                #
                # final_df = merged_df.join(box_df, merged_df.box_barcode == box_df.box).drop("box")
                item_type = batch_name.split("_")[2].split('.')[0].upper()
                tr_ocr_column = batch_name.split("_")[3].split('.')[0].upper()

                final_df = merged_df.withColumn("Customer File Barcode", lit("")) \
                    .withColumn("customerFileBarcode", lit("")) \
                    .withColumn("customerSchemaType", lit(item_type)) \
                    .withColumn("itemType", lit(item_type)) \
                    .withColumn("customerId", lit(unify_customer_id)) \
                    .withColumn("sourcePath", col("image_path")) \
                    .withColumn("locationId", lit("")) \
                    .withColumn("departmentId", lit("")) \
                    .withColumn(tr_ocr_column, substring(col("ocr_liquid_text"), 1, 900)) \
                    .withColumn("AFS Index Date", date_format(current_timestamp(), "yyyy-MM-dd HH:mm")) \
                    .withColumn("REF30", date_format(current_timestamp(), "yyyy-MM-dd HH:mm")) \
                    .withColumn("Department", lit("")) \
                    .withColumn("TR File Barcode", lit("")) \
                    .withColumn("ItemCode", col("item_code")) \
                    .withColumn("ItemCodeID", col("item_code_id"))

                log.info("Final Dataframe containing all the fields: ")
                log.info(final_df.show(2))

                final_df.cache()
                log.info("Cached the final_df")

                content_type = final_df.select("customerSchemaType").first()[0]

                log.info(f"Item type of the batch is : {content_type}")

                log.info(
                    f"Fetching the TR Unify Mapping from Template Collection for the customer with Entity Type: {content_type}")
                template_data = get_one_item(db_name, "Template", {"customerId": unify_customer_id, "isActive": True})
                tr_unify_mapping = get_tr_unify_mapping(template_data, content_type)

                tr_unify_field_mapping = [(i["name"], f"{i['ref']}" if 'ref' in i else None) for i in tr_unify_mapping]

                tr_unify_field_mapping_df = spark.createDataFrame(tr_unify_field_mapping, ["unify_field", "tr_field"])

                log.info(f"TR Unify Field Mapping is: {tr_unify_field_mapping_df.show()}")
                log.info("Filtering not null fields from TR Unify Field Mapping for creating TR dataframe")
                tr_field_mapping_df = tr_unify_field_mapping_df.filter(
                    tr_unify_field_mapping_df["tr_field"].isNotNull())

                tr_mapping_dict = {row["unify_field"]: row["tr_field"] for row in tr_field_mapping_df.collect()}

                add_tr_columns = [(col_name, tr_mapping_dict[col_name]) for col_name in final_df.columns if
                                  col_name in tr_mapping_dict]
                # renamed_columns.extend(
                #     [("customerId", "customerId"), ("departmentId", "departmentId"), ("itemType", "itemType"),
                #      ("action", "action")])
                for old_col, new_col in add_tr_columns:
                    if 'REF' in new_col:
                        final_df = final_df.withColumn(new_col, col(old_col))

                add_tr_columns.extend(
                    [("customerId", "customerId"), ("departmentId", "departmentId"), ("itemType", "itemType")])
                tr_df = final_df.select(
                    [final_df[col_name].alias(new_col_name) for col_name, new_col_name in add_tr_columns]
                )

                tr_df = tr_df.withColumn(
                    "itemType",
                    when(col("itemType") == "FILE", "3")
                    .when(col("itemType") == "BOX", "2")
                    .otherwise("1")) \
                    .withColumn("itemCode", lit(""))

                tr_mask_columns_list, unify_mask_columns_list = fetch_columns_to_mask(tr_mapping_dict)

                log.info("Registering the masking UDF")
                mask_pii_udf = udf(mask_pii, StringType())

                tr_mask_df = tr_df
                for column in tr_mask_columns_list:
                    tr_mask_df = tr_mask_df.withColumn(column, mask_pii_udf(col(column)))

                log.info("TR Dataframe created:")
                tr_df.show(2, vertical=True, truncate=False)
                tr_mask_df.show(2, vertical=True, truncate=False)

                log.info("Selecting the columns required for Unify Item v2")
                metadata_column_names = [row["unify_field"] for row in
                                         tr_unify_field_mapping_df.select("unify_field").collect()]
                log.info(f"Columns are: {metadata_column_names}")
                col_length = len(metadata_column_names)
                log.info(f"Number of columns for itemV2 is: {col_length}")

                unify_item_df = final_df.select("customerId", "locationId",
                                                "customerSchemaType",
                                                "itemType", "departmentId", "sourcePath",
                                                tr_ocr_column, "REF30")

                # spark.udf.register("mask_pii_udf", mask_pii)

                unify_item_mask_df = final_df
                unify_item_mask_df = unify_item_mask_df.withColumn(tr_ocr_column, mask_pii_udf(col(tr_ocr_column)))

                unify_item_mask_df = unify_item_mask_df.select("customerId", "locationId",
                                                               "customerSchemaType",
                                                               "itemType", "departmentId", "sourcePath",
                                                               tr_ocr_column, "REF30")

                log.info("Unify Item v2 Dataframe created:")
                unify_item_df.show(2, vertical=True, truncate=False)
                unify_item_mask_df.show(2, vertical=True, truncate=False)

                # todo Add timestream event to capture the start of the module

                if not load_to_system_of_records:
                    log.info("Logging the details identical to what it will write to TR and Unify")
                    log.info("The data that will be written through TR API")
                    tr_mask_df.show(vertical=True, truncate=False)
                    tr_record_count = str(tr_mask_df.count())
                    log.info(f"Number of records to be loaded to TR System of Records: {tr_record_count}")
                    rows = final_df.sort("image_path").collect()
                    for tr_row in rows:
                        Onboarding_Timestream_Manager.timestream_insert_data(
                            db=args["TimestreamDB"],
                            table=args["TimestreamTable"],
                            timestream_queue=args["TimestreamSQSQueue"],
                            measure_name=f'OnboardingWF_{pipeline_mod}',
                            content=Onboarding_Timestream_Manager.create_timestream_content(
                                source=source,
                                source_device=source_device,
                                transaction_id=transaction_id,
                                batch_name=batch_name,
                                pipeline_mod=pipeline_mod,
                                state="Complete",
                                no_of_asset="1",
                                file_path=tr_row['image_path'],
                                event_name="TRLoadLog",
                                tr_account_name=tr_account_name if tr_account_name is not None else '-'
                            )
                        )

                    final_record_count = final_df.count()
                    log.info(f"Records in the LoadData Module that got loaded: {final_record_count}")
                    Onboarding_Timestream_Manager.timestream_insert_data(
                        db=args["TimestreamDB"],
                        table=args["TimestreamTable"],
                        timestream_queue=args["TimestreamSQSQueue"],
                        measure_name=f'OnboardingWF_{pipeline_mod}',
                        content=Onboarding_Timestream_Manager.create_timestream_content(
                            source=source,
                            source_device=source_device,
                            transaction_id=transaction_id,
                            batch_name=batch_name,
                            pipeline_mod=pipeline_mod,
                            state="Complete",
                            no_of_asset=str(final_record_count),
                            tr_account_name=tr_account_name if tr_account_name is not None else '-'
                        )
                    )
                    log.info("Logged to Timestream")
                else:
                    log.info("Writing to TR API")
                    log.info("Load to TR Starting")
                    log.info("Creating Token to call TR APIs")
                    tr_access_token = create_tr_access_token(tr_grant_type, tr_username, tr_password)
                    log.info("Access Token Created")

                    # todo Confirm the flow if a batch could have multiple boxes
                    # box_barcode = distinct_box_list[0]
                    #
                    # log.info(f"Getting Parent Id of Box Barcode {box_barcode} from TR")
                    # tr_parent_id = get_tr_box_parent_id(tr_customer_id, box_barcode)
                    # log.info(f"TR Item ID for Box {box_barcode} is {tr_parent_id}")
                    #
                    # log.info(f"Creating Work Order for Box {box_barcode}")
                    # tr_work_order_id = create_tr_work_order(tr_customer_id, api_user_id)
                    # log.info(f"Created Work Order with ID {tr_work_order_id}")

                    # todo Fetch these 2 values dynamically
                    # tr_type_id = 720899 if customBucketName.split('-')[0] == "prod" else 115704
                    # tr_storage_code = "FILES"
                    log.info("Converting the Pyspark Dataframe to Pandas Dataframe")
                    final_pdf = final_df.toPandas()

                    # log.info("Creating Items in TR")
                    # item_codes = final_pdf.apply(create_itemcode, axis=1, args=(
                    # tr_customer_id, tr_work_order_id, tr_type_id, tr_storage_code, tr_parent_id))
                    # final_pdf = pd.concat([final_pdf, item_codes], axis=1)
                    # final_pdf.head()
                    #
                    # if (final_pdf['ItemCode'] == 'Error: 500').all():
                    #     log.error("All values in the ItemCode column are Error: 500.")
                    #     error = f"{tr_base_api}/api/v1.0/Items/CreateItem API call failed to create Items in TR"
                    #     log.error(error)
                    #     Onboarding_Timestream_Manager.timestream_insert_data(
                    #         db=args["TimestreamDB"],
                    #         table=args["TimestreamTable"],
                    #         timestream_queue=args["TimestreamSQSQueue"],
                    #         measure_name=f'OnboardingWF_{pipeline_mod}',
                    #         content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                    #             source=source,
                    #             source_device=source_device,
                    #             transaction_id=transaction_id,
                    #             batch_name=batch_name,
                    #             pipeline_mod=pipeline_mod,
                    #             record_count="0",
                    #             error=error,
                    #             error_code="45",
                    #             event_name="TRLoad",
                    #             tr_account_name=tr_account_name if tr_account_name is not None else '-'
                    #         )
                    #     )
                    #     exit(error)
                    # log.info("Created the Items")
                    #
                    # log.info("Populating the Department Name")
                    final_pdf['TR File Barcode'] = final_pdf['ItemCode']
                    final_pdf['Customer File Barcode'] = final_pdf['ItemCode']
                    final_pdf['REF1'] = final_pdf['ItemCode']
                    final_pdf['customerFileBarcode'] = final_pdf['ItemCode']

                    log.info("Updating Ref Fields in TR")

                    log.info(final_pdf.head())

                    final_pdf['tr_load_status'] = final_pdf.apply(update_item_refs, axis=1)
                    if (final_pdf['tr_load_status'] == 'Error: 500').all():
                        log.error("All values in the ItemCode column are Error: 500.")
                        error = f"{tr_base_api}/api/v1.0/Items/UpdateItemRef API call failed to Update Items in TR"
                        log.error(error)
                        Onboarding_Timestream_Manager.timestream_insert_data(
                            db=args["TimestreamDB"],
                            table=args["TimestreamTable"],
                            timestream_queue=args["TimestreamSQSQueue"],
                            measure_name=f'OnboardingWF_{pipeline_mod}',
                            content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                                source=source,
                                source_device=source_device,
                                transaction_id=transaction_id,
                                batch_name=batch_name,
                                pipeline_mod=pipeline_mod,
                                record_count="0",
                                error=error,
                                error_code="46",
                                tr_account_name=tr_account_name if tr_account_name is not None else '-'
                            )
                        )
                        exit(error)
                    log.info("Updated the Item Refs for the Images")

                    final_pdf_sorted = final_pdf.sort_values(by='image_filename')
                    for tr_row in final_pdf_sorted.itertuples(index=True):
                        if tr_row.tr_load_status == "Ref Fields Updated":
                            Onboarding_Timestream_Manager.timestream_insert_data(
                                db=args["TimestreamDB"],
                                table=args["TimestreamTable"],
                                timestream_queue=args["TimestreamSQSQueue"],
                                measure_name=f'OnboardingWF_{pipeline_mod}',
                                content=Onboarding_Timestream_Manager.create_timestream_content(
                                    source=source,
                                    source_device=source_device,
                                    transaction_id=transaction_id,
                                    batch_name=batch_name,
                                    pipeline_mod=pipeline_mod,
                                    state="Complete",
                                    no_of_asset="1",
                                    item_code=tr_row.ItemCode,
                                    file_path=tr_row.image_path,
                                    tr_account_name=tr_account_name if tr_account_name is not None else '-',
                                    event_name="TRLoad"
                                )
                            )
                        else:
                            Onboarding_Timestream_Manager.timestream_insert_data(
                                db=args["TimestreamDB"],
                                table=args["TimestreamTable"],
                                timestream_queue=args["TimestreamSQSQueue"],
                                measure_name=f'OnboardingWF_{pipeline_mod}',
                                content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                                    source=source,
                                    source_device=source_device,
                                    transaction_id=transaction_id,
                                    batch_name=batch_name,
                                    pipeline_mod=pipeline_mod,
                                    record_count="1",
                                    error=tr_row.tr_load_status,
                                    tr_account_name=tr_account_name if tr_account_name is not None else '-',
                                    event_name="TRLoad",
                                    file_path=tr_row.image_path
                                )
                            )

                    log.info("TR Load: Complete")

                    final_record_count = len(final_pdf[final_pdf.tr_load_status == "Ref Fields Updated"])
                    log.info(f"Records in the LoadData Module that got loaded: {final_record_count}")
                    Onboarding_Timestream_Manager.timestream_insert_data(
                        db=args["TimestreamDB"],
                        table=args["TimestreamTable"],
                        timestream_queue=args["TimestreamSQSQueue"],
                        measure_name=f'OnboardingWF_{pipeline_mod}',
                        content=Onboarding_Timestream_Manager.create_timestream_content(
                            source=source,
                            source_device=source_device,
                            transaction_id=transaction_id,
                            batch_name=batch_name,
                            pipeline_mod=pipeline_mod,
                            state="Complete",
                            no_of_asset=str(final_record_count),
                            tr_account_name=tr_account_name if tr_account_name is not None else '-'
                        )
                    )
                    log.info("Logged to Timestream")
        else:
            final_pdf = pd.DataFrame()
            merged_df = None

            # Checking the ref field to update
            tr_ocr_column = batch_name.split("_")[3].split('.')[0].upper()
            log.info(f"OCR Liquid Text that has to be populated to: {tr_ocr_column}")
                
            # 2. Config Missing: Partial Update (OCR + Date)
            log.info(f"Scenario: Existing Items + Config Missing -> Partial Update ({tr_ocr_column}/30)")
            merged_df = ocr_catalog_df.join(incoming_catalog_df,
                                    (ocr_catalog_df["transaction_id"] == incoming_catalog_df["transaction_id_scanner"]) &
                                    (ocr_catalog_df["image_path"] == incoming_catalog_df["image_path_scanner"])) \
                                    .drop("image_path_scanner", "transaction_id_scanner")

            merged_df = merged_df.groupBy("tr_customer_id", "item_code_id", "item_code") \
                .agg(
                concat_ws(" ", collect_list("ocr_liquid_text")).alias("ocr_liquid_text"),
                first("image_path").alias("image_path")
            )

            merged_df = (merged_df.withColumn("ItemCodeID", incoming_catalog_df["item_code_id"])
                                .withColumn(tr_ocr_column, substring(col("ocr_liquid_text"), 1, 900))
                                .withColumn("REF30", date_format(current_timestamp(), "yyyy-MM-dd HH:mm")))
            
            incoming_catalog_df.show(truncate=False, vertical=True)    
            ocr_catalog_df.show(truncate=False, vertical=True)       
            merged_df.show(truncate=False, vertical=True)

            total_record_count = str(merged_df.count())

            log.info(f"Number of Images in the Batch to be updated to TR: {total_record_count}")

            Onboarding_Timestream_Manager.timestream_insert_data(
                db=args["TimestreamDB"],
                table=args["TimestreamTable"],
                timestream_queue=args["TimestreamSQSQueue"],
                measure_name=f'OnboardingWF_{pipeline_mod}',
                content=Onboarding_Timestream_Manager.create_timestream_content(
                    source=source,
                    source_device=source_device,
                    transaction_id=transaction_id,
                    batch_name=batch_name,
                    pipeline_mod=pipeline_mod,
                    state="Ready",
                    no_of_asset=total_record_count,
                    tr_account_name=tr_account_name if tr_account_name is not None else '-'
                )
            )
                                    
            # Execute Update
            final_pdf = merged_df.toPandas()
            final_pdf['tr_ocr_column'] = tr_ocr_column.lower()

            log.info("Fetching the TR Item data")
            final_pdf['tr_item_refs'] = final_pdf.apply(get_item_refs, axis=1)

            log.info("Appending tr_ocr_column's existing data with new incoming ocr text and limiting it to 900 chars")
            final_pdf[tr_ocr_column] = final_pdf.apply(lambda row: (f"{row['tr_item_refs']} {row[tr_ocr_column]}".strip())[:900], axis=1)

            log.info("Loading data to TR")
            final_pdf['tr_load_status'] = final_pdf.apply(update_item_refs, axis=1)

            for tr_row in final_pdf.itertuples(index=True):
                if tr_row.tr_load_status == "Ref Fields Updated":
                    Onboarding_Timestream_Manager.timestream_insert_data(
                        db=args["TimestreamDB"],
                        table=args["TimestreamTable"],
                        timestream_queue=args["TimestreamSQSQueue"],
                        measure_name=f'OnboardingWF_{pipeline_mod}',
                        content=Onboarding_Timestream_Manager.create_timestream_content(
                            source=source,
                            source_device=source_device,
                            transaction_id=transaction_id,
                            batch_name=batch_name,
                            pipeline_mod=pipeline_mod,
                            state="Complete",
                            no_of_asset="1",
                            item_code=tr_row.item_code,
                            file_path=tr_row.image_path,
                            tr_account_name=tr_account_name if tr_account_name is not None else '-',
                            event_name="TRLoad"
                        )
                    )
                else:
                    Onboarding_Timestream_Manager.timestream_insert_data(
                        db=args["TimestreamDB"],
                        table=args["TimestreamTable"],
                        timestream_queue=args["TimestreamSQSQueue"],
                        measure_name=f'OnboardingWF_{pipeline_mod}',
                        content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                        source=source,
                        source_device=source_device,
                        transaction_id=transaction_id,
                        batch_name=batch_name,
                        pipeline_mod=pipeline_mod,
                        record_count="1",
                        error=tr_row.tr_load_status,
                        tr_account_name=tr_account_name if tr_account_name is not None else '-',
                        event_name="TRLoad",
                        file_path=tr_row.image_path
                     )
                    )

            log.info("TR Load: Complete")

            final_record_count = len(final_pdf[final_pdf.tr_load_status == "Ref Fields Updated"])
            log.info(f"Records in the LoadData Module that got loaded: {final_record_count}")
            Onboarding_Timestream_Manager.timestream_insert_data(
                db=args["TimestreamDB"],
                table=args["TimestreamTable"],
                timestream_queue=args["TimestreamSQSQueue"],
                measure_name=f'OnboardingWF_{pipeline_mod}',
                content=Onboarding_Timestream_Manager.create_timestream_content(
                    source=source,
                    source_device=source_device,
                    transaction_id=transaction_id,
                    batch_name=batch_name,
                    pipeline_mod=pipeline_mod,
                    state="Complete",
                    no_of_asset=str(final_record_count),
                    tr_account_name=tr_account_name if tr_account_name is not None else '-'
                )
            )
            log.info("Logged to Timestream")

    except Exception as e:
        log.error('Loader-Process:: Generic Exception is %s', e.__cause__)
        log.error('ETL:: %s', traceback.print_exception(type(e), e, e.__traceback__))
        error = str(e.__cause__) if e.__cause__ else str(e)
        Onboarding_Timestream_Manager.timestream_insert_data(
            db=args["TimestreamDB"],
            table=args["TimestreamTable"],
            timestream_queue=args["TimestreamSQSQueue"],
            measure_name=f'OnboardingWF_{pipeline_mod}',
            content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                source=source,
                source_device=source_device,
                transaction_id=transaction_id,
                batch_name=batch_name,
                pipeline_mod=pipeline_mod,
                record_count="0",
                error=error,
                error_code="47",
                tr_account_name=tr_account_name if tr_account_name is not None else '-'
            )
        )
        raise e


if __name__ == "__main__":
    main()