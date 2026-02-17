import boto3
from typing import Dict
import time
import json
import Onboarding_Log_Manager
from datetime import datetime

logger = Onboarding_Log_Manager.get_module_logger('Timestream_Mod')
logger.debug('___Timestream module___')



class SQSOperations:
    def __init__(self, queue_name):
        self.sqs_client = boto3.client('sqs')
        self.queue_name = queue_name
        self.queue_url = None

    def set_queue_url(self):
        response = self.sqs_client.get_queue_url(QueueName=self.queue_name)
        self.queue_url = response['QueueUrl']

    def push_message(self, message: str):
        self.set_queue_url()
        response = self.sqs_client.send_message(QueueUrl=self.queue_url, MessageBody=message)

        if 'MessageId' not in response:
            err_msg = f"ETL:: Error publishing message:\n'{message}'\nTo\n'{self.queue_name}'"
            logger.error(err_msg)
            raise ValueError(err_msg)

        logger.info(f'ETL:: Successfully published message: {message} to {self.queue_name}')



class DataFormationValidation:
    DIMENSIONS = {'TransactionId'}  # Values required by the dashboards


    def data_validation(self, input_data: Dict) -> None:

        for field in ["db", "table", "measure_name", "content"]:
            self._validate_field_exists(input_data, field)  # Validate top-level fields

        content = input_data["content"]

        for field in ["Dimensions", "Measures"]:
            self._validate_field_exists(content, field, "content")  # Validate nested content fields

        dimensions = content["Dimensions"]

        all_dimensions = {key for dim in dimensions for key in dim}  # Validate the list of required dimensions

        if not all([True if dim in all_dimensions else False for dim in self.__class__.DIMENSIONS]):
            err_msg = f"Following dimensions are required in the input -- {', '.join(self.__class__.DIMENSIONS)}"
            logger.error(err_msg)
            raise ValueError(err_msg)


    def form_sqs_timestream_data(self, input_data: Dict):
        db = input_data["db"]
        table = input_data["table"]
        measure_name = input_data["measure_name"]
        measure_type = 'MULTI'  # Defaulted to multi
        time_input = str(int(round(time.time() * 1000)))
        time_unit = 'MILLISECONDS'
        measures = []
        dimensions = []

        for item in input_data["content"]["Dimensions"]:
            _temp_item = {}
            for key, value in item.items():
                if key != "Type":
                    _temp_item["Name"] = key
                    _temp_item["Value"] = value
                else:
                    _temp_item["DimensionValueType"] = value

            dimensions.append(_temp_item)  # dimensions.append({'Name': key, 'Value': value, 'DimensionValueType': 'VARCHAR'})

        for item in input_data["content"]["Measures"]:
            _temp_item = {}
            for key, value in item.items():
                if key != "Type":
                    _temp_item["Name"] = key
                    _temp_item["Value"] = value
                else:
                    _temp_item["Type"] = value

            measures.append(_temp_item)  # measures.append({'Name': key, 'Value': value, 'Type': 'VARCHAR'})


        return {"db": db, "table": table, "records": [{'Dimensions': dimensions,
                                                       'MeasureName': measure_name,
                                                       'MeasureValues': measures,
                                                       'MeasureValueType': measure_type,
                                                       'Time': time_input,
                                                       'TimeUnit': time_unit
                                                       }]}


    @staticmethod
    def _validate_field_exists(data: Dict, field: str, context: str = "") -> None:

        if not data.get(field):
            error_context = f"inside the '{context}' key" if context else "in the input data"
            err_msg = f"The Key '{field}' is missing {error_context}"
            logger.error(err_msg)
            raise ValueError(err_msg)



def timestream_insert_data(db: str, table: str, measure_name: str, content: Dict, timestream_queue: str = "dev-csp-sqs-unify-timestream-queue"):
    """
    Main driver function to load the data into Timestream using SQS
    Args:
        content: Timestream Dimension and Measure values.

    Returns: None.
    """
    logger.info('Starting timestream...')

    # Data validation and creation
    data_validation_obj = DataFormationValidation()
    data_validation_obj.data_validation(locals())  # "The process fails immediately if any validation check fails."
    records = data_validation_obj.form_sqs_timestream_data(locals())
    
    # SQS operations
    sqs_obj = SQSOperations(locals()["timestream_queue"])
    sqs_obj.push_message(json.dumps(records))


def create_timestream_content(
    source: str,
    source_device: str,
    transaction_id: str,
    batch_name: str,
    pipeline_mod: str,
    state: str,
    no_of_asset: str,
    file_path: str = None,
    item_code: str = None,
    tr_account_name: str = "-",
    tr_customer_id: str = None,
    event_name: str = None
) -> dict:
    """
    Build a Timestream payload for success/processing state tracking.

    Args:
        source (str): Source identifier.
        source_device (str): Device/Pipeline name.
        transaction_id (str): Transaction identifier.
        batch_name (str): Batch identifier.
        pipeline_mod (str): Pipeline module name.
        state (str): Processing state (e.g., Ready, Complete).
        no_of_asset (str): Asset count.
        file_path (str): Optional file path.
        item_code (str): Optional item code (e.g., box barcode).
        tr_account_name (str): Optional TR account number/name.
        tr_customer_id (str): Optional TR customer ID.
        event_name (str): Optional event name override.

    Returns:
        dict: Payload for Timestream insertion.
    """
    content = {
        "Dimensions": [
            {"Source": source, "Type": "VARCHAR"},
            {"PipelineName": source_device, "Type": "VARCHAR"},
            {"TransactionId": transaction_id, "Type": "VARCHAR"},
            {"BatchName": batch_name, "Type": "VARCHAR"},
            {
                "TimeStamp": str(int(datetime.now().timestamp() * 1000)),
                "Type": "VARCHAR",
            },
        ],
        "Measures": [
            {"PipelineModule": pipeline_mod, "Type": "VARCHAR"},
            {"State": state, "Type": "VARCHAR"},
            {"NoOfAsset": no_of_asset, "Type": "BIGINT"},
        ],
    }
    
    if file_path:
        content["Dimensions"].append({"FilePath": file_path, "Type": "VARCHAR"})
    
    if item_code:
        content["Dimensions"].append({"ItemCode": item_code, "Type": "VARCHAR"})
        
    if tr_customer_id:
        content["Dimensions"].append({"TRAccountNo": str(tr_customer_id), "Type": "VARCHAR"})
    elif tr_account_name != "-":
         content["Dimensions"].append({"TRAccountNo": tr_account_name, "Type": "VARCHAR"})

    if event_name:
        content["Measures"].append({"EventName": event_name, "Type": "VARCHAR"})
        
    # If logging an event like validation success, timestamp is often added by caller logic
    # or implicitly by the timestream manager.
    # However, to match previous logic explicitly:
    if event_name == "BoxBarcodeValidation":
         content["Dimensions"].append({"TimeStamp": str(int(datetime.now().timestamp() * 1000)), "Type": "VARCHAR"})

    return content


def create_timestream_failure_content(
    source: str,
    source_device: str,
    transaction_id: str,
    batch_name: str,
    pipeline_mod: str,
    record_count: str,
    error: str = None,
    error_code: str = None,
    tr_account_name: str = "-",
    item_code: str = None,
    file_path: str = None,
    tr_customer_id: str = None,
    event_name: str = None
) -> dict:
    """
    Build a Timestream payload for failure reporting.

    Args:
        source (str): Source identifier.
        source_device (str): Device/Pipeline name.
        transaction_id (str): Transaction identifier.
        batch_name (str): Batch identifier.
        pipeline_mod (str): Pipeline module name.
        record_count (str): Affected record count.
        error (str): Optional error message.
        error_code (str): Optional error code.
        tr_account_name (str): Optional TR account name.
        item_code (str): Optional item code.
        tr_customer_id (str): Optional TR customer ID.
        event_name (str): Optional event name override.

    Returns:
        dict: Payload for Timestream insertion.
    """
    content = {
        "Dimensions": [
            {"Source": source, "Type": "VARCHAR"},
            {"PipelineName": source_device, "Type": "VARCHAR"},
            {"TRAccountNo": str(tr_customer_id) if tr_customer_id else tr_account_name, "Type": "VARCHAR"},
            {"TransactionId": transaction_id, "Type": "VARCHAR"},
            {
                "TimeStamp": str(int(datetime.now().timestamp() * 1000)),
                "Type": "VARCHAR",
            },
            {"BatchName": batch_name, "Type": "VARCHAR"},
        ],
        "Measures": [
            {"PipelineModule": pipeline_mod, "Type": "VARCHAR"},
            {"State": "Failed", "Type": "VARCHAR"},
            {"NoOfAsset": record_count, "Type": "BIGINT"},
        ],
    }

    if error:
        content["Dimensions"].append({"Error": error, "Type": "VARCHAR"})

    if error_code:
        content["Dimensions"].append({"ErrorCode": error_code, "Type": "VARCHAR"})
    
    if item_code:
        content["Dimensions"].append({"ItemCode": item_code, "Type": "VARCHAR"})

    if file_path:
        content["Dimensions"].append({"FilePath": file_path, "Type": "VARCHAR"})
        
    if event_name:
        content["Measures"].append({"EventName": event_name, "Type": "VARCHAR"})

    return content
