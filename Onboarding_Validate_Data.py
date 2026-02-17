import sys
import pandas as pd
import Onboarding_Timestream_Manager
import logging as log
import Onboarding_Timestream_Manager
import pandas as pd
import time
import json
import re
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from Onboarding_Glue_Job_Base import GlueJobBase, JobVariables, WorkflowParams
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
from Onboarding_OpsIQ_Manager import OpsIQMessageManager
from typing import Callable
from pyspark.sql.types import StructType, StructField, StringType
from botocore.client import BaseClient
from functools import wraps
from Onboarding_Configuration_Template import (
    Transaction,
    S3Location,
    GlueCatalog,
    ConfigurationTemplate,
    ConfigurationNotFound
)

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 200)


def log_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        log.info(f"Starting the execution of {func.__name__} function\n")
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            log.info(f"Finishing the execution of {func.__name__} function\n")
    return wrapper



class DocumentValidator:
    """
    Validate extracted document fields against a configuration template.

    The template defines per-field schemas including type, validation rules,
    and whether null values are allowed. Validation results are returned as a
    list of per-field dictionaries with error messages when rules fail.
    """

    def __init__(self, validation_template: list):
        """
        Initialize the validator with the validation template.

        Args:
            validation_template (list): List of field schema dictionaries that
                include FieldName, Type, and Validation rules.

        Returns:
            None
        """
        self.field_schemas = {item["FieldName"]: item for item in validation_template}

    @staticmethod
    @log_execution
    def validate_min_lenght(result: dict, value_str: str, validation_rules: dict) -> dict:
        """
        Validate the MinLength rule for a string value.

        Args:
            result (dict): Current validation result to update.
            value_str (str): Normalized string value to validate.
            validation_rules (dict): Validation rule set for the field.

        Returns:
            dict: Updated validation result with possible errors added.
        """
        min_length = validation_rules.get("MinLength")

        if min_length is not None and min_length != "":
            try:
                min_length_int = int(min_length)
                if len(value_str) < min_length_int:
                    result["IsValid"] = False
                    result["ValidationErrors"].append(
                        f"Length '{len(value_str)}' < MinLength '{min_length_int}'"
                    )
            except Exception as e:
                # Case where min_length cannot be converted to int
                log.error(f"Unexpected error parsing the min_length value...\n{str(e)}")
                result["IsValid"] = False
                result["ValidationErrors"].append("Invalid MinLength configuration")
        return result

    @staticmethod
    @log_execution
    def validate_max_lenght(result: dict, value_str: str, validation_rules: dict) -> dict:
        """
        Validate the MaxLength rule for a string value.

        Args:
            result (dict): Current validation result to update.
            value_str (str): Normalized string value to validate.
            validation_rules (dict): Validation rule set for the field.

        Returns:
            dict: Updated validation result with possible errors added.
        """
        max_length = validation_rules.get("MaxLength")

        if max_length is not None and max_length != "":
            try:
                max_length_int = int(max_length)
                if len(value_str) > max_length_int:
                    result["IsValid"] = False
                    result["ValidationErrors"].append(
                        f"Length '{len(value_str)}' > MaxLength '{max_length_int}'"
                    )
            except Exception as e:
                # Case where max_length cannot be converted to int
                log.error(f"Unexpected error parsing the max_length value...\n{str(e)}")
                result["IsValid"] = False
                result["ValidationErrors"].append("Invalid MaxLength configuration")
        return result

    @staticmethod
    @log_execution
    def validate_pattern(result: dict, value_str: str, validation_rules: dict) -> dict:
        """
        Validate a string value against a regex pattern rule.

        Args:
            result (dict): Current validation result to update.
            value_str (str): Normalized string value to validate.
            validation_rules (dict): Validation rule set for the field.

        Returns:
            dict: Updated validation result with possible errors added.
        """
        pattern = validation_rules.get("Pattern", "")
        if pattern and not re.match(pattern, value_str):
            result["IsValid"] = False
            result["ValidationErrors"].append("Pattern validation failed")
        return result

    @staticmethod
    @log_execution
    def special_chars_not_allowed(result: dict, value_str: str, all_special_chars: str) -> dict:
        """
        Fail validation if any disallowed special characters are found in the value.

        Args:
            result (dict): Current validation result to update.
            value_str (str): Normalized string value to validate.
            all_special_chars (str): String containing all disallowed chars.

        Returns:
            dict: Updated validation result with possible errors added.
        """
        forbidden_chars_pattern = "".join(
            [re.escape(char) if char != " " else r"\s" for char in all_special_chars]
        )

        if re.search(f"[{forbidden_chars_pattern}]", value_str):

            forbidden_chars_found = set()

            for char in value_str:
                if char in all_special_chars:
                    forbidden_chars_found.add(char)

            result["IsValid"] = False
            result["ValidationErrors"].append(
                f"Special characters not allowed: {', '.join(forbidden_chars_found)}"
            )
        return result

    @staticmethod
    @log_execution
    def check_allowed_special_chars(
        result: dict, value_str: str, all_special_chars: str, special_chars_allowed: str
    ) -> dict:
        """
        Validate that only a configured subset of special characters is present.

        Args:
            result (dict): Current validation result to update.
            value_str (str): Normalized string value to validate.
            all_special_chars (str): String containing all possible special chars.
            special_chars_allowed (str): Allowed special characters string.

        Returns:
            dict: Updated validation result with possible errors added.
        """
        forbidden_chars_pattern = "".join(
            [
                re.escape(char) if char != " " else r"\s"
                for char in all_special_chars if char not in special_chars_allowed
            ]
        )

        if forbidden_chars_pattern and re.search(f"[{forbidden_chars_pattern}]", value_str):

            forbidden_chars_found = set()

            for char in value_str:
                if char in all_special_chars and char not in special_chars_allowed:
                    forbidden_chars_found.add(char)

            result["IsValid"] = False
            result["ValidationErrors"].append(
                f"Special characters not allowed: {', '.join(forbidden_chars_found)}"
            )
        return result

    @staticmethod
    @log_execution
    def validate_special_characters(result: dict, value_str: str, validation_rules: dict) -> dict:
        """
        Validate a string value against special-character rules.

        Rules supported:
        - AllowSpecialChars = False: no special characters allowed.
        - AllowSpecialChars = True: all special characters allowed.
        - AllowSpecialChars = "<subset>": only the given subset is allowed.

        Args:
            result (dict): Current validation result to update.
            value_str (str): Normalized string value to validate.
            validation_rules (dict): Validation rule set for the field.

        Returns:
            dict: Updated validation result with possible errors added.
        """
        special_chars_allowed = validation_rules.get('AllowSpecialChars', False)
        all_special_chars = '!@#$%^&*()-_=+[]{};:\'",.’<>/? '

        if special_chars_allowed is False:
            result = DocumentValidator.special_chars_not_allowed(
                result, value_str, all_special_chars
            )
        elif special_chars_allowed is True:
            pass
        else:
            result = DocumentValidator.check_allowed_special_chars(
                result, value_str, all_special_chars, special_chars_allowed
            )
        return result

    @log_execution
    def validate_string_field(self, field_name: str, value: str, validation_rules: dict) -> dict:
        """
        Validate a string field against template rules.

        This method validates:
        - AllowNull rule (for empty or missing values)
        - MinLength and MaxLength rules
        - Regex Pattern rule
        - Special character rules

        Args:
            field_name (str): Field name to validate.
            value (str): Extracted value to validate.
            validation_rules (dict): Validation rule set for the field.

        Returns:
            dict: Field validation result with IsValid and ValidationErrors.
        """

        result = {
            "FieldName": field_name,
            "ValueScanned": value,
            "IsValid": True,
            "ValidationErrors": []
        }

        # Handle null values
        if value is None or value == "" or str(value).strip() == "":
            if validation_rules.get("AllowNull", False) is False:
                result["IsValid"] = False
                result["ValidationErrors"].append("Null value not allowed")
            return result

        value_str = str(value).strip()

        result = DocumentValidator.validate_min_lenght(result, value_str, validation_rules)
        result = DocumentValidator.validate_max_lenght(result, value_str, validation_rules)
        result = DocumentValidator.validate_pattern(result, value_str, validation_rules)
        result = DocumentValidator.validate_special_characters(result, value_str, validation_rules)
        return result

    @staticmethod
    @log_execution
    def parse_date_value(
        result: dict, value_str: str, validation_rules: dict
    ) -> tuple[dict, datetime.date, str]:
        """
        Parse a date string using the configured template date format.

        Args:
            result (dict): Current validation result to update.
            value_str (str): Normalized date string to parse.
            validation_rules (dict): Validation rules for the date field.

        Returns:
            tuple[dict, datetime.date, str]: Updated result, parsed date, and the python format
            string.
        """
        date_format = validation_rules.get("Format", "mm/dd/yyyy")
        format_mapping = {
            "mm/dd/yyyy": "%m/%d/%Y",
            "dd/mm/yyyy": "%d/%m/%Y",
            "yyyy-mm-dd": "%Y-%m-%d"
        }
        python_format = format_mapping.get(date_format, "%m/%d/%Y")

        try:
            parsed_date = datetime.strptime(value_str, python_format).date()
        except ValueError:
            result["IsValid"] = False
            error_msg = f"Invalid date format. Expected: '{date_format}', got: '{value_str}'"
            result["ValidationErrors"].append(error_msg)
        return result, parsed_date, python_format

    @staticmethod
    @log_execution
    def validate_min_date(
        result: dict,
        parsed_date: datetime.date,
        field_name: str,
        validation_rules: dict,
        utc_minus_5: timezone,
        python_format: str
    ) -> dict:
        """
        Validate that a parsed date is not earlier than MinDate.

        Args:
            result (dict): Current validation result to update.
            parsed_date (datetime.date): Parsed date value to validate.
            field_name (str): Field name to validate (for logging context).
            validation_rules (dict): Validation rules that may include MinDate.
            utc_minus_5 (timezone): Timezone used to resolve 'today'.
            python_format (str): Python strptime format string.

        Returns:
            dict: Updated validation result with possible errors added.
        """
        min_date_str = validation_rules.get("MinDate")
        if min_date_str:
            try:
                if min_date_str.lower() == "today":
                    min_date = datetime.now(utc_minus_5).date()

                else:
                    min_date = datetime.strptime(min_date_str, python_format).date()

                if parsed_date < min_date:
                    result["IsValid"] = False
                    result["ValidationErrors"].append(f"Date before MinDate '{min_date_str}'")
            except ValueError:
                log.info(f"Invalid MinDate format in template for field '{field_name}'")
        return result

    @log_execution
    def validate_max_date(
        result: dict,
        parsed_date: datetime.date,
        field_name: str,
        validation_rules: dict,
        utc_minus_5: timezone,
        python_format: str
    ) -> dict:
        """
        Validate that a parsed date is not later than MaxDate.

        Args:
            result (dict): Current validation result to update.
            parsed_date (datetime.date): Parsed date value to validate.
            field_name (str): Field name to validate (for logging context).
            validation_rules (dict): Validation rules that may include MaxDate.
            utc_minus_5 (timezone): Timezone used to resolve 'today'.
            python_format (str): Python strptime format string.

        Returns:
            dict: Updated validation result with possible errors added.
        """
        max_date_str = validation_rules.get("MaxDate")
        if max_date_str:
            try:
                if max_date_str.lower() == "today":
                    max_date = datetime.now(utc_minus_5).date()

                else:
                    max_date = datetime.strptime(max_date_str, python_format).date()

                if parsed_date > max_date:
                    result["IsValid"] = False
                    result["ValidationErrors"].append(f"Date after MaxDate {max_date_str}")

            except ValueError:
                log.info(f"Invalid MaxDate format in template for field '{field_name}'")
        return result

    @log_execution
    def validate_date_field(self, field_name, value, validation_rules):
        """
        Validate a date field against template rules.

        This method validates:
        - AllowNull rule (for empty or missing values)
        - Date parsing based on the configured Format
        - MinDate and MaxDate constraints (supports 'today')

        Args:
            field_name (str): Field name to validate.
            value (Any): Extracted value to validate.
            validation_rules (dict): Validation rule set for the field.

        Returns:
            dict: Field validation result with IsValid and ValidationErrors.
        """

        result = {
            "FieldName": field_name,
            "ValueScanned": value,
            "IsValid": True,
            "ValidationErrors": []
        }

        # Handle null values
        if value is None or value == "" or str(value).strip() == "":
            if validation_rules.get("AllowNull", False) is False:
                result["IsValid"] = False
                result["ValidationErrors"].append("Null value not allowed")
            return result

        value_str = str(value).strip()

        result, parsed_date, python_format = DocumentValidator.parse_date_value(
            result, value_str, validation_rules
        )

        if result.get("IsValid") is False:
            return result

        utc_minus_5 = timezone(timedelta(hours=-5))
        result = DocumentValidator.validate_min_date(
            result, parsed_date, field_name, validation_rules, utc_minus_5, python_format
        )
        result = DocumentValidator.validate_max_date(
            result, parsed_date, field_name, validation_rules, utc_minus_5, python_format
        )
        return result

    @log_execution
    def validate_field(self, field_name, value, score=None, score_threshold = 80.0):
        """
        Validate a single field including score-based validation.

        Validation order:
        - Score threshold check for non-null values.
        - AllowNull check independent of score.
        - Business rule validation based on field Type and Validation rules.

        Args:
            field_name (str): Field name to validate.
            value (Any): Extracted field value.
            score (float | int | None): Confidence score for the field value.
            score_threshold (float): Minimum acceptable confidence score.

        Returns:
            dict: Field validation result with IsValid and ValidationErrors.
        """

        field_schema = self.field_schemas[field_name]
        allow_null =  field_schema.get("Validation", {}).get("AllowNull", False)

        if score < score_threshold and value is not None:  # allow_null flag independent
            return {
                "FieldName": field_name,
                "ValueScanned": value,
                "IsValid": False,
                "ValidationErrors": [
                    f"Not null value has a score '{score}' below threshold '{score_threshold}'"
                ]
            }
        elif allow_null is False and value is None:  # Score independent

            return {
                "FieldName": field_name,
                "ValueScanned": value,
                "IsValid": False,
                "ValidationErrors": ["Null values are not allowed for this field"]
            }

        # Business rules validation
        field_type = field_schema.get("Type", "string")
        validation_rules = field_schema.get("Validation", {})

        if field_type == "date":
            return self.validate_date_field(field_name, value, validation_rules)
        else:
            return self.validate_string_field(field_name, value, validation_rules)

    @log_execution
    def validate_document(self, document: dict, score_threshold: int=80.0):
        """
        Validate a full document against the configured field schemas.

        Each field is validated using its associated confidence score key
        '{FieldName}_s' when available. A result is produced per schema field.

        Args:
            document (dict): Extracted document dictionary containing fields
                and optional score keys in the form '{FieldName}_s'.
            score_threshold (int): Minimum acceptable confidence score.

        Returns:
            list[dict]: List of per-field validation results.
        """

        validation_results = []

        for field_name in self.field_schemas.keys():
            # Get value and score
            value = document.get(field_name)
            score_key = f"{field_name}_s"
            score = document.get(score_key, 0)

            result = self.validate_field(field_name, value, score, score_threshold)
            validation_results.append(result)

        return validation_results


class Validator(ABC):
    """
    Abstract base class for all validation strategies.

    Concrete implementations must provide a validate() method that
    executes the validation workflow.
    """

    @abstractmethod
    def validate(self):
        """
        Execute the validation logic.

        Returns:
            None
        """
        pass


class WithConfigurationDataValidator(Validator):
    """
    Validate enriched documents using a configuration-driven approach.

    This validator:
    - Reads enriched data from the Glue catalog.
    - Applies field-level validation rules defined in the collection configuration.
    - Sends validation results to OpsIQ when required.
    - Writes validation results back to the Glue catalog.
    - Emits operational metrics to Timestream.
    """

    def __init__(
        self,
        job_vars: JobVariables,
        workflow_params: WorkflowParams,
        spark: SparkSession,
        glue_client: BaseClient,
        pipeline_mod: str,
        collection_config: dict,
        tr_account_name: str,
    ) -> None:
        """
        Initialize the configuration-based data validator.

        Args:
            job_vars (JobVariables): Job-level configuration variables.
            workflow_params (WorkflowParams): Workflow runtime parameters.
            spark (SparkSession): Active Spark session.
            glue_client (BaseClient): Boto3 Glue client.
            pipeline_mod (str): Pipeline module identifier.
            collection_config (dict): Collection configuration template.
            tr_account_name (str): TR account name associated with the batch.
        """
        self.job_vars = job_vars
        self.workflow_params = workflow_params
        self.spark = spark
        self.glue_client = glue_client
        self.pipeline_mod = pipeline_mod
        self.collection_config = collection_config
        self.tr_account_name = tr_account_name

    @log_execution
    def get_process_table_from_catalog(self) -> DataFrame:
        """
        Read enriched records for the current batch and transaction from Glue.

        Returns:
            DataFrame: Spark DataFrame containing enriched records.

        Raises:
            SystemExit: If a critical error occurs while querying Glue.
        """
        try:
            catalog_db = self.job_vars.catalog_db
            enrichment_catalog_table = self.job_vars.enrichment_catalog_table
            batch_name = self.workflow_params.batch_name
            transaction_id = self.workflow_params.transaction_id

            query = f"""
            SELECT
                _enrichment_catalog_table.image_path,
                _enrichment_catalog_table.batch_name,
                _enrichment_catalog_table.transaction_id,
                _enrichment_catalog_table.ocr_liquid_text,
                _enrichment_catalog_table.key_value_pairs

            FROM
                `{catalog_db}`.`{enrichment_catalog_table}` AS _enrichment_catalog_table
            WHERE
                _enrichment_catalog_table.batch_name = '{batch_name}' and
                _enrichment_catalog_table.transaction_id = '{transaction_id}'
            """

            log.info(f"The query to be executed is:\n\n{query}\n\n")
            query_result = self.spark.sql(query)

            log.info("Success! Data was read from Glue Catalog.")
            return query_result

        except Exception as e:
            log.info(
                f"Critical error in the 'get_process_table_from_catalog' function querying\n"
                f"the table '{catalog_db}.{enrichment_catalog_table}':\n{str(e)}\n"
            )
            log.info(f"Error type: {type(e).__name__}")
            exit("Extract module - Critical error")

    @log_execution
    def get_pd_df_and_display_info(
        self, spark_df: DataFrame,
        tr_account_name: str
    ) -> pd.DataFrame:
        """
        Convert Spark DataFrame to Pandas and log diagnostic information.

        Also emits a Timestream event indicating readiness for validation.

        Args:
            spark_df (DataFrame): Input Spark DataFrame.
            tr_account_name (str): TR account name for metrics reporting.

        Returns:
            pd.DataFrame: Converted Pandas DataFrame.

        Raises:
            ValueError: If the DataFrame is empty.
        """

        process_df = spark_df.toPandas()
        log.info(
            f"Successfully read data!\n\n"
            f"Shape for input frame:\n'{process_df.shape}'\n\n"
            f"First 10 rows:\n{process_df.head(10).to_string()}\n\n"
            f"Input types:\n{process_df.dtypes}\n"
            f"Original columns:\n'{process_df.columns.tolist()}'\n"
        )

        Onboarding_Timestream_Manager.timestream_insert_data(
            db=self.job_vars.timestream_db,
            table=self.job_vars.timestream_table,
            timestream_queue=self.job_vars.timestream_sqs_queue,
            measure_name=f"OnboardingWF_{self.pipeline_mod}",
            content=Onboarding_Timestream_Manager.create_timestream_content(
                source=self.workflow_params.source,
                source_device=self.workflow_params.source_device,
                transaction_id=self.workflow_params.transaction_id,
                batch_name=self.workflow_params.batch_name,
                pipeline_mod=self.pipeline_mod,
                state="Ready",
                no_of_asset=str(len(process_df)),
                tr_account_name=tr_account_name if tr_account_name is not None else "-"
            )
        )

        log.info("Checking if the Pandas Dataframe is empty or not...")

        if process_df.empty:
            raise ValueError("¡No data to write or DataFrame is empty or invalid!...")
        else:
            log.info("Pandas Dataframe is not empty.")

        return process_df

    @staticmethod
    @log_execution
    def format_validation_date_rules(field_list: dict) -> dict:
        """
        Replace 'today' placeholders in date validation rules with actual dates.

        Args:
            field_list (dict): Field validation rules list.

        Returns:
            dict: Updated field list with resolved date values.
        """

        log.info("Extracting the validation template for the customer...")
        utc_minus_5 = timezone(timedelta(hours=-5))

        for dict_struct in field_list:
            if "Validation" in dict_struct:
                validation = dict_struct["Validation"]

                for key, value in validation.items():
                    if isinstance(value, str) and value.lower() == "today":
                        date_format = validation.get("Format", "mm/dd/yyyy")
                        format_mapping = {
                            "mm/dd/yyyy": "%m/%d/%Y",
                            "dd/mm/yyyy": "%d/%m/%Y",
                            "yyyy-mm-dd": "%Y-%m-%d"
                        }

                        python_format = format_mapping.get(date_format, "%m/%d/%Y")
                        validation[key] = datetime.now(utc_minus_5).strftime(python_format)

        log.info(
            f"The validation template parsing all 'Today' occurrences is: "
            f"\n\n{json.dumps(field_list, indent = 4)}\n\n"
        )
        return field_list

    @log_execution
    def get_collection_field_rules(self) -> dict:
        """
        Retrieve and normalize field validation rules for the collection.

        Returns:
            dict: Normalized collection field validation rules.
        """
        collection_fields_rules = ConfigurationTemplate.get_field_list(self.collection_config)
        collection_fields_rules = (
            WithConfigurationDataValidator.format_validation_date_rules(collection_fields_rules)
        )
        return collection_fields_rules

    @staticmethod
    @log_execution
    def safe_document_validation(
        df: pd.DataFrame,
        source_col: str,
        image_col: str,
        new_col: str,
        func: Callable,
        opsiq_message_manager: OpsIQMessageManager,
        score_threshold: int,
        incoming_bucket: str,
        error_value: str
    ) -> pd.DataFrame:
        """
        Validate documents safely and optionally send failures to OpsIQ.

        This method parses the JSON in source_col, validates the document using
        the provided function, and stores a boolean-like string result ("True"/
        "False") in new_col per row. If OpsIQ audit is enabled or any field fails
        validation, an OpsIQ payload is built and queued for sending.

        Args:
            df (pd.DataFrame): Input DataFrame containing extracted key-value data.
            source_col (str): Column containing JSON strings for validation.
            image_col (str): Column containing the image path for OpsIQ context.
            new_col (str): Output column to store validation results ("True"/"False").
            func (Callable): Validation function (e.g., validate_document) that
                returns a list of per-field validation results.
            opsiq_message_manager (OpsIQMessageManager): Manager used to build and
                store OpsIQ payloads for later sending.
            score_threshold (int): Minimum confidence score required for non-null
                values to be considered valid.
            incoming_bucket (str): S3 bucket used to build DocumentImages URIs.
            error_value (str): Fallback value returned when validation fails.

        Returns:
            pd.DataFrame: Updated DataFrame with new_col populated.
        """
        @log_execution
        def safe_json_loads(json_str, default={}):
            """
            Safely parse a JSON string into a dictionary.

            Args:
                json_str (str): JSON string to parse.
                default (dict): Default value returned on parse failure.

            Returns:
                dict: Parsed JSON dict or default on failure.
            """

            if json_str is None or json_str == "":
                log.warning("Empty JSON received...")
                return default

            try:
                return json.loads(json_str)
            except json.JSONDecodeError as e:
                log.error(f"JSON decode error...\n{str(e)}")
                log.error(f"Problematic JSON (first 200 chars):\n{json_str[:200]}")
                return default
            except Exception as e:
                log.error(f"Unexpected error parsing JSON...\n{str(e)}")
                log.error(f"JSON type: '{type(json_str)}',\nlength: '{len(str(json_str))}'")
                return default

        @log_execution
        def build_and_send_opsiq_msg(
            field_results: list, has_validation_errors: bool, image_path: str
        ) -> None:
            """
            Build an OpsIQ message payload and store it in the message manager.

            Args:
                field_results (list): List of per-field validation results.
                has_validation_errors (bool): True if at least one field is invalid.
                image_path (str): Image path associated with the document.

            Returns:
                None
            """
            if opsiq_audit_flag:
                message = "\nThe 'opsiq_audit_flag' is ON, sending this item to OpsIQ...\n"
            else:
                message = "\nAt least one field didn't pass the validation...\n"

            log.info(message)

            global global_var_to_send
            global_var_to_send = True

            fieldlist = [
                {
                    "FieldName": result["FieldName"],
                    "ValueScanned": result["ValueScanned"],
                    "IsValid": result["IsValid"]
                }
                for result in field_results
            ]

            if has_validation_errors:
                status = "Failed"
                error_msg = "Validation Error"
                error_code = "1"
            else:
                status = "Complete"
                error_msg = ""
                error_code = ""

            log.info(
                "The message structure for the failed item was created.\n"
                "Sending the message content to OpsIQ...\n"
            )

            opsiq_message_manager.save_item_message_content(
                ItemCode="",
                Status=status,
                Error=error_msg,
                ErrorCode=error_code,
                EventName="Validation",
                FieldList=fieldlist,
                DocumentImages=["s3://" + incoming_bucket + "/" + image_path]
            )

        @log_execution
        def safe_wrapper(corpus_document_data: dict, image_path: str) -> pd.Series:
            """
            Run document validation with exception handling and OpsIQ reporting.

            Args:
                corpus_document_data (dict): Parsed document data to validate.
                image_path (str): Image path associated with the document.

            Returns:
                pd.Series: ("True"/"False", "True"/"False") or (error_value, error_value).
                First value is flow decision (influenced by audit flag).
                Second value is actual validation result.
            """
            try:
                field_results = func(corpus_document_data, score_threshold)

                log.info(f"Validation results:\n\n{json.dumps(field_results, indent=4)}")

                if field_results is not None:
                    has_validation_errors = any(
                        result["IsValid"] is False
                        for result in field_results
                    )

                    # Actual validation status (ignoring audit flag)
                    actual_validation_status = "False" if has_validation_errors else "True"

                    should_process = opsiq_audit_flag or has_validation_errors

                    if should_process:
                        build_and_send_opsiq_msg(
                            field_results, has_validation_errors, image_path
                        )
                        return pd.Series(["False", actual_validation_status])
                    else:
                        log.info(
                            "\nAll the Keys inside the present image,\n"
                            "passed validation successfully...\n"
                        )
                        return pd.Series(["True", actual_validation_status])
                else:
                    return pd.Series([error_value, error_value])

            except Exception as e:
                log.info(f"Error in the 'validate_document()' function call:\n{str(e)}\n")
                log.info(f"Error type: {type(e).__name__}\n")
                # Possible Timestream call
                return pd.Series([error_value, error_value])

        log.info("Applying the validation functions to all the rows inside the table...\n")

        df[[new_col, "validation_passed"]] = df[[source_col, image_col]].apply(
            lambda x: safe_wrapper(safe_json_loads(x.iloc[0]), x.iloc[1]), axis=1
        )
        return df

    @log_execution
    def pd_df_to_spark(self, process_df: pd.DataFrame) -> DataFrame:
        """
        Convert a Pandas DataFrame into a Spark DataFrame with a string-based schema.

        This method dynamically builds a Spark StructType schema from the Pandas
        DataFrame columns, casting all fields to StringType to ensure compatibility
        with downstream Glue catalog writes.

        Args:
            process_df (pd.DataFrame): Input Pandas DataFrame to be converted.

        Returns:
            DataFrame: Spark DataFrame with all columns defined as nullable strings.
        """
        all_columns = process_df.columns.tolist()

        log.info("Creating the Spark Dataframe out from the Pandas Dataframe...")

        schema = StructType(
            [StructField(column_name, StringType(), nullable=True) for column_name in all_columns]
        )

        final_spark_df = self.spark.createDataFrame(process_df, schema=schema)

        log.info("Spark Dataframe created.")

        return final_spark_df

    @log_execution
    def processed_rows_to_timestream(self, final_sdf: DataFrame, tr_account_name: str) -> None:
        """
        Emit per-row validation results to Timestream.

        This method iterates over the validated Spark DataFrame rows and sends
        one Timestream event per image. Each event includes validation status
        ("IsValid") along with workflow, batch, and transaction metadata.

        Args:
            final_sdf (DataFrame): Spark DataFrame containing validation results,
                including the 'data_is_valid' and 'image_path' columns.
            tr_account_name (str): TR account name associated with the batch. If
                None, a default value '-' is used.

        Returns:
            None
        """
        processed_rows = final_sdf.sort("image_path").collect()
        for row in processed_rows:
            content = Onboarding_Timestream_Manager.create_timestream_content(
                source=self.workflow_params.source,
                source_device=self.workflow_params.source_device,
                transaction_id=self.workflow_params.transaction_id,
                batch_name=self.workflow_params.batch_name,
                pipeline_mod=self.pipeline_mod,
                state="Complete",
                no_of_asset="1",
                file_path=str(row["image_path"]),
                tr_account_name=tr_account_name if tr_account_name is not None else "-",
                event_name="ImageValidation"
            )
            content["Dimensions"].append({"IsValid": str(row["data_is_valid"]), "Type": "VARCHAR"})

            Onboarding_Timestream_Manager.timestream_insert_data(
                db=self.job_vars.timestream_db,
                table=self.job_vars.timestream_table,
                timestream_queue=self.job_vars.timestream_sqs_queue,
                measure_name=f'OnboardingWF_{self.pipeline_mod}',
                content=content
            )

    @staticmethod
    @log_execution
    def get_glue_table_schema(
        glue_client: BaseClient,
        glue_catalog: GlueCatalog
    ) -> list:
        """
        Retrieve the ordered column schema for a Glue catalog table.

        Args:
            glue_client (BaseClient): Boto3 Glue client instance.
            glue_catalog (GlueCatalog): Glue catalog database and table info.

        Returns:
            list: Ordered list of column names from the Glue table schema.
        """
        try:
            log.info(
                "Fetching schema for table:\n"
                f"'{glue_catalog.catalog_db}.{glue_catalog.catalog_table}'"
            )

            response = glue_client.get_table(
                DatabaseName=glue_catalog.catalog_db,
                Name=glue_catalog.catalog_table,
            )
            columns = response["Table"]["StorageDescriptor"]["Columns"]
            partition_keys = response["Table"].get("PartitionKeys", [])
            ordered_columns = [_col["Name"] for _col in columns] + [p["Name"] for p in partition_keys]

            log.info(
                "Retrieved Glue schema order for table\n"
                f"'{glue_catalog.catalog_db}.{glue_catalog.catalog_table}':\n"
                f"{ordered_columns}"
            )
            return ordered_columns

        except Exception as e:
            log.error(
                "Critical error in the 'get_glue_table_schema' function for "
                f"the table '{glue_catalog.catalog_db}."
                f"{glue_catalog.catalog_table}':\n{str(e)}\n"
                f"Error type: {type(e).__name__}"
            )
            exit("ConnectCrawl - Critical error")

    @staticmethod
    @log_execution
    def validate_and_prepare_dataframe(
        result_df: DataFrame,
        expected_columns: list
    ) -> DataFrame:
        """
        Align a Spark DataFrame with an expected column schema.

        This method compares the input DataFrame schema with the expected
        schema, adds any missing columns as null string columns, and returns
        a DataFrame containing only the expected columns in the correct order.

        Args:
            result_df (DataFrame): Input Spark DataFrame to validate.
            expected_columns (list): Ordered list of required column names.

        Returns:
            DataFrame: Spark DataFrame aligned to the expected schema.
        """
        set_expected_cols = set(expected_columns)
        set_input_cols = set(result_df.columns)

        log.info(
            f"set_expected_cols:\n{set_expected_cols}\n"
            f"set_input_cols:\n{set_input_cols}"
        )

        missing_columns = set_expected_cols - set_input_cols
        present_columns = set_expected_cols & set_input_cols
        extra_columns = set_input_cols - set_expected_cols

        log.info(
            f"missing_columns:\n{missing_columns}\n"
            f"present_columns:\n{present_columns}\n"
            f"extra_columns:\n{extra_columns}\n"
        )

        if missing_columns:
            log.warning(
                f"Missing columns in input DataFrame:\n{missing_columns}"
            )

            for col in missing_columns:
                result_df = result_df.withColumn(
                    col,
                    F.lit("null").cast(StringType()),
                )

        else:
            log.info(
                "There are not missing columns in the input dataframe with respect\n"
                "to the required or expected columns."
            )

        return result_df.select(*expected_columns)

    @log_execution
    def write_to_glue_catalog(
        self,
        result_df: DataFrame,
        glue_catalog: GlueCatalog
    ) -> None:
        """
        Validate, align, and write a Spark DataFrame to a Glue catalog table.

        This method retrieves the target table schema from the Glue catalog,
        ensures the input DataFrame matches the expected columns and order,
        and writes the data in Parquet format using overwrite mode.

        Args:
            result_df (DataFrame): Spark DataFrame containing the processed
                key-value extraction results.
            glue_catalog (GlueCatalog): Glue catalog database and table
                destination for the write operation.

        Returns:
            None
        """
        target_db = glue_catalog.catalog_db
        target_table_name = glue_catalog.catalog_table

        log.info(f"Writing to Glue: '{target_db}.{target_table_name}'")

        expected_columns = WithConfigurationDataValidator.get_glue_table_schema(
            self.glue_client,
            glue_catalog,
        )
        prepared_df = WithConfigurationDataValidator.validate_and_prepare_dataframe(
            result_df,
            expected_columns,
        )

        log.info(
            f"Writing '{prepared_df.count()}' records to\n"
            f"'{target_db}.{target_table_name}'"
        )
        log.info(f"Final columns are:\n{prepared_df.columns}")

        (
            prepared_df.coalesce(1)
            .write.mode("overwrite")
            .format("parquet")
            .insertInto(f"`{target_db}`.`{target_table_name}`")
        )

        log.info("Successfully wrote to Glue catalog")

    @log_execution
    def log_transactions_counts(self, result_df: DataFrame, tr_account_name: str) -> None:
        """
        Compute and publish validation result counts to logs and Timestream.

        This method calculates:
        - Total number of processed records.
        - Number of valid records where data_is_valid == "True".
        - Number of invalid records where data_is_valid == "False".

        It then emits multiple Timestream events:
        - Total record count for the module.
        - Valid transaction count (event_name="ValidTransactionCount").
        - Invalid transaction count (event_name="InvalidTransactionCount").

        Args:
            result_df (DataFrame): Spark DataFrame containing validation results,
                including the 'data_is_valid' column.
            tr_account_name (str): TR account name associated with the batch. If
                None, a default value '-' is used.

        Returns:
            None
        """
        final_record_count = result_df.count()
        tr_account_name = tr_account_name if tr_account_name is not None else "-"
        result_df.select("data_is_valid").distinct().show(vertical=True, truncate=False)

        # Using validation_passed column to count based on actual validation result
        # ignoring if the item was audited (sent to OpsIQ) or not.
        valid_count = result_df.filter(result_df.validation_passed == "True").count()
        invalid_count = result_df.filter(result_df.validation_passed == "False").count()

        log.info(f"Records Written in the ValidateData Module: {final_record_count}")
        Onboarding_Timestream_Manager.timestream_insert_data(
            db=self.job_vars.timestream_db,
            table=self.job_vars.timestream_table,
            timestream_queue=self.job_vars.timestream_sqs_queue,
            measure_name=f'OnboardingWF_{self.pipeline_mod}',
            content=Onboarding_Timestream_Manager.create_timestream_content(
                source=self.workflow_params.source,
                source_device=self.workflow_params.source_device,
                transaction_id=self.workflow_params.transaction_id,
                batch_name=self.workflow_params.batch_name,
                pipeline_mod=self.pipeline_mod,
                state="Complete",
                no_of_asset=str(final_record_count),
                tr_account_name=tr_account_name
            )
        )

        # Log Valid Transaction Count
        Onboarding_Timestream_Manager.timestream_insert_data(
            db=self.job_vars.timestream_db,
            table=self.job_vars.timestream_table,
            timestream_queue=self.job_vars.timestream_sqs_queue,
            measure_name=f"OnboardingWF_{self.pipeline_mod}",
            content=Onboarding_Timestream_Manager.create_timestream_content(
                source=self.workflow_params.source,
                source_device=self.workflow_params.source_device,
                transaction_id=self.workflow_params.transaction_id,
                batch_name=self.workflow_params.batch_name,
                pipeline_mod=self.pipeline_mod,
                state="Complete",
                no_of_asset=str(valid_count),
                tr_account_name=tr_account_name,
                event_name="ValidTransactionCount"
            )
        )

        # Log Invalid Transaction Count
        Onboarding_Timestream_Manager.timestream_insert_data(
            db=self.job_vars.timestream_db,
            table=self.job_vars.timestream_table,
            timestream_queue=self.job_vars.timestream_sqs_queue,
            measure_name=f"OnboardingWF_{self.pipeline_mod}",
            content=Onboarding_Timestream_Manager.create_timestream_content(
                source=self.workflow_params.source,
                source_device=self.workflow_params.source_device,
                transaction_id=self.workflow_params.transaction_id,
                batch_name=self.workflow_params.batch_name,
                pipeline_mod=self.pipeline_mod,
                state="Complete",
                no_of_asset=str(invalid_count),
                tr_account_name=tr_account_name,
                event_name="InvalidTransactionCount"
            )
        )

    @log_execution
    def validate(self) -> None:
        """
        Execute the configuration-driven validation workflow for enriched documents.

        Returns:
            None

        Raises:
            Exception: Re-raises any exception after logging critical details.
        """
        try:
            global opsiq_audit_flag
            opsiq_audit_flag = ConfigurationTemplate.get_audit_flag(self.collection_config)

            log.info(f"OpsIQ audit flag (send_all_to_opsiq) is: {opsiq_audit_flag}")

            input_df = self.get_process_table_from_catalog()
            process_df = self.get_pd_df_and_display_info(
                input_df, self.tr_account_name
            )

            log.info("\nThe process execution timer has started...\n")
            total_start_time = time.time()

            collection_fields_rules = self.get_collection_field_rules()
            opsiq_message_manager = OpsIQMessageManager(
                self.job_vars.opsiq_queue,
                self.workflow_params.batch_name,
                collection_fields_rules,
                self.workflow_params.transaction_id,
                "opsiq",
                self.workflow_params.source,
                self.workflow_params.incoming_bucket,
                self.collection_config
            )
            validator = DocumentValidator(collection_fields_rules)
            process_df = WithConfigurationDataValidator.safe_document_validation(
                df=process_df,
                source_col="key_value_pairs",
                image_col="image_path",
                new_col="data_is_valid",
                func=validator.validate_document,
                opsiq_message_manager=opsiq_message_manager,
                score_threshold=80,
                incoming_bucket=self.workflow_params.incoming_bucket,
                error_value="Could Not be Processed"
            )
            total_time = time.time() - total_start_time

            log.info(
                "\nThe validator function was applied to all targeted corpus documents...\n"
                f"\nTotal process time for all corpus document items is:\n"
                f"'{total_time:.4f}' seconds\n"
            )
            log.info(
                f"Showing the first 10 rows out of the processed dataframe:"
                f"\n{process_df.head(10)}\n\n"
            )
            result_df = self.pd_df_to_spark(process_df)
            self.processed_rows_to_timestream(result_df, self.tr_account_name)
            enrichment_catalog_data = GlueCatalog(
                self.job_vars.catalog_db, self.job_vars.enrichment_catalog_table
            )
            self.write_to_glue_catalog(result_df, enrichment_catalog_data)
            self.log_transactions_counts(result_df, self.tr_account_name)
        except Exception as e:
            log.error(f"Critical error in the 'validate' function:\n{str(e)}\n")
            log.error(f"Error type: {type(e).__name__}")
            raise
        finally:
            if global_var_to_send is True:
                opsiq_message_manager.send_opsiq_message()


class WithoutConfigurationDataValidator(Validator):
    """
    No-op validator used when no collection configuration is available.

    This implementation intentionally skips the validation step and returns
    immediately. It is selected by the factory when the source requires
    validation but configuration data cannot be found.
    """

    def validate(self) -> None:
        """
        Skip validation execution.

        Returns:
            None
        """
        log.warning("Skipping Validation Module execution")
        return


class FactoryValidator:
    """
    Factory to select the appropriate Validator implementation.

    The selection is based on:
    - The workflow zip key / source.
    - Whether collection configuration data can be retrieved from the catalog.
    """

    @staticmethod
    def get_configuration(
        workflow_params: WorkflowParams, job_vars: JobVariables, spark: SparkSession
    ) -> tuple[str, dict]:
        """
        Retrieve collection configuration for the current transaction and batch.

        This method queries configuration data from a Glue catalog table
        and returns the TR account name, the collection configuration, and a flag indicating
        whether the configuration exists.

        Args:
            workflow_params (WorkflowParams): Workflow runtime parameters.
            job_vars (JobVariables): Job-level configuration variables.
            spark (SparkSession): Active Spark session.

        Returns:
            tuple[str, dict, bool]: (tr_account_name, collection_config, has_conf)
            - tr_account_name: TR account name resolved from configuration.
            - collection_config: Collection configuration dictionary.
            - has_conf: True if configuration was found, otherwise False.

        Raises:
            Exception: If an unexpected error occurs while retrieving config.
        """
        collection_config = {}
        try:
            location = S3Location(
                workflow_params.incoming_bucket, "Onb_Customer_Conf/", ""
            )
            radix_catalog = GlueCatalog(
                job_vars.catalog_db, job_vars.incoming_catalog_table
            )
            transaction = Transaction(
                workflow_params.transaction_id, workflow_params.batch_name
            )
            tr_account_name, collection_config = (
                ConfigurationTemplate.get_collection_config_from_catalog(
                    location, radix_catalog, transaction, spark, log
                )
            )
            collection_config.pop("tr_customer_id")
            return tr_account_name, collection_config, True
        except ConfigurationNotFound as cnf:
            log.warning(f"Collection configuration not found: {str(cnf)}")
            return "", {}, False
        except Exception as e:
            log.error(
                f"Error retrieving configuration data: {str(e)}"
            )
            raise

    @staticmethod
    def get_data_validator(
        source: str,
        job_vars: JobVariables,
        workflow_params: WorkflowParams,
        spark: SparkSession,
        glue_client: BaseClient,
        pipeline_mod: str,
    ) -> Validator:
        """
        Select and build the correct Validator based on source and configuration.

        Args:
            source (str): Source identifier for the run (kept for API symmetry).
            job_vars (JobVariables): Job-level configuration variables.
            workflow_params (WorkflowParams): Workflow runtime parameters.
            spark (SparkSession): Active Spark session.
            glue_client (BaseClient): Boto3 Glue client.
            pipeline_mod (str): Pipeline module identifier.

        Returns:
            Validator: A concrete Validator implementation.

        Raises:
            ValueError: If the source/zip_key is not supported by the factory.
        """
        tr_account_name, collection_config, has_conf = FactoryValidator.get_configuration(
            workflow_params, job_vars, spark
        )
        zip_key = workflow_params.zip_key
        log.info("Getting validator...")
        if "radix-onboarding-indexed" in zip_key or ("tr-image-extract" in zip_key and has_conf):
            log.info(
                "Configuration data found for the collection.\n"
                "Proceeding with the Validator that uses configuration data...\n"
            )
            return WithConfigurationDataValidator(
                job_vars=job_vars,
                workflow_params=workflow_params,
                spark=spark,
                glue_client=glue_client,
                pipeline_mod=pipeline_mod,
                collection_config=collection_config,
                tr_account_name=tr_account_name,
            )
        elif "tr-image-extract" in zip_key and not has_conf:
            log.info(
                "Configuration data not found for the collection.\n"
                "Proceeding with the Validator that does not use configuration data...\n"
            )
            return WithoutConfigurationDataValidator()
        else:
            raise ValueError(f"Unsupported zip_key type: {zip_key}")


class GlueJobExecution(GlueJobBase):
    def __init__(self, pipeline_mod: str):
        """
        Initialize the Glue job execution for data validation.

        Args:
            pipeline_mod (str): Name of the pipeline module executing the job.

        Returns:
            None
        """
        super().__init__(pipeline_mod)

    def main(self):
        try:
            args = getResolvedOptions(
                sys.argv,
                [
                    "JOB_NAME",
                    "WORKFLOW_NAME",
                    "WORKFLOW_RUN_ID",
                    "CatalogDB",
                    "IncomingCatalogTable",
                    "EnrichmentCatalogTable",
                    "OpsIQQueue",
                    "BaseApi",
                    "SecretManagerName",
                    "RegionName",
                    "TimestreamDB",
                    "TimestreamTable",
                    "TimestreamSQSQueue"
                ],
            )

            job_vars = JobVariables(args)

            self.job.init(job_vars.job_name, args)

            self.log.info(f"Job Variables:\n{job_vars}")

            workflow_params = self.get_workflow_params(job_vars)
            self.log.info(f"Workflow params:\n{workflow_params}")

            global global_var_to_send
            global_var_to_send = False

            validator = FactoryValidator.get_data_validator(
                source=workflow_params.source,
                job_vars=job_vars,
                workflow_params=workflow_params,
                spark=self.spark,
                glue_client=self.glue_client,
                pipeline_mod=self.pipeline_mod,
            )
            validator.validate()

            self.log.info("Job completed successfully")
        except Exception as e:
            self.log.error(f"Critical error in the 'main' function:\n{str(e)}")
            self.log.error(f"Error type: {type(e).__name__}")
            Onboarding_Timestream_Manager.timestream_insert_data(
                db=job_vars.timestream_db,
                table=job_vars.timestream_table,
                timestream_queue=job_vars.timestream_sqs_queue,
                measure_name=f"OnboardingWF_{self.pipeline_mod}",
                content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                    source=workflow_params.source,
                    source_device=workflow_params.source_device,
                    transaction_id=workflow_params.transaction_id,
                    batch_name=workflow_params.batch_name,
                    pipeline_mod=self.pipeline_mod,
                    record_count="0",
                    error=str(e),
                    error_code="42",
                    tr_account_name="-"
                )
            )
            raise RuntimeError("General process failed") from e
        finally:
            self.job.commit()


if __name__ == "__main__":
    glue_job = GlueJobExecution(pipeline_mod="ValidateData")
    glue_job.main()
