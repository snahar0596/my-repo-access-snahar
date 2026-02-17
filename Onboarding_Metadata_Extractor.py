import requests
import logging
import pandas as pd
import re
import uuid
import hashlib
import json
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from Onboarding_Configuration_Template import S3Location, ConfigurationTemplate, GlueCatalog
from abc import ABC, abstractmethod
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from Onboarding_OpsIQ_Manager import OpsIQMessageManager
import Onboarding_Timestream_Manager


class TRCredentials():
    def __init__(
        self,
        tr_secret_keys: dict,
        tr_base_api: str
    ) -> None:
        """
        Initialize credentials required to authenticate against the TR API.

        Args:
            tr_secret_keys (dict): Dictionary containing authentication keys
                such as grant type, username, and password.
            tr_base_api (str): Base URL of the TR API.

        Returns:
            None
        """
        self.tr_grant_type = tr_secret_keys.get("grant_type", "")
        self.tr_username = tr_secret_keys.get("username", "")
        self.tr_password = tr_secret_keys.get("password", "")
        self.tr_base_api = tr_base_api


class MetadataExtractor(ABC):
    @abstractmethod
    def extract_metadata(self, location: S3Location):
        pass


class CsvNoItemExtractor(MetadataExtractor):
    def __init__(
        self,
        source_file_name : str,
        source: str,
        location: S3Location,
        tr_credentials: TRCredentials,
        s3_client: BaseClient,
        glue_client: BaseClient,
        spark: SparkSession,
        batch_name: str,
        transaction_id: str,
        workflow_name: str,
        workflow_run_id: str,
        opsiq_queue: str = None,
        filter_parameters: dict = None,
        timestream_config: dict = None,
    ):
        """
        Initialize a CSV metadata extractor for batches without item records.

        Args:
            source_file_name (str): Source identifier for the incoming data.
            source (str): Source identifier for the the pipeline.
            location (S3Location): S3 bucket/prefix where loadfile.csv lives.
            tr_credentials (TRCredentials): Credentials used to call TR APIs.
            s3_client (BaseClient): Boto3 S3 client used to read S3 objects.
            glue_client (BaseClient): Boto3 Glue client to set workflow props.
            spark (SparkSession): Spark session used to build Spark DataFrames.
            batch_name (str): Batch identifier for the incoming data.
            transaction_id (str): Transaction identifier for the batch.
            workflow_name (str): Glue workflow name to update run properties.
            workflow_run_id (str): Glue workflow run id to update properties.
            filter_parameters (dict): Optional filter parameters to update data.
            timestream_config (dict): Timestream settings for error logging.

        Returns:
            None
        """
        super().__init__()
        self.source_file_name = source_file_name
        self.source = source
        self.bucket_name = location.bucket
        self.target_prefix = location.prefix
        self.s3_client = s3_client
        self.glue_client = glue_client
        self.loadfile_path = (
            f"s3://{self.bucket_name}/{self.target_prefix}loadfile.csv"
        )
        self.batch_name = batch_name
        self.transaction_id = transaction_id
        self.tr_credentials = tr_credentials
        self.spark = spark
        self.workflow_name = workflow_name
        self.workflow_run_id = workflow_run_id
        self.opsiq_queue = opsiq_queue
        self.filter_parameters = filter_parameters
        self.timestream_config = timestream_config

    def load_file_exists(self) -> bool:
        """
        Check whether loadfile.csv exists under the configured S3 prefix.

        Args:
            None

        Returns:
            bool: True if loadfile.csv exists in S3, otherwise False.
        """
        load_file_key = self.target_prefix + "loadfile.csv"
        try:
            self.s3_client.get_object(Bucket=self.bucket_name, Key=load_file_key)
            logging.info(f"loadfile.csv exist in: {self.loadfile_path}")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                message = f"The object '{load_file_key}' was not found."
            else:
                message = f"Unexpected error occurred: {e}"
            logging.error(f"{message}")
            return False
        except Exception as e:
            logging.error(
                f"An error occurred while checking for loadfile.csv: {e}"
            )
            return False

    def apply_filter_parameters(self, loadfile_df: pd.DataFrame) -> None:
        """
        Apply filter parameters to update the DataFrame in place.

        Args:
            loadfile_df (pd.DataFrame): The DataFrame to update.

        Returns:
            None
        """
        if not self.filter_parameters:
            return

        batch_params = self.filter_parameters.get(self.batch_name)
        if not batch_params:
            logging.info(f"No filter parameters found for batch: {self.batch_name}")
            return

        box_barcode = batch_params.get("box_barcode")
        if box_barcode:
            logging.info(f"Updating Box Barcode to '{box_barcode}' from filter parameters.")
            loadfile_df["Box Barcode"] = box_barcode

    def read_csv_loadfile(self) -> pd.DataFrame:
        """
        Read loadfile.csv from S3 into a Pandas DataFrame.

        Args:
            None

        Returns:
            pd.DataFrame: DataFrame containing the parsed CSV content.

        Raises:
            ValueError: If the resulting DataFrame is empty.
        """
        logging.info(
            "The incoming data to process is:\n"
            f"batch_name: '{self.batch_name}'\n"
            f"transaction_id: '{self.transaction_id}'\n"
            f"loadfile_path: '{self.loadfile_path}'\n\n"
            "reading the CSV file..."
        )

        loadfile_df = pd.read_csv(
            self.loadfile_path,
            dtype=str,
            sep="|",
            quotechar='"',
            escapechar="~",
            doublequote=True,
            on_bad_lines="warn",
        )

        frame_shape = loadfile_df.shape

        logging.info(
            f"Successfully read data!\n\n"
            f"Shape for input frame:\n{frame_shape}\n\n"
            f"First 10 rows:\n{loadfile_df.head(10).to_string()}\n\n"
            f"Input types:\n{loadfile_df.dtypes}\n"
            f"Original columns:\n{loadfile_df.columns.tolist()}\n\n"
            "Checking if the Pandas Dataframe is empty or not..."
        )

        if loadfile_df.empty:
            raise ValueError("¡No data to write or DataFrame is empty or invalid!...")

        logging.info("Pandas Dataframe is not empty.")
        return loadfile_df

    def ensure_uniqueness(self, loadfile_df: pd.DataFrame) -> tuple[str, str, str]:
        """
        Validate batch-level uniqueness constraints in the loadfile DataFrame.

        Args:
            loadfile_df (pd.DataFrame): Input DataFrame read from loadfile.csv.

        Returns:
            tuple: (box_barcode, tr_account_name, collection_name) derived from the input data.
        """
        box_barcode_list = loadfile_df["Box Barcode"].unique().tolist()
        tr_account_name = loadfile_df["Collection"].unique().tolist()[0]
        collection_name = loadfile_df["Project"].unique().tolist()[0]

        if len(box_barcode_list) != 1:
            message = "Multiple Box Barcodes present in the batch"
            logging.error(message)
            exit(message)

        box_barcode = box_barcode_list[0]
        logging.info(
            f"Box Barcode is: {box_barcode}"
            f"TR Customer Name is: {tr_account_name}"
            f"Project Name is: {collection_name}"
        )
        return box_barcode, tr_account_name, collection_name

    def get_customer_config(self, tr_account_name: str) -> dict:
        """
        Retrieve the TR customer configuration file for a given account name.

        Args:
            tr_account_name (str): TR account name used to locate the config.

        Returns:
            dict: Parsed customer configuration for the TR account.
        """
        location = S3Location(
            bucket=self.bucket_name,
            prefix="Onb_Customer_Conf/",
            key=f"{tr_account_name}",
        )
        customer_config = ConfigurationTemplate.get_tr_account_config_file(
            location
        )
        logging.info(
            f"Customer configuration for '{tr_account_name}': {customer_config}"
        )
        return customer_config

    def create_tr_access_token(self):
        """
        Create an OAuth access token for authenticating TR API requests.

        Args:
            None

        Returns:
            str: Access token to be used in the Authorization header.

        Raises:
            Exception: If the token creation request fails.
        """
        create_access_token_api = (
            f"https://{self.tr_credentials.tr_base_api}/Token"
        )
        headers = {"Content-Type": "application/json"}
        token_payload = {
            "grant_type": self.tr_credentials.tr_grant_type,
            "username": self.tr_credentials.tr_username,
            "password": self.tr_credentials.tr_password,
        }
        logging.info(create_access_token_api)
        res = requests.post(
            create_access_token_api, data=token_payload, headers=headers
        )
        if res.status_code != 200:
            message = f"Failed to create TR access token: {res.text}"
            logging.error(message)
            raise Exception(message)
        tr_access_token = res.json()["access_token"]
        return tr_access_token

    def get_tr_box_parent_id(
        self,
        tr_customer_id: str,
        box_barcode: str,
        tr_access_token: str
    ) -> bool:
        """
        Check whether a box barcode exists for a given TR customer via TR API.

        Args:
            tr_customer_id (str): TR customer id used to query TR items.
            box_barcode (str): Box barcode to validate for the customer.
            tr_access_token (str): Bearer token used to authenticate the call.

        Returns:
            bool: True if the box barcode is valid, otherwise False.
        """
        logging.info(
            "Calling the TR API to check if the Box Barcode exists for the TR "
            "Customer"
        )
        get_parent_id_api = (
            f"https://{self.tr_credentials.tr_base_api}/api/v1.0/Items/"
            f"{tr_customer_id}/{box_barcode}/GetItemByItemCode"
        )
        headers = {
            "Authorization": f"Bearer {tr_access_token}",
            "Content-Type": "application/json",
        }
        res = requests.get(get_parent_id_api, headers=headers)
        if res.status_code == 200:
            logging.info("Valid Box Barcode for TR Customer")
            return True
        if res.status_code == 404:
            logging.info("Invalid Box Barcode for TR Customer")
            return False
        logging.info(f"Error while calling the API: {res.json}")
        return False

    def validate_box_barcode(self, box_barcode: str, tr_customer_id: str):
        """
        Validate a box barcode for a TR customer by calling TR APIs.

        Args:
            box_barcode (str): Box barcode to validate.
            tr_customer_id (str): TR customer id used to query TR items.

        Returns:
            bool: True if the box barcode is valid, otherwise False.
        """
        logging.info("Creating Token to call TR APIs")
        tr_access_token = self.create_tr_access_token()
        logging.info("Access Token Created")
        return self.get_tr_box_parent_id(
            tr_customer_id, box_barcode, tr_access_token
        )

    @staticmethod
    def convert_columns_to_snake_case(df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert all column names in a pandas DataFrame to snake_case.

        Args:
            df (pd.DataFrame): Input DataFrame whose columns are renamed.

        Returns:
            pd.DataFrame: DataFrame with renamed columns in snake_case.
        """

        def to_snake_case(name: str) -> str:
            # Remove "(Auto Extracted)" (case-insensitive)
            name = re.sub(
                r"\s*\(Auto Extracted\)\s*",
                " ",
                name,
                flags=re.IGNORECASE,
            )

            # Handle camelCase and PascalCase
            name = re.sub(r"([a-z])([A-Z])", r"\1_\2", name)
            name = re.sub(r"([A-Z])([a-z])", r"_\1\2", name)

            # Replace separators with underscore
            name = re.sub(r"[\s\-.]+", "_", name)

            # Normalize
            name = name.lower().strip("_")
            name = re.sub(r"_+", "_", name)

            return name

        logging.info("Converting column names to snake case...")
        return df.rename(columns={col: to_snake_case(col) for col in df.columns})

    def process_loadfile_df(self, loadfile_df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize and enrich the loadfile DataFrame prior to Spark conversion.

        Args:
            loadfile_df (pd.DataFrame): Raw DataFrame read from loadfile.csv.

        Returns:
            pd.DataFrame: Processed DataFrame with added columns and cleaned
                values.
        """
        loadfile_df = CsvNoItemExtractor.convert_columns_to_snake_case(loadfile_df)
        logging.info(f"Snake converted columns:\n{loadfile_df.columns.tolist()}")

        loadfile_df = loadfile_df.fillna("null")
        loadfile_df["transaction_id"] = self.transaction_id
        loadfile_df["batch_name"] = self.batch_name
        loadfile_df["image_path"] = (
            self.target_prefix
            + loadfile_df["image_path"].str.replace("\\", "/")
        )
        loadfile_df["tr_customer_id"] = ""
        loadfile_df["item_code"] = ""
        loadfile_df["item_code_id"] = ""

        logging.info(
            "N/A filled with 'null' and colums 'transaction_id' and "
            "'batch_name' created.\n"
            "'target_prefix' added to the path inside the 'image_path' "
            "column.\n"
            "'tr_customer_id', 'item_code', 'item_code_id' added to enforce "
            "schema"
        )
        return loadfile_df

    def build_spark_df(self, loadfile_df: pd.DataFrame) -> DataFrame:
        """
        Create a Spark DataFrame with a string schema from a Pandas DataFrame.

        Args:
            loadfile_df (pd.DataFrame): Processed Pandas DataFrame.

        Returns:
            DataFrame: Spark DataFrame with all fields cast as StringType.
        """
        logging.info(
            "Building Spark DataFrame from Pandas DataFrame. "
            f"Size: {loadfile_df.shape}"
        )
        all_columns = loadfile_df.columns.tolist()
        schema = StructType(
            [
                StructField(column_name, StringType(), nullable=True)
                for column_name in all_columns
            ]
        )
        df_spark = self.spark.createDataFrame(loadfile_df, schema=schema)

        logging.info("Spark Dataframe created.")

        return df_spark

    @F.pandas_udf(StringType())
    def create_uuid_from_string(val_series: pd.Series) -> pd.Series:
        """
        Create deterministic UUID strings from an input series of strings.

        Args:
            val_series (pd.Series): Series of strings to hash into UUIDs.

        Returns:
            pd.Series: Series of UUID strings derived from the input strings.
        """
        return val_series.apply(
            lambda val: str(
                uuid.UUID(hex=hashlib.md5(val.encode("UTF-8")).hexdigest())
            )
        )

    @staticmethod
    def create_customer_filebarcode(df_spark: DataFrame) -> DataFrame:
        """
        Ensure the 'ref1' field is populated using a deterministic UUID.

        Args:
            df_spark (DataFrame): Spark DataFrame containing ref fields.

        Returns:
            DataFrame: Spark DataFrame with 'ref1' filled when missing and
                intermediate columns dropped.
        """
        df_spark = (
            df_spark.withColumn(
                "ref1GUIDstr",
                F.concat(
                    F.col("batch_name"),
                    F.lit("_"),
                    F.col("image_path"),
                    F.lit("_"),
                    F.col("image_filename"),
                ),
            )
            .withColumn(
                "ref1GUID",
                CsvNoItemExtractor.create_uuid_from_string(F.col("ref1GUIDstr")),
            )
            .withColumn(
                "ref1",
                F.when(F.col("ref1") == "null", F.col("ref1GUID")).otherwise(
                    F.col("ref1")
                ),
            )
            .drop("ref1GUIDstr", "ref1GUID")
        )
        return df_spark

    def no_valid_box_barcode(
        self,
        customer_config: dict,
        collection_name: str,
        box_barcode: str,
        tr_account_name: str,
        tr_customer_id: str,
    ) -> None:
        """
        Handle the case where a box barcode is invalid for a given collection.

        This method builds and sends a failure message to OpsIQ indicating that
        the provided box barcode did not pass validation for the collection.
        It also logs the failure to Timestream.

        Args:
            customer_config (dict): Customer-level configuration used to resolve
                collection-specific rules and templates.
            collection_name (str): Name of the collection being processed.
            box_barcode (str): Box barcode that failed validation.
            tr_account_name (str): TR Account Name for error logging.
            tr_customer_id (str): TR Customer ID for error logging.

        Returns:
            None
        """

        FieldSchema = [
            {
                "FieldName": "collection_name",
                "AlternativeLabels": [],
                "FieldLabel": "Collection Name",
                "Type": "string",
                "Validation": {
                    "MinLength": "",
                    "MaxLength": "",
                    "Pattern": "",
                    "AllowSpecialChars": 'false',
                    "AllowNull": 'false'
                }
            },            
            {
                "FieldName": "box_barcode",
                "AlternativeLabels": [],
                "FieldLabel": "Box Barcode",
                "Type": "string",
                "Validation": {
                    "MinLength": "",
                    "MaxLength": "",
                    "Pattern": "",
                    "AllowSpecialChars": 'false',
                    "AllowNull": 'false'
                }
            }                      
        ]

        opsiq_message_manager = OpsIQMessageManager(
            opsiq_queue=self.opsiq_queue,
            batch_name=self.batch_name,
            field_schema=FieldSchema,
            transaction_id=self.transaction_id,
            system="opsiq",
            source=self.source,
            source_bucket=self.bucket_name,
            config_template=dict(),
        )

        FieldList = [

        {
            "FieldName": "collection_name",
            "ValueScanned": collection_name,
            "IsValid": 'true'
        },
        {
            "FieldName": "box_barcode",
            "ValueScanned": box_barcode,
            "IsValid": 'false'
        }
        ]
        
        opsiq_message_manager.save_item_message_content(
            ItemCode=box_barcode,
            Status="Failed",
            Error="Invalid Box Barcode",
            ErrorCode="74",
            EventName="BoxBarcodeValidation",
            FieldList=FieldList,
            DocumentImages=[],
        )

        opsiq_message_manager.send_opsiq_message()

        if self.timestream_config:
            error = f"Customer ID and Box Barcode Combination is not valid for TR Account Name: {tr_account_name}"
            logging.error(error)
            error_code = "74"
            record_count = "0"
            failed_timestamp = str(int(datetime.now().timestamp() * 1000))
            
            content = {
                "Dimensions": [
                    {"Source": self.source, "Type": "VARCHAR"},
                    {"PipelineName": self.timestream_config["source_device"], "Type": "VARCHAR"},
                    {"TRAccountNo": str(tr_customer_id) if tr_customer_id is not None else '-', "Type": "VARCHAR"},
                    {"TransactionId": self.transaction_id, "Type": "VARCHAR"},
                    {"TimeStamp": failed_timestamp, "Type": "VARCHAR"},
                    {"BatchName": self.batch_name, "Type": "VARCHAR"},
                    {"ItemCode": box_barcode, "Type": "VARCHAR"},
                    {"Error": error, "Type": "VARCHAR"},
                    {"ErrorCode": error_code, "Type": "VARCHAR"}
                ],
                "Measures": [
                    {"PipelineModule": self.timestream_config["module"], "Type": "VARCHAR"},
                    {"EventName": "BoxBarcodeValidation", "Type": "VARCHAR"},
                    {"State": "Failed", "Type": "VARCHAR"},
                    {"NoOfAsset": record_count, "Type": "BIGINT"}
                ]
            }

            Onboarding_Timestream_Manager.timestream_insert_data(
                db=self.timestream_config["db"],
                table=self.timestream_config["table"],
                timestream_queue=self.timestream_config["queue"],
                measure_name=f'OnboardingWF_{self.timestream_config["module"]}',
                content=content
            )

    def log_valid_box_barcode_to_timestream(
        self,
        box_barcode: str,
        tr_account_name: str,
        tr_customer_id: str,
    ) -> None:
        """
        Log a success event to Timestream when the box barcode is valid.

        Args:
            box_barcode (str): Box barcode that passed validation.
            tr_account_name (str): TR Account Name.
            tr_customer_id (str): TR Customer ID.

        Returns:
            None
        """
        if self.timestream_config:
            timestamp = str(int(datetime.now().timestamp() * 1000))
            
            content = {
                "Dimensions": [
                    {"Source": self.source, "Type": "VARCHAR"},
                    {"PipelineName": self.timestream_config["source_device"], "Type": "VARCHAR"},
                    {"TRAccountNo": str(tr_customer_id) if tr_customer_id is not None else '-', "Type": "VARCHAR"},
                    {"TransactionId": self.transaction_id, "Type": "VARCHAR"},
                    {"TimeStamp": timestamp, "Type": "VARCHAR"},
                    {"BatchName": self.batch_name, "Type": "VARCHAR"},
                    {"ItemCode": box_barcode, "Type": "VARCHAR"},
                ],
                "Measures": [
                    {"PipelineModule": self.timestream_config["module"], "Type": "VARCHAR"},
                    {"EventName": "BoxBarcodeValidation", "Type": "VARCHAR"},
                    {"State": "Complete", "Type": "VARCHAR"},
                    {"NoOfAsset": "1", "Type": "BIGINT"}
                ]
            }

            logging.info(f"Logging valid box barcode '{box_barcode}' to Timestream.")
            Onboarding_Timestream_Manager.timestream_insert_data(
                db=self.timestream_config["db"],
                table=self.timestream_config["table"],
                timestream_queue=self.timestream_config["queue"],
                measure_name=f'OnboardingWF_{self.timestream_config["module"]}',
                content=content
            )


    def extract_metadata(self, location: S3Location) -> DataFrame:
        """
        Extract and validate metadata from loadfile.csv and return a Spark DF.

        Args:
            location (S3Location): S3 location that identifies the CSV source.

        Returns:
            DataFrame: Processed Spark DataFrame ready for downstream steps.

        Raises:
            FileNotFoundError: If loadfile.csv does not exist in S3.
            ValueError: If the input is empty or the box barcode is invalid.
        """
        logging.info(
            f"Extracting metadata from CSV source: {location.prefix}"
        )
        exists = self.load_file_exists()
        if not exists:
            raise FileNotFoundError(
                "loadfile.csv not found in "
                f"s3://{self.bucket_name}/{self.target_prefix}"
            )
        loadfile_df = self.read_csv_loadfile()
        self.apply_filter_parameters(loadfile_df)

        with pd.option_context('display.max_columns', None,
                            'display.max_rows', None,
                            'display.width', None,
                            'display.max_colwidth', None):
            print(loadfile_df.head(5))  # Limits to 10 rows, context removes truncation

        box_barcode, tr_account_name, collection_name = self.ensure_uniqueness(loadfile_df)
        customer_config = self.get_customer_config(tr_account_name)
        tr_customer_id = customer_config["tr_customer_id"]
        is_valid_box_barcode = self.validate_box_barcode(
            box_barcode, tr_customer_id
        )
  
        if is_valid_box_barcode:
            self.log_valid_box_barcode_to_timestream(
                box_barcode, tr_account_name, tr_customer_id
            )

        if not is_valid_box_barcode:
            self.no_valid_box_barcode(
                customer_config,
                collection_name,
                box_barcode,
                tr_account_name,
                tr_customer_id
            )
            exit(
                f"Invalid Box Barcode '{box_barcode}' for TR Customer "
                f"'{tr_account_name}'"
            )

        processed_loadfile_df = self.process_loadfile_df(loadfile_df)
        spark_df = self.build_spark_df(processed_loadfile_df)
        processed_spark_df = CsvNoItemExtractor.create_customer_filebarcode(
            spark_df
        )
        self.glue_client.put_workflow_run_properties(
            Name=self.workflow_name,
            RunId=self.workflow_run_id,
            RunProperties={"tr_account_name": tr_account_name},
        )
        return processed_spark_df


class PerImageExtractor(MetadataExtractor):
    def __init__(
        self,
        location: S3Location,
        glue_catalog: GlueCatalog,
        s3_client: BaseClient,
        glue_client: BaseClient,
        spark: SparkSession,
        batch_name: str,
        transaction_id: str,
    ):
        """
        Initialize a per-image metadata extractor for image-based ingestion.

        Args:
            location (S3Location): S3 location containing image objects.
            glue_catalog (GlueCatalog): Glue catalog database and table info.
            s3_client (BaseClient): Boto3 S3 client to list and read objects.
            glue_client (BaseClient): Boto3 Glue client to query table schema.
            spark (SparkSession): Spark session used to build DataFrames.
            batch_name (str): Batch identifier derived from the file key.
            transaction_id (str): Transaction identifier for the ingestion.

        Returns:
            None
        """
        super().__init__()
        self.location = location
        self.s3_client = s3_client
        self.glue_client = glue_client
        self.batch_name = batch_name
        self.transaction_id = transaction_id
        self.spark = spark
        self.glue_catalog = glue_catalog
        self.file_key_parts = batch_name.split("_")
        self.collection = self.file_key_parts[1]
        self.project = self.file_key_parts[2]
        self.tr_ocr_column = self.file_key_parts[3].split('.')[0].upper()

    def get_objects_at_prefix(self) -> list[dict]:
        """
        List S3 objects available under the configured prefix.

        Args:
            None

        Returns:
            list[dict]: List of S3 paginator pages containing object metadata.
        """
        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.location.bucket,
            Prefix=f"{self.location.prefix}",
        )

        filtered_pages = [page for page in pages if "Contents" in page]
        return filtered_pages

    def get_glue_table_schema(
        self,
        glue_client,
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
            logging.info(
                "Fetching schema for table: "
                f"'{glue_catalog.catalog_db}.{glue_catalog.catalog_table}'"
            )

            response = glue_client.get_table(
                DatabaseName=glue_catalog.catalog_db,
                Name=glue_catalog.catalog_table,
            )
            columns = response["Table"]["StorageDescriptor"]["Columns"]
            partition_keys = response["Table"].get("PartitionKeys", [])
            ordered_columns = [_col["Name"] for _col in columns] + [p["Name"] for p in partition_keys]

            logging.info(
                "Retrieved Glue schema order for table "
                f"'{glue_catalog.catalog_db}.{glue_catalog.catalog_table}':\n"
                f"{ordered_columns}"
            )
            return ordered_columns

        except Exception as e:
            logging.error(
                "Critical error in the 'get_glue_table_schema' function for "
                f"the table '{glue_catalog.catalog_db}."
                f"{glue_catalog.catalog_table}':\n{str(e)}\n"
                f"Error type: {type(e).__name__}"
            )
            exit("ConnectCrawl - Critical error")

    def process_image(self, image_path: str) -> tuple:
        """
        Extract metadata fields from a single image object key.

        Args:
            image_path (str): Full S3 object key of the image file.

        Returns:
            tuple: Tuple containing extracted metadata fields, or an empty
                tuple if the file extension is not supported.
        """
        if not image_path.lower().endswith(("tif", "jpeg", "jpg", "png", "tiff")):
            return ()

        img_key = image_path.split("/")[-1]
        img_key_parts = img_key.split("_")
        tr_customer_id = img_key_parts[0]
        item_code = img_key_parts[1]
        item_code_id = img_key_parts[2]

        now = datetime.now()
        current_datetime = f"{now.month}/{now.day}/{now.strftime('%y %H:%M')}"

        data = (
            "",
            image_path,
            img_key,
            self.transaction_id,
            self.project,
            self.collection,
            "",
            current_datetime,
            "",
            "",
            "",
            item_code,
            item_code_id,
            tr_customer_id,
            self.batch_name,
        )
        return data

    def process_objects(self, objects: list[dict]) -> DataFrame:
        """
        Process a list of S3 objects and extract image-level metadata.

        Args:
            objects (list[dict]): List of S3 paginator pages with contents.

        Returns:
            list: List of tuples representing extracted image metadata.
        """
        results = []
        for obj in objects:
            for item in obj["Contents"]:
                img_path = item["Key"]
                res = self.process_image(img_path)
                if res:
                    results.append(res)
        return results

    def extract_metadata(self, location: S3Location) -> DataFrame:
        """
        Extract per-image metadata from S3 and build a Spark DataFrame.

        Args:
            location (S3Location): S3 location containing image objects.

        Returns:
            DataFrame: Spark DataFrame populated with per-image metadata.
        """
        logging.info(
            "Extracting metadata from Zip source: "
            f"{location.prefix}\n"
            f"File key parts: '{self.file_key_parts}'\n"
            f"Project: '{self.project}'\n"
            f"Collection: {self.collection}"

        )
        logging.info(f"tr_ocr_column is: {self.tr_ocr_column}")
        if "REF" not in self.tr_ocr_column:
            exit(
                f"Invalid Batch Name {self.batch_name}. Missing REF field where OCR Text will be populated"
            )
            #todo Write to Timestream

        objects = self.get_objects_at_prefix()
        data = self.process_objects(objects)
        catalog_schema = self.get_glue_table_schema(
            self.glue_client, self.glue_catalog
        )
        logging.info(f"Data to build spark dataframe: {data}")
        logging.info(f"Columns to build spark dataframe: {catalog_schema}")
        df_spark = self.spark.createDataFrame(data, schema=catalog_schema)
        return df_spark


class FactoryMetadataExtractor:
    @staticmethod
    def get_metadata_extractor(
        source_file_name: str,
        source: str,
        location: S3Location,
        s3_client: BaseClient,
        spark: SparkSession,
        batch_name: str,
        transaction_id: str,
        tr_credentials: TRCredentials,
        glue_client: BaseClient,
        glue_catalog: GlueCatalog,
        workflow_name: str,
        workflow_run_id: str,
        opsiq_queue: str = None,
        filter_parameters: dict = None,
        timestream_config: dict = None,
    ) -> MetadataExtractor:
        """
        Return a concrete MetadataExtractor implementation based on the source.

        Args:
            source_file_name (str): source_file_name identifier used to choose the extractor type.
            source (str): Source identifier used for the pipeline.
            location (S3Location): S3 location that points to the input data.
            s3_client (BaseClient): Boto3 S3 client used by the extractor.
            spark (SparkSession): Spark session used by the extractor.
            batch_name (str): Batch identifier for the incoming data.
            transaction_id (str): Transaction identifier for the batch.
            tr_credentials (TRCredentials): Credentials for TR API calls when
                required by the extractor.
            glue_client (BaseClient): Boto3 Glue client used by the extractor.
            glue_catalog (GlueCatalog): Glue catalog info used by image
                extraction to fetch schema.
            workflow_name (str): Glue workflow name used to store properties.
            workflow_run_id (str): Glue workflow run id used to store props.
            filter_parameters (dict): Optional filter parameters to update data.
            timestream_config (dict): Timestream settings for error logging.

        Returns:
            MetadataExtractor: Concrete extractor instance for the given
                source.

        Raises:
            ValueError: If the source_file_name does not match a supported extractor.
        """
        if "radix-onboarding-indexed" in source_file_name:
            return CsvNoItemExtractor(
                source_file_name=source_file_name,
                source=source,
                location=location,
                tr_credentials=tr_credentials,
                s3_client=s3_client,
                glue_client=glue_client,
                spark=spark,
                batch_name=batch_name,
                transaction_id=transaction_id,
                workflow_name=workflow_name,
                workflow_run_id=workflow_run_id,
                opsiq_queue=opsiq_queue,
                filter_parameters=filter_parameters,
                timestream_config=timestream_config,
            )
        elif "tr-image-extract" in source_file_name:
            return PerImageExtractor(
                location=location,
                glue_catalog=glue_catalog,
                s3_client=s3_client,
                glue_client=glue_client,
                spark=spark,
                batch_name=batch_name,
                transaction_id=transaction_id,
            )
        else:
            raise ValueError(f"Unsupported source type: {location.prefix}")
