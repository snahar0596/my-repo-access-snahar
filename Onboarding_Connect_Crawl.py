import importlib
import sys
import boto3
import zipfile
import io
import os
import ast
import Onboarding_Log_Manager
import Onboarding_Timestream_Manager
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.context import SparkContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from datetime import datetime
from botocore.exceptions import ClientError
from Onboarding_Configuration_Template import S3Location, GlueCatalog
from Onboarding_Metadata_Extractor import TRCredentials, FactoryMetadataExtractor
from logging import Logger

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 200)


class JobVariables:
    def __init__(self, args: dict):
        """
        Parse and store job-level variables passed to the Glue job.

        Args:
            args (dict): Dictionary containing job arguments provided at
                runtime, including workflow, catalog, and TR settings.

        Returns:
            None
        """
        self.job_name = args["JOB_NAME"]
        self.workflow_name = args["WORKFLOW_NAME"]
        self.workflow_run_id = args["WORKFLOW_RUN_ID"]
        self.catalog_db = args["CatalogDB"]
        self.incoming_catalog_table = args["IncomingCatalogTable"]
        self.timestream_db = args["TimestreamDB"]
        self.timestream_table = args["TimestreamTable"]
        self.timestream_sqs_queue = args["TimestreamSQSQueue"]
        self.regionName = args["RegionName"]
        self.tr_base_api = args["BaseTRApi"]
        self.trSecretManagerName = args["TRSecretManagerName"]
        self.decompressedFolder = args["DecompressedFolder"]
        self.opsiq_queue = args.get("OpsIQQueue", None)

    def __repr__(self) -> str:
        """
        Return a human-readable representation of the job variables.

        Args:
            None

        Returns:
            str: Multiline string containing key job configuration values.
        """
        return (
            f"job_name: '{self.job_name}'\n"
            f"workflow_name: '{self.workflow_name}'\n"
            f"workflow_run_id: '{self.workflow_run_id}'\n"
            f"catalog_db: '{self.catalog_db}'\n"
            f"incoming_catalog_table: '{self.incoming_catalog_table}'\n"
            f"timestream_db: '{self.timestream_db}'\n"
            f"timestream_table: '{self.timestream_table}'\n"
            f"timestream_sqs_queue: '{self.timestream_sqs_queue}'\n"
        )
    

class WorkflowParams:
    def __init__(self, workflow_params: dict, log: Logger):
        """
        Parse and store workflow parameters passed to the Glue workflow run.

        Args:
            workflow_params (dict): Dictionary containing workflow runtime
                parameters such as source, zip key, and transaction id.
            log (Logger): Logger instance used to emit workflow-related logs.

        Returns:
            None
        """
        self.log = log
        self.transaction_id = workflow_params["transaction_id"]
        self.source = workflow_params["source"]
        self.incoming_bucket = workflow_params["incoming_bucket"]
        self.source_device = workflow_params["source_device"]
        
        # 1. Parse filter parameters first
        self.filter_parameters = self.get_filter_parameters(workflow_params)

        # 2. Extract raw zip_key
        raw_zip_key = workflow_params["zip_key"]
        
        # 3. Derive batch name (needed for looking up specific batch config)
        # Note: Logic assumes batch name is the filename without extension
        self.batch_name = raw_zip_key.rsplit("/", 1)[-1].rsplit(".", 1)[0]

        # 4. Normalize zip_key (Handle missing prefix)
        self.zip_key = self.normalize_zip_key(raw_zip_key)

    def get_filter_parameters(self, workflow_params: dict) -> dict:
        """
        Extract and parse filter parameters from workflow parameters.

        Args:
            workflow_params (dict): Dictionary containing workflow runtime
                parameters.

        Returns:
            dict: Parsed filter parameters or empty dict if not present.
        """
        filter_params_str = workflow_params.get("filter_parameters", None)
        if not filter_params_str:
            return {}
        
        try:
            return ast.literal_eval(filter_params_str)
        except (ValueError, SyntaxError):
            self.log.warning(f"Failed to parse filter_parameters: {filter_params_str}")
            return {}

    def normalize_zip_key(self, raw_zip_key: str) -> str:
        """
        Ensure the zip_key has the correct prefix path.
        If the key has no slashes, it tries to find a prefix in filter_parameters
        or defaults to 'radix-onboarding-indexed/'.

        Args:
            raw_zip_key (str): The zip key string from workflow params.

        Returns:
            str: The normalized zip key with a path prefix.
        """
        if "/" in raw_zip_key:
            return raw_zip_key
        
        self.log.info(f"Detected zip_key without path: '{raw_zip_key}'. Attempting to normalize.")

        # Try to find a specific prefix in the filter params for this batch
        prefix = None
        if self.filter_parameters:
            # Check if there is a 'zip_key_prefix' at the top level
            prefix = self.filter_parameters.get("zip_key_prefix")
            
            # If not, check inside the specific batch config
            if not prefix and self.batch_name in self.filter_parameters:
                batch_config = self.filter_parameters[self.batch_name]
                prefix = batch_config.get("zip_key_prefix")

        # Fallback default if no prefix found
        if not prefix:
            prefix = "radix-onboarding-indexed/"
            self.log.info("No 'zip_key_prefix' found in parameters. Using default: 'radix-onboarding-indexed/'")
        else:
            self.log.info(f"Using configured 'zip_key_prefix': '{prefix}'")

        # Ensure prefix ends with a slash
        if not prefix.endswith("/"):
            prefix += "/"
            
        normalized_key = f"{prefix}{raw_zip_key}"
        self.log.info(f"Normalized zip_key: '{normalized_key}'")
        return normalized_key

    def __repr__(self) -> str:
        """
        Return a human-readable representation of the workflow parameters.

        Args:
            None

        Returns:
            str: Multiline string describing the workflow parameters.
        """
        return (
            f"transaction_id: '{self.transaction_id}'\n"
            f"source: '{self.source}'\n"
            f"zip_key: '{self.zip_key}'\n"
            f"incoming_bucket: '{self.incoming_bucket}'\n"
            f"source_device: '{self.source_device}'\n"
            f"batch_name: '{self.batch_name}'\n"
            f"filter_parameters: '{self.filter_parameters}'\n"
        )

    def get_batch_name(self) -> str:
        """
        Derive the batch name from the zip file key.
        (Deprecated/Redundant: Logic moved to __init__ but kept for compatibility if needed)

        Args:
            None

        Returns:
            str: Batch name extracted from the zip file name.
        """
        return self.batch_name

    def get_target_prefix(self, decompressed_folder: str) -> str:
        """
        Build the target S3 prefix for the decompressed batch contents.

        Args:
            decompressed_folder (str): Root folder used for decompressed data.

        Returns:
            str: Target S3 prefix where extracted files will be written.
        """
        zip_path = self.zip_key.rsplit("/", 1)[0]
        self.log.info(f"zip_path: '{zip_path}'")

        path_parts = zip_path.split("/", 1)
        self.log.info(f"Initial value for 'path_parts' path: '{path_parts}'")

        path_parts[0] = decompressed_folder
        self.log.info(f"Final value for 'path_parts' path: '{path_parts}'")

        target_prefix = (
            "/".join(path_parts) + "/" + self.batch_name + "/"
        )
        self.log.info(f"target_prefix: '{target_prefix}'")

        return target_prefix

    

class GlueJobExecution:
    def __init__(self):
        """
        Initialize Glue and Spark clients required to run the job.

        Args:
            None

        Returns:
            None
        """
        self.glue_client = boto3.client("glue")
        self.s3_client = boto3.client("s3")
        self.secret_manager = importlib.import_module(
            "etl-plugin-scripts.helper.ssm_services"
        )
        self.pipeline_mod = "ConnectCrawl"
        self.log = Onboarding_Log_Manager.get_module_logger(self.pipeline_mod)
        sc = SparkContext()
        glueContext = GlueContext(sc)
        self.spark = glueContext.spark_session
        self.job = Job(glueContext)
        self.spark.conf.set(
            "hive.exec.dynamic.partition.mode",
            "nonstrict",
        )  # Enable dynamic partitioning for flexible DataFrame writes.

    def unzip_s3_file(
        self,
        bucket_name: str,
        zip_file_key: str,
        target_prefix: str
    ) -> None:
        """
        Decompress a zip file stored in S3 and upload its contents back to S3.

        Args:
            bucket_name (str): S3 bucket containing the zip file.
            zip_file_key (str): S3 key for the zip file to download.
            target_prefix (str): S3 prefix where decompressed files are saved.

        Returns:
            None
        """
        try:
            self.log.info(f"Downloading zip file: {zip_file_key}")
            zip_obj = self.s3_client.get_object(
                Bucket=bucket_name,
                Key=zip_file_key,
            )
            zip_content = zip_obj["Body"].read()

            zip_file = io.BytesIO(zip_content)

            with zipfile.ZipFile(zip_file, "r") as zip_ref:
                file_list = sorted(zip_ref.namelist())

                self.log.info("Files found in zip (alphabetically sorted):")

                for file_name in file_list:
                    self.log.info(f"  - {file_name}")
                    if file_name.endswith("/"):
                        continue

                    file_content = zip_ref.read(file_name)

                    target_key = os.path.join(target_prefix, file_name).replace(
                        "\\",
                        "/",
                    )

                    self.log.info(f"Uploading: {target_key}")
                    self.s3_client.put_object(
                        Bucket=bucket_name,
                        Key=target_key,
                        Body=file_content,
                    )

            self.log.info(
                f"Successfully extracted {len(file_list)} files to "
                f"s3://{bucket_name}/{target_prefix}"
            )

        except ClientError as e:
            self.log.error(f"Error accessing S3: {e}")
        except zipfile.BadZipFile:
            self.log.error("Error: The file is not a valid zip file")
        except Exception as e:
            self.log.error(f"Unexpected error: {e}")

    def get_glue_table_schema(self, database_name: str, table_name: str) -> list:
        """
        Fetch the schema (non-partition columns) for a Glue table.

        Args:
            database_name (str): Glue database name.
            table_name (str): Glue table name.

        Returns:
            list: Ordered list of non-partition column names.
        """
        try:
            self.log.info(
                f"Fetching schema for table: '{database_name}.{table_name}'"
            )

            response = self.glue_client.get_table(
                DatabaseName=database_name,
                Name=table_name,
            )
            columns = response["Table"]["StorageDescriptor"]["Columns"]
            ordered_columns = [_col["Name"] for _col in columns]

            self.log.info(
                "Retrieved Glue schema order for table "
                f"'{database_name}.{table_name}':\n{ordered_columns}"
            )
            return ordered_columns

        except Exception as e:
            self.log.error(
                "Critical error in the 'get_glue_table_schema' function for "
                f"the table '{database_name}.{table_name}':\n{str(e)}\n"
            )
            self.log.error(f"Error type: {type(e).__name__}")
            exit("ConnectCrawl - Critical error")

    def validate_and_prepare_dataframe(
        self,
        input_sdf: DataFrame,
        expected_columns: list
    ) -> DataFrame:
        """
        Ensure a Spark DataFrame matches the expected schema column set/order.

        Notes:
        - Missing columns are added as 'null' strings.
        - Extra columns are ignored.
        - Output is returned with columns ordered as expected_columns.

        Args:
            input_sdf (DataFrame): Input Spark DataFrame to validate.
            expected_columns (list): Ordered list of required column names.

        Returns:
            DataFrame: Prepared Spark DataFrame with expected columns/order.
        """
        set_expected_cols = set(expected_columns)
        set_input_cols = set(input_sdf.columns)

        self.log.info(
            f"set_expected_cols:\n{set_expected_cols}\n"
            f"set_input_cols:\n{set_input_cols}"
        )

        missing_columns = set_expected_cols - set_input_cols
        present_columns = set_expected_cols & set_input_cols
        extra_columns = set_input_cols - set_expected_cols

        self.log.info(
            f"missing_columns:\n{missing_columns}\n"
            f"present_columns:\n{present_columns}\n"
            f"extra_columns:\n{extra_columns}\n"
        )

        if missing_columns:
            self.log.warning(
                f"Missing columns in input DataFrame:\n{missing_columns}"
            )

            for col in missing_columns:
                input_sdf = input_sdf.withColumn(
                    col,
                    lit("null").cast(StringType()),
                )
        else:
            self.log.info(
                "There are not missing columns in the input dataframe with "
                "respect to the required or expected columns."
            )

        return input_sdf.select(*expected_columns)

    def write_to_glue_catalog(
        self,
        input_sdf: DataFrame,
        target_db: str,
        target_table_name: str
    ) -> None:
        """
        Validate, align, and write a Spark DataFrame into a Glue table.

        Args:
            input_sdf (DataFrame): Input Spark DataFrame to write.
            target_db (str): Glue database name for the target table.
            target_table_name (str): Glue table name to write into.

        Returns:
            None
        """
        self.log.info(f"Writing to Glue: '{target_db}.{target_table_name}'")

        expected_columns = (
            self.get_glue_table_schema(target_db, target_table_name)
            + ["batch_name"]
        )
        prepared_df = self.validate_and_prepare_dataframe(
            input_sdf,
            expected_columns,
        )

        self.log.info(
            f"Writing '{prepared_df.count()}' records to "
            f"'{target_db}.{target_table_name}'"
        )
        self.log.info(f"Final columns are:\n{prepared_df.columns}")

        prepared_df.coalesce(1).write.mode("append").format("parquet").insertInto(
            f"`{target_db}`.`{target_table_name}`"
        )

        self.log.info("Successfully wrote to Glue catalog")

    def create_timestream_content(
        self,
        source: str,
        source_device: str,
        transaction_id: str,
        batch_name: str,
        state: str,
        no_of_asset: str,
        file_path: str = None
    ) -> dict:
        """
        Build a Timestream payload for success/processing state tracking.

        Args:
            source (str): Source identifier for the ingestion.
            source_device (str): Device/Pipeline name for dimension.
            transaction_id (str): Transaction identifier for the run.
            batch_name (str): Batch identifier for the run.
            state (str): Pipeline state to report (e.g., Ready, Complete).
            no_of_asset (str): Number of assets represented by the event.
            file_path (str): Optional file path for per-asset reporting.

        Returns:
            dict: Timestream payload containing dimensions and measures.
        """
        content = {
            "Dimensions": [
                {"Source": source, "Type": "VARCHAR"},
                {"PipelineName": source_device, "Type": "VARCHAR"},
                {"TransactionId": transaction_id, "Type": "VARCHAR"},
                {"BatchName": batch_name, "Type": "VARCHAR"},
            ],
            "Measures": [
                {"PipelineModule": self.pipeline_mod, "Type": "VARCHAR"},
                {"State": state, "Type": "VARCHAR"},
                {"NoOfAsset": no_of_asset, "Type": "BIGINT"},
            ],
        }
        if file_path:
            content["Dimensions"].append(
                {"FilePath": file_path, "Type": "VARCHAR"}
            )
        return content

    def create_timestream_failure_content(
        self,
        source: str,
        source_device: str,
        transaction_id: str,
        batch_name: str,
        error: str,
        error_code: str,
        record_count: str,
        tr_account_name: str = "-"
    ) -> dict:
        """
        Build a Timestream payload for failure reporting.

        Args:
            source (str): Source identifier for the ingestion.
            source_device (str): Device/Pipeline name for dimension.
            transaction_id (str): Transaction identifier for the run.
            batch_name (str): Batch identifier for the run.
            error (str): Error message to report.
            error_code (str): Error code to classify the failure.
            record_count (str): Number of affected records/assets.
            tr_account_name (str): Optional TR account number/name.

        Returns:
            dict: Timestream payload containing failure details.
        """
        content = {
            "Dimensions": [
                {"Source": source, "Type": "VARCHAR"},
                {"PipelineName": source_device, "Type": "VARCHAR"},
                {"TRAccountNo": tr_account_name, "Type": "VARCHAR"},
                {"TransactionId": transaction_id, "Type": "VARCHAR"},
                {
                    "TimeStamp": str(int(datetime.now().timestamp() * 1000)),
                    "Type": "VARCHAR",
                },
                {"BatchName": batch_name, "Type": "VARCHAR"},
                {"Error": error, "Type": "VARCHAR"},
                {"ErrorCode": error_code, "Type": "VARCHAR"},
            ],
            "Measures": [
                {"PipelineModule": self.pipeline_mod, "Type": "VARCHAR"},
                {"State": "Failed", "Type": "VARCHAR"},
                {"NoOfAsset": record_count, "Type": "BIGINT"},
            ],
        }
        return content

    def report_complete_per_row(
        self,
        df_spark: DataFrame,
        workflow_params: WorkflowParams,
        job_vars: JobVariables,
    ) -> None:
        """
        Report a "Complete" Timestream event per row in the Spark DataFrame.

        Args:
            df_spark (DataFrame): Spark DataFrame containing processed assets.
            workflow_params (WorkflowParams): Workflow runtime parameters.
            job_vars (JobVariables): Job configuration for Timestream targets.

        Returns:
            None
        """
        rows = df_spark.collect()
        for row in rows:
            Onboarding_Timestream_Manager.timestream_insert_data(
                db=job_vars.timestream_db,
                table=job_vars.timestream_table,
                timestream_queue=job_vars.timestream_sqs_queue,
                measure_name=f"OnboardingWF_{self.pipeline_mod}",
                content=self.create_timestream_content(
                    source=workflow_params.source,
                    source_device=workflow_params.source_device,
                    transaction_id=workflow_params.transaction_id,
                    batch_name=workflow_params.batch_name,
                    state="Complete",
                    no_of_asset="1",
                    file_path=str(row["image_path"]),
                ),
            )

    def get_workflow_params(self, job_vars: JobVariables) -> WorkflowParams:
        """
        Fetch workflow run properties and build a WorkflowParams object.

        Args:
            job_vars (JobVariables): Job variables containing workflow ids.

        Returns:
            WorkflowParams: Parsed workflow parameters object.
        """
        workflow_params = self.glue_client.get_workflow_run_properties(
            Name=job_vars.workflow_name,
            RunId=job_vars.workflow_run_id,
        )["RunProperties"]
        workflow_params_obj = WorkflowParams(workflow_params, self.log)
        self.log.info(f"Workflow params: {workflow_params_obj}")
        return workflow_params_obj

    def get_tr_credentials(self, job_vars: JobVariables) -> TRCredentials:
        """
        Fetch TR credentials from Secrets Manager and build TRCredentials.

        Args:
            job_vars (JobVariables): Job variables containing secret settings.

        Returns:
            TRCredentials: TR credentials used for token generation and APIs.
        """
        tr_secret_keys = self.secret_manager.AWSSecretsManager(
            job_vars.trSecretManagerName,
            job_vars.regionName,
        ).get_secret_value()
        tr_credentials = TRCredentials(
            tr_secret_keys=tr_secret_keys,
            tr_base_api=job_vars.tr_base_api,
        )
        self.log.info("Fetched TR Credentials to create Token")
        return tr_credentials

    def main(self):
        """
        Run the Glue job end-to-end for onboarding ingestion.

        Notes:
        - Reads job args and workflow params.
        - Unzips the incoming payload into a decompressed S3 prefix.
        - Extracts metadata using a factory-selected extractor.
        - Writes results to Glue and emits Timestream events.

        Args:
            None

        Returns:
            None

        Raises:
            RuntimeError: If the overall process fails.
        """
        try:
            args = getResolvedOptions(
                sys.argv,
                [
                    "JOB_NAME",
                    "WORKFLOW_NAME",
                    "WORKFLOW_RUN_ID",
                    "CatalogDB",
                    "IncomingCatalogTable",
                    "TimestreamDB",
                    "TimestreamTable",
                    "TimestreamSQSQueue",
                    "RegionName",
                    "BaseTRApi",
                    "TRSecretManagerName",
                    "DecompressedFolder",
                    "OpsIQQueue"
                ],
            )

            job_vars = JobVariables(args)

            self.job.init(job_vars.job_name, args)

            self.log.info(
                f"""
                Step Name: Connect_Crawl
                Execution Date: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                Next Step: OCR_Image
            """
            )

            self.log.info(f"Job Variables:\n{job_vars}")

            workflow_params = self.get_workflow_params(job_vars)
            target_prefix = workflow_params.get_target_prefix(
                job_vars.decompressedFolder
            )

            tr_credentials = self.get_tr_credentials(job_vars)

            Onboarding_Timestream_Manager.timestream_insert_data(
                db=job_vars.timestream_db,
                table=job_vars.timestream_table,
                timestream_queue=job_vars.timestream_sqs_queue,
                measure_name=f"OnboardingWF_{self.pipeline_mod}",
                content=self.create_timestream_content(
                    source=workflow_params.source,
                    source_device=workflow_params.source_device,
                    transaction_id=workflow_params.transaction_id,
                    batch_name=workflow_params.batch_name,
                    state="Ready",
                    no_of_asset="0",
                ),
            )
            self.unzip_s3_file(
                workflow_params.incoming_bucket,
                workflow_params.zip_key,
                target_prefix,
            )
            location = S3Location(
                bucket=workflow_params.incoming_bucket,
                prefix=target_prefix,
            )
            glue_catalog = GlueCatalog(
                catalog_db=job_vars.catalog_db,
                catalog_table=job_vars.incoming_catalog_table,
            )
            timestream_config = {
                "db": job_vars.timestream_db,
                "table": job_vars.timestream_table,
                "queue": job_vars.timestream_sqs_queue,
                "module": self.pipeline_mod,
                "source_device": workflow_params.source_device
            }
            metadata_extractor = FactoryMetadataExtractor.get_metadata_extractor(
                source_file_name=workflow_params.zip_key,
                source=workflow_params.source,
                location=location,
                s3_client=self.s3_client,
                spark=self.spark,
                batch_name=workflow_params.batch_name,
                transaction_id=workflow_params.transaction_id,
                tr_credentials=tr_credentials,
                glue_client=self.glue_client,
                glue_catalog=glue_catalog,
                workflow_name=job_vars.workflow_name,
                workflow_run_id=job_vars.workflow_run_id,
                opsiq_queue=job_vars.opsiq_queue,
                filter_parameters=workflow_params.filter_parameters,
                timestream_config=timestream_config,
            )

            try:
                df_spark = metadata_extractor.extract_metadata(location)
            except Exception as e:
                message = f"Error during metadata extraction: {e}"
                self.log.error(message)

                Onboarding_Timestream_Manager.timestream_insert_data(
                    db=job_vars.timestream_db,
                    table=job_vars.timestream_table,
                    timestream_queue=job_vars.timestream_sqs_queue,
                    measure_name=f"OnboardingWF_{self.pipeline_mod}",
                    content=self.create_timestream_failure_content(
                        source=workflow_params.source,
                        source_device=workflow_params.source_device,
                        transaction_id=workflow_params.transaction_id,
                        batch_name=workflow_params.batch_name,
                        error=str(e),
                        error_code="42",
                        record_count="0",
                    ),
                )
                exit(message)

            self.report_complete_per_row(df_spark, workflow_params, job_vars)
            self.write_to_glue_catalog(
                df_spark,
                job_vars.catalog_db,
                job_vars.incoming_catalog_table,
            )
            Onboarding_Timestream_Manager.timestream_insert_data(
                db=job_vars.timestream_db,
                table=job_vars.timestream_table,
                timestream_queue=job_vars.timestream_sqs_queue,
                measure_name=f"OnboardingWF_{self.pipeline_mod}",
                content=self.create_timestream_content(
                    source=workflow_params.source,
                    source_device=workflow_params.source_device,
                    transaction_id=workflow_params.transaction_id,
                    batch_name=workflow_params.batch_name,
                    state="Complete",
                    no_of_asset=str(df_spark.count()),
                ),
            )

            self.log.info("Job completed successfully")

        except Exception as e:
            self.log.error(f"Critical error in the 'main' function:\n{str(e)}")
            self.log.error(f"Error type: {type(e).__name__}")

            Onboarding_Timestream_Manager.timestream_insert_data(
                db=job_vars.timestream_db,
                table=job_vars.timestream_table,
                timestream_queue=job_vars.timestream_sqs_queue,
                measure_name=f"OnboardingWF_{self.pipeline_mod}",
                content=self.create_timestream_content(
                    source=workflow_params.source,
                    source_device=workflow_params.source_device,
                    transaction_id=workflow_params.transaction_id,
                    batch_name=workflow_params.batch_name,
                    state="Failed",
                    no_of_asset="-",
                ),
            )

            raise RuntimeError("General process failed") from e

        finally:
            self.job.commit()


if __name__ == "__main__":
    glue_job = GlueJobExecution()
    glue_job.main()