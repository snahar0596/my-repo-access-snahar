import Onboarding_Log_Manager
import boto3
from logging import Logger
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


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
        self.job_name = args.get("JOB_NAME")
        self.workflow_name = args.get("WORKFLOW_NAME")
        self.workflow_run_id = args.get("WORKFLOW_RUN_ID")
        self.catalog_db = args.get("CatalogDB")
        self.incoming_catalog_table = args.get("IncomingCatalogTable")
        self.timestream_db = args.get("TimestreamDB")
        self.timestream_table = args.get("TimestreamTable")
        self.timestream_sqs_queue = args.get("TimestreamSQSQueue")
        self.regionName = args.get("RegionName", "")
        self.tr_base_api = args.get("BaseTRApi", "")
        self.trSecretManagerName = args.get("TRSecretManagerName", "")
        self.decompressedFolder = args.get("DecompressedFolder", "")
        self.enrichment_catalog_table = args.get("EnrichmentCatalogTable", "")
        self.opsiq_queue = args.get("OpsIQQueue", "")
        self.secret_manager_name = args.get("SecretManagerName", "")

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
            f"enrichment_catalog_table: '{self.enrichment_catalog_table}'\n"
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
        self.transaction_id = workflow_params["transaction_id"]
        self.source = workflow_params["source"]
        self.zip_key = workflow_params["zip_key"]
        self.incoming_bucket = workflow_params["incoming_bucket"]
        self.source_device = workflow_params["source_device"]
        self.log = log
        self.batch_name = self.get_batch_name()

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
        )

    def get_batch_name(self) -> str:
        """
        Derive the batch name from the zip file key.

        Args:
            None

        Returns:
            str: Batch name extracted from the zip file name.
        """
        batch_name = self.zip_key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        self.log.info(f"batch_name: '{batch_name}'")
        return batch_name

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


class GlueJobBase:
    def __init__(self, pipeline_mod: str):
        """
        Initialize Glue and Spark clients required to run the job.

        Args:
            None

        Returns:
            None
        """
        self.glue_client = boto3.client("glue")
        self.s3_client = boto3.client("s3")
        self.pipeline_mod = pipeline_mod
        self.log = Onboarding_Log_Manager.get_module_logger(self.pipeline_mod)
        sc = SparkContext()
        glueContext = GlueContext(sc)
        self.spark = glueContext.spark_session
        self.job = Job(glueContext)
        self.spark.conf.set(
            "hive.exec.dynamic.partition.mode",
            "nonstrict",
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
