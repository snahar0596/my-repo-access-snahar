import json
import sys
import boto3
import concurrent.futures
import Onboarding_Log_Manager
import pandas as pd
import Onboarding_Timestream_Manager
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit
from awsglue.utils import getResolvedOptions
from botocore.config import Config
from Onboarding_Glue_Job_Base import GlueJobBase, JobVariables, WorkflowParams
from Onboarding_Configuration_Template import GlueCatalog
from pyspark.sql import SparkSession, DataFrame
from botocore.client import BaseClient
from abc import ABC, abstractmethod
from typing import Callable
from functools import wraps

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 200)

log = Onboarding_Log_Manager.get_module_logger("OcrImage")
log.info('OnboardingOcrImage module')

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

class OcrExtractor(ABC):
    """
    Base OCR extractor that reads target rows from Glue Catalog, runs OCR (Textract),
    enriches the dataset with extracted text + spatial info, and returns a Spark DataFrame.

    Subclasses must implement `extract_text`, which defines the OCR strategy for a
    single image/document path.

    Args:
        job_vars (JobVariables): Job configuration container (catalog DB/tables,
            Timestream config, etc.) typically provided by GlueJobBase.
        workflow_params (WorkflowParams): Workflow runtime parameters (batch_name,
            transaction_id, incoming_bucket, source info, etc.).
        textract_client (BaseClient): Boto3 Textract client used to call Textract APIs.
        spark (SparkSession): Active Spark session used for Spark SQL and DataFrame ops.
        glue_client (BaseClient): Boto3 Glue client used to retrieve table schemas.
        pipeline_mod (str): Pipeline module identifier used for audit/metrics
            (e.g., included in Timestream measure names).

    Attributes:
        job_vars (JobVariables): Stored job configuration.
        workflow_params (WorkflowParams): Stored workflow parameters.
        textract_client (BaseClient): Stored Textract client.
        spark (SparkSession): Stored Spark session.
        glue_client (BaseClient): Stored Glue client.
        pipeline_mod (str): Stored pipeline module identifier.
    """
    def __init__(
        self,
        job_vars: JobVariables,
        workflow_params: WorkflowParams,
        textract_client: BaseClient,
        spark: SparkSession,
        glue_client: BaseClient,
        pipeline_mod: str
    ) -> None:
        self.job_vars = job_vars
        self.workflow_params = workflow_params
        self.textract_client = textract_client
        self.spark = spark
        self.glue_client = glue_client
        self.pipeline_mod = pipeline_mod

    @abstractmethod
    def extract_text(self, image_path: str) -> str:
        """
        This method is intentionally abstract so each child extractor can define
        its own OCR strategy.
        """
        pass

    @staticmethod
    @log_execution
    def find_polygon_center(polygon_points: list[dict[str, float]]) -> tuple[float, float]:
        """
        Compute the center point of a 4-vertex polygon (Textract bounding polygon).
        Textract returns geometry polygons as a list of points with normalized
        coordinates in the range [0, 1]. This helper computes the centroid by
        averaging the X and Y coordinates.

        Args:
            polygon_points (list[dict[str, float]]): Polygon vertices. Expected to
                contain exactly 4 dict items with keys "X" and "Y".

        Returns:
            tuple[float, float]: Center point as (center_x, center_y), rounded to 4 decimals.
        """

        if not polygon_points or len(polygon_points) != 4:
            raise ValueError("Polygon must have exactly 4 points")
        
        # Calculate the average of all X coordinates
        center_x = sum(point['X'] for point in polygon_points) / len(polygon_points)
        
        # Calculate the average of all Y coordinates
        center_y = sum(point['Y'] for point in polygon_points) / len(polygon_points)

        return (round(center_x, 4), round(center_y, 4))

    @log_execution
    def textract_api_call(
            textract_client: BaseClient, incoming_bucket: str, image_keys: list[str]
        ) -> list[tuple[str, tuple[float, float]]]:
        """
        This method retrieves OCR output from Textract for the provided S3 objects
        and extracts only LINE blocks. Each line is paired with the center point of
        its polygon (computed via `find_polygon_center`) for each image in the list.
        Results are accumulated across all provided image keys.

        Args:
            textract_client (BaseClient): Boto3 Textract client.
            incoming_bucket (str): S3 bucket containing the image/document.
            image_keys (list[str]): List of S3 object keys for the images.

        Returns:
            list[tuple[str, tuple[float, float]]]: A list of (line_text, (center_x, center_y))
                tuples for each LINE block found by Textract for all images in the list.
        """
        result = []
        for key in image_keys:
            log.info(f"Calling Textract for image key: {key}")
            response = textract_client.detect_document_text(
                Document={
                    'S3Object': {
                        'Bucket': incoming_bucket,
                        'Name': key
                    }
                }
            )
            log.info(
                f"Response from Textract API for image '{key}':\n"
                f"{json.dumps(response, indent=2)}"
            )
            blocks = response['Blocks']
            initial_text = [
                (block['Text'], OcrExtractor.find_polygon_center(block['Geometry']['Polygon']))
                for block in blocks if block['BlockType'] == 'LINE'
            ]
            result += initial_text

            log.info("A response was obtained from the 'detect_document_text' Textract API call...")
        return result

    @log_execution
    def get_process_table_from_catalog(self) -> DataFrame:
        """
        Read the incoming/process dataset from Glue Catalog filtered by workflow identifiers.

        Returns:
            pyspark.sql.DataFrame: Spark DataFrame containing:
                - image_path
                - batch_name
                - transaction_id
        """
        try:
            catalog_db = self.job_vars.catalog_db
            incoming_catalog_table = self.job_vars.incoming_catalog_table
            query = f"""
            SELECT
                _incoming_table.image_path,
                _incoming_table.batch_name,
                _incoming_table.transaction_id
            FROM
                `{catalog_db}`.`{incoming_catalog_table}` AS _incoming_table
            WHERE
                _incoming_table.batch_name = '{self.workflow_params.batch_name}' and
                _incoming_table.transaction_id = '{self.workflow_params.transaction_id}'
            """

            log.info(f"The query to be executed is:\n\n{query}\n\n")
            query_result = self.spark.sql(query)

            log.info("Success! Data was read from Glue Catalog.")
            return query_result

        except Exception as e:
            log.error(
                f"Critical error in the 'get_process_table_from_catalog' function querying the\n"
                f"table '{catalog_db}.{incoming_catalog_table}':\n{str(e)}\n"
            )
            log.error(f"Error type: {type(e).__name__}")
            exit("OCR - Critical error")

    @log_execution
    def get_glue_table_schema(self, glue_catalog: GlueCatalog) -> list:
        """
        Retrieve the ordered column list for a Glue table (including partition keys).

        Args:
            glue_catalog (GlueCatalog): Target catalog metadata containing:
                - catalog_db
                - catalog_table

        Returns:
            list[str]: Ordered column names: non-partition columns followed by partition keys.
        """
        try:
            log.info(
                "Fetching schema for table:\n"
                f"'{glue_catalog.catalog_db}.{glue_catalog.catalog_table}'"
            )

            response = self.glue_client.get_table(
                DatabaseName=glue_catalog.catalog_db,
                Name=glue_catalog.catalog_table,
            )
            columns = response["Table"]["StorageDescriptor"]["Columns"]
            partition_keys = response["Table"].get("PartitionKeys", [])
            ordered_columns = (
                [_col["Name"] for _col in columns] + [p["Name"] for p in partition_keys]
            )

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
            exit("OCR - Critical error")

    @staticmethod
    @log_execution
    def validate_and_prepare_dataframe(
        final_sdf: DataFrame,
        expected_columns: list[str]
    ) -> DataFrame:
        """
        Validate and align a Spark DataFrame to a required schema (by column names).

        Args:
            final_sdf (DataFrame): Input Spark DataFrame to validate.
            expected_columns (list[str]): Target column order as returned from Glue.

        Returns:
            DataFrame: DataFrame containing exactly `expected_columns` in order.
        """

        set_expected_cols = set(expected_columns)
        set_input_cols = set(final_sdf.columns)

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
            log.warning(f"Missing columns in input DataFrame:\n{missing_columns}")

            for col in missing_columns:
                final_sdf = final_sdf.withColumn(col, lit('null').cast(StringType()))

        else:
            log.info(
                "There are not missing columns in the input dataframe with respect to the\n"
                "required or expected columns."
            )

        return final_sdf.select(*expected_columns)

    @log_execution
    def write_to_glue_catalog(self, final_sdf: DataFrame, glue_catalog: GlueCatalog) -> None:
        """
        Write a Spark DataFrame into a Glue Catalog table as Parquet (append mode).

        Args:
            final_sdf (DataFrame): Spark DataFrame to be written.
            glue_catalog (GlueCatalog): Target catalog metadata containing:
                - catalog_db
                - catalog_table

        Returns:
            None
        """
        target_db = glue_catalog.catalog_db
        target_table = glue_catalog.catalog_table
        log.info(f"Writing to Glue: '{target_db}.{target_table}'")

        expected_columns = self.get_glue_table_schema(glue_catalog)
        prepared_df = OcrExtractor.validate_and_prepare_dataframe(final_sdf, expected_columns)

        log.info(f"Writing '{prepared_df.count()}' records to '{target_db}.{target_table}'")
        log.info(f"Final columns are:\n{prepared_df.columns}")

        (
            prepared_df.coalesce(1)
            .write.mode("append")
            .format('parquet')
            .insertInto(f"`{target_db}`.`{target_table}`")
        )

        log.info("Successfully wrote to Glue catalog")

    @staticmethod
    @log_execution
    def get_images_keys(
        df: pd.DataFrame, source_col: str, gray_images_folder: str, rotated_images: bool = False
    ) -> list[list[str]]:
        """
        Generate grayscale (and optionally rotated) image S3 keys
        based on original image paths stored in a DataFrame column.

        Args:
            df (pd.DataFrame): Input DataFrame containing original image paths.
            source_col (str): Column name with original S3 image paths.
            gray_images_folder (str): Target folder prefix for grayscale images.
            rotated_images (bool, optional): If True, include rotated image
                variants (_rot90, _rot180, _rot270). Defaults to False.

        Returns:
            list[list[str]]: A list of lists containing generated S3 image keys.
                Each inner list corresponds to one original image and may contain
                either one path (grayscale only) or multiple paths (grayscale + rotations).
        """
        gray_imgs_paths = (
            df[source_col].str
            .replace("RadixDecompressed/", f"{gray_images_folder}/", regex=False)
            .tolist()
        )

        if rotated_images:
            result = []
            for path in gray_imgs_paths:
                rotations = ["", "_rot90", "_rot180", "_rot270"]
                path_list = []
                original_ext = path.rsplit(".", 1)[-1] 
                ext = "JPEG"
                for rot in rotations:
                    path_list.append(path.replace(f".{original_ext}", f"{rot}.{ext}"))
                result.append(path_list)
        else:
            result = [[path] for path in gray_imgs_paths]

        return result

    @staticmethod
    @log_execution
    def safe_textract_api_columnar_operation(
            df: pd.DataFrame,
            source_col: str,
            new_col: str,
            func: Callable,
            error_value: str ="Could Not be Processed",
            max_workers: int =10,
            chunk_size: int =50,
            rotated_images: bool = False,
            **kwargs
        ) -> pd.DataFrame:

        """
        Apply a Textract-based OCR function safely over a pandas column using multithreading.

        This helper:
          - Reads values from `source_col` (typically S3 keys/paths).
          - Executes `func(textract_client, incoming_bucket, value)` for each row.
          - Processes work in chunks to limit concurrency pressure.
          - Captures and logs exceptions per-row without failing the entire job.
          - Produces:
              - `new_col`: concatenated "liquid text" (joined LINE strings)
              - `ocr_spatial_info`: JSON dump of the raw [(text, (x,y)), ...] output

        Args:
            df (pd.DataFrame): Input pandas DataFrame containing the source column.
            source_col (str): Column containing the values to process.
            new_col (str): Output column where extracted "liquid text" will be stored.
            func (Callable): OCR function to execute. Expected signature:
                (textract_client, incoming_bucket, image_key) -> list[(text, (x,y))].
            error_value (str): Fallback value used when a row cannot be processed.
            max_workers (int): Maximum number of worker threads per chunk.
            chunk_size (int): Number of rows processed per chunk.
            **kwargs: Extra keyword args used to supply dependencies.

        Returns:
            pd.DataFrame: Updated DataFrame with `new_col` and `ocr_spatial_info` populated.
        """

        def safe_wrapper(textract_client: BaseClient, incoming_bucket: str, value: str):

            try:
                result = func(textract_client, incoming_bucket, value)
                return result if result is not None else error_value

            except Exception as e:
                log.error(f"Error in the 'textract_api_call':\n{str(e)}\n")
                log.error(f"Error type: {type(e).__name__}\n")
                # Possible Timestream call
                return error_value

        log.info("Applying the OCR function to all the rows using multithreading...")

        gray_images_folder = "Onb_Gray_Images" if not rotated_images else "Onb_Gray_Images/Rotated"

        values = OcrExtractor.get_images_keys(df, source_col, gray_images_folder, rotated_images)
        ocr_liquid_text_list = [None] * len(values)
        ocr_raw_results_list = [None] * len(values)
        total_chunks = (len(values) - 1) // chunk_size + 1  # Calculate total chunks

        textract_client = kwargs.get("textract_client")
        incoming_bucket = kwargs.get("incoming_bucket")

        for chunk_start in range(0, len(values), chunk_size):
            chunk_end = min(chunk_start + chunk_size, len(values))
            chunk_values = values[chunk_start:chunk_end]
            
            current_chunk = chunk_start // chunk_size + 1
            rows_in_chunk = chunk_end - chunk_start
            
            log.info(f"Processing chunk {current_chunk}/{total_chunks}\n"
                    f"(rows {chunk_start}-{chunk_end-1}, {rows_in_chunk} rows)\n")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

                future_to_index = {
                    executor.submit(
                        safe_wrapper,
                        textract_client,
                        incoming_bucket,
                        value
                    ): chunk_start + idx 
                    for idx, value in enumerate(chunk_values)
                }

                # Collecting results as they complete
                for future in concurrent.futures.as_completed(future_to_index):
                    try:
                        idx = future_to_index[future]
                        ocr_raw_result = future.result()  # Get the raw result
                        unique_texts = set(text for text, _ in ocr_raw_result if text)
                        liquid_text = " ".join(unique_texts)

                        ocr_liquid_text_list[idx] = liquid_text
                        ocr_raw_results_list[idx] = json.dumps(ocr_raw_result)
                    except Exception as e:
                        log.error(
                            f"An error occurred while retrieving the results from futures: {e}"
                        )
                        ocr_liquid_text_list[idx] = error_value
                        ocr_raw_results_list[idx] = error_value

        df[new_col] = ocr_liquid_text_list
        df["ocr_spatial_info"] = ocr_raw_results_list
        return df

    @log_execution
    def pd_to_sparf_df(self, process_df: DataFrame) -> DataFrame:
        """
        Convert a pandas DataFrame to a Spark DataFrame using an all-string schema.

        Args:
            process_df (pd.DataFrame): Pandas DataFrame to convert.

        Returns:
            DataFrame: Spark DataFrame with all columns as StringType.
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
    def get_pd_df_and_display_info(self, df: DataFrame) -> pd.DataFrame:
        """
        Convert an input Spark DataFrame to pandas, log diagnostics, and emit a Timestream 
        "Ready" event.

        Args:
            df (pyspark.sql.DataFrame): Input Spark DataFrame.

        Returns:
            pd.DataFrame: Converted pandas DataFrame.
        """
        process_df = df.toPandas()

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
                state="Ready",
                no_of_asset=str(len(process_df))
            )
        )

        log.info(
            f"Successfully read data!\n\n"
            f"Shape for input frame:\n{process_df.shape}\n\n"
            f"First 10 rows:\n{process_df.head(10).to_string()}\n\n"
            f"Input types:\n{process_df.dtypes}\n"
            f"Original columns:\n{process_df.columns.tolist()}\n"
        )

        log.info("Checking if the Pandas Dataframe is empty or not...")

        if process_df.empty:
            raise ValueError("¡No data to write or DataFrame is empty or invalid!...")
        
        log.info("Pandas Dataframe is not empty.")
        return process_df

    @log_execution
    def processed_rows_to_timestream(self, final_df: DataFrame) -> None:
        """
        Emit per-row "Complete" events to Timestream after OCR processing.

        Args:
            final_df (pyspark.sql.DataFrame): Final Spark DataFrame containing processed rows.

        Returns:
            None
        """
        processed_rows = final_df.sort("image_path").collect()
        for row in processed_rows:
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
                    no_of_asset="1",
                    file_path=str(row['image_path']),
                    event_name="ImageTextExtract"
                )
            )

    @log_execution
    def get_final_df(self) -> DataFrame:
        """
        Orchestrate the OCR pipeline and return the processed Spark DataFrame.

        Returns:
            DataFrame: Final Spark DataFrame including OCR outputs:
                - ocr_liquid_text
                - ocr_spatial_info
        """
        input_df = self.get_process_table_from_catalog()
        pd_input_df = self.get_pd_df_and_display_info(input_df)
        process_df = OcrExtractor.safe_textract_api_columnar_operation(
            pd_input_df,
            'image_path',
            'ocr_liquid_text',
            OcrExtractor.textract_api_call,
            error_value="Could Not be Processed",
            max_workers=10,
            chunk_size=50,
            textract_client=self.textract_client,
            incoming_bucket=self.workflow_params.incoming_bucket,
            rotated_images=True
        )

        log.info("The OCR function was applied to all targeted documents....")
        log.info(
            f"Showing the first 10 rows out of the processed dataframe:\n"
            f"{process_df.head(10)}"
        )

        final_sdf = self.pd_to_sparf_df(process_df)
        return final_sdf


class RadixOcrExtractor(OcrExtractor):
    """
    OCR extractor implementation for the Radix pipeline.

    This concrete extractor uses the shared OCR pipeline implemented in `OcrExtractor`
    to:
      - Build the final OCR Spark DataFrame (reading from the incoming table and
        applying Textract over the `image_path` column).
      - Emit per-row completion events to Timestream.
      - Write the enriched OCR results into the enrichment Glue Catalog table.
      - Emit a final aggregate completion event to Timestream including the number
        of processed assets.

    Args:
        job_vars (JobVariables): Job configuration container with Glue/Timestream metadata.
        workflow_params (WorkflowParams): Workflow runtime parameters.
        textract_client (BaseClient): Boto3 Textract client used to call Textract APIs.
        spark (SparkSession): Active Spark session used for Spark SQL and DataFrame operations.
        glue_client (BaseClient): Boto3 Glue client used to retrieve table schemas.
        pipeline_mod (str): Pipeline module identifier used for audit/metrics.

    Inherits:
        OcrExtractor: Provides the shared OCR pipeline building blocks.
    """
    def __init__(
        self,
        job_vars: JobVariables,
        workflow_params: WorkflowParams,
        textract_client: BaseClient,
        spark: SparkSession,
        glue_client: BaseClient,
        pipeline_mod: str
    ) -> None:
        """
        Initialize the Radix OCR extractor.

        Args:
            job_vars (JobVariables): Job configuration container.
            workflow_params (WorkflowParams): Workflow runtime parameters.
            textract_client (BaseClient): Boto3 Textract client.
            spark (SparkSession): Spark session.
            glue_client (BaseClient): Boto3 Glue client.
            pipeline_mod (str): Pipeline module identifier.
        """
        super().__init__(
            job_vars,
            workflow_params,
            textract_client,
            spark,
            glue_client,
            pipeline_mod
        )

    @log_execution
    def extract_text(self) -> None:
        """
        Run the Radix OCR extraction workflow end-to-end.

        Args:
            None

        Returns:
            None

        Side Effects:
            - Reads from the incoming Glue Catalog table (via the base class).
            - Calls Textract for OCR processing (via the base class).
            - Writes processed records to the enrichment Glue Catalog table.
            - Publishes per-row and aggregate events to Timestream (via SQS queue).
        """
        final_sdf = self.get_final_df()
        self.processed_rows_to_timestream(final_sdf)
        enrichment_catalog_data = GlueCatalog(
            self.job_vars.catalog_db, self.job_vars.enrichment_catalog_table
        )
        self.write_to_glue_catalog(final_sdf, enrichment_catalog_data)

        final_record_count = final_sdf.count()
        log.info(f"Records Written in the OcrImage Module: {final_record_count}")
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
                no_of_asset=str(final_record_count)
            )
        )


class HHDOcrExtractor(OcrExtractor):
    """
    OCR extractor implementation for the HHD pipeline.

    This concrete extractor reuses the shared OCR pipeline implemented in `OcrExtractor`
    to:
      - Build the final OCR Spark DataFrame (reading from the incoming table and
        applying Textract over the `image_path` column).
      - Aggregate OCR outputs into a higher-level dataset via `aggregate_data`.
      - Emit per-row completion events to Timestream.
      - Write OCR-enriched records into the enrichment Glue Catalog table.
      - Emit a final aggregate completion event to Timestream including the number
        of processed assets.

    Args:
        job_vars (JobVariables): Job configuration container with Glue/Timestream metadata.
        workflow_params (WorkflowParams): Workflow runtime parameters.
        textract_client (BaseClient): Boto3 Textract client used to call Textract APIs.
        spark (SparkSession): Active Spark session used for Spark SQL and DataFrame operations.
        glue_client (BaseClient): Boto3 Glue client used to retrieve table schemas.
        pipeline_mod (str): Pipeline module identifier used for audit/metrics.

    Inherits:
        OcrExtractor: Provides the shared OCR pipeline building blocks.
    """
    def __init__(
        self,
        job_vars: JobVariables,
        workflow_params: WorkflowParams,
        textract_client: BaseClient,
        spark: SparkSession,
        glue_client: BaseClient,
        pipeline_mod: str
    ) -> None:
        """
        Initialize the HHD OCR extractor.

        Args:
            job_vars (JobVariables): Job configuration container.
            workflow_params (WorkflowParams): Workflow runtime parameters.
            textract_client (BaseClient): Boto3 Textract client.
            spark (SparkSession): Spark session.
            glue_client (BaseClient): Boto3 Glue client.
            pipeline_mod (str): Pipeline module identifier.
        """
        super().__init__(
            job_vars,
            workflow_params,
            textract_client,
            spark,
            glue_client,
            pipeline_mod
        )

    @log_execution
    def aggregate_data(self) -> DataFrame:
        log.info("This logic is pending to be implemented by Shivam")
        pass

    @log_execution
    def extract_text(self) -> None:
        """
        Run the HHD OCR extraction workflow end-to-end.

        Args:
            None

        Returns:
            None

        Side Effects:
            - Reads from the incoming Glue Catalog table (via the base class).
            - Calls Textract for OCR processing (via the base class).
            - Writes processed records to the enrichment Glue Catalog table.
            - Publishes per-row and aggregate events to Timestream (via SQS queue).
        """
        final_sdf = self.get_final_df()

        # aggregated_df = self.aggregate_data()

        self.processed_rows_to_timestream(final_sdf)
        enrichment_catalog_data = GlueCatalog(
            self.job_vars.catalog_db, self.job_vars.enrichment_catalog_table
        )
        self.write_to_glue_catalog(final_sdf, enrichment_catalog_data)

        final_record_count = final_sdf.count()
        log.info(f"Records Written in the OcrImage Module: {final_record_count}")
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
                no_of_asset=str(final_record_count)
            )
        )


class FactoryOcrExtractor:
    """
    Factory class responsible for instantiating the appropriate OCR extractor
    implementation based on workflow metadata.

    This class centralizes the decision logic that maps a given `workflow_params.zip_key`
    to a concrete `OcrExtractor` implementation. It ensures that the calling code
    remains decoupled from specific extractor classes such as:

      - `RadixOcrExtractor`
      - `HHDOcrExtractor`

    By using this factory, the OCR module can be extended with new extractor
    implementations without modifying the orchestration logic: only the factory
    needs to be updated.
    """

    @staticmethod
    @log_execution
    def get_ocr_extractor(
        job_vars: JobVariables,
        workflow_params: WorkflowParams,
        textract_client: BaseClient,
        spark: SparkSession,
        glue_client: BaseClient,
        pipeline_mod: str
    ) -> OcrExtractor:
        """
        Return the appropriate OCR extractor implementation based on `zip_key`.

        Args:
            job_vars (JobVariables): Job configuration container with Glue metadata.
            workflow_params (WorkflowParams): Workflow runtime parameters.
            textract_client (BaseClient): Boto3 Textract client.
            spark (SparkSession): Active Spark session.
            glue_client (BaseClient): Boto3 Glue client.
            pipeline_mod (str): Pipeline module identifier used for auditing/metrics.

        Returns:
            OcrExtractor: A concrete implementation of `OcrExtractor`
                (e.g., `RadixOcrExtractor` or `HHDOcrExtractor`).
        """
        if "radix-onboarding-indexed" in workflow_params.zip_key:
            log.info("Proceeding with the OCR extractor for Radix...\n")
            return RadixOcrExtractor(
                job_vars=job_vars,
                workflow_params=workflow_params,
                textract_client=textract_client,
                spark=spark,
                glue_client=glue_client,
                pipeline_mod=pipeline_mod
            )
        elif "tr-image-extract" in workflow_params.zip_key:
            log.info("Proceeding with the OCR extractor for HHD...\n")
            return HHDOcrExtractor(
                job_vars=job_vars,
                workflow_params=workflow_params,
                textract_client=textract_client,
                spark=spark,
                glue_client=glue_client,
                pipeline_mod=pipeline_mod
            )
        else:
            raise ValueError(f"Unsupported zip_key type: {workflow_params.zip_key}")


class GlueJobExecution(GlueJobBase):
    """
    AWS Glue job entrypoint for the OCR Image module.

    This class orchestrates the end-to-end execution of the OCR workflow inside
    an AWS Glue job.

    Args:
        pipeline_mod (str): Pipeline module identifier used for audit/telemetry.
    """
    def __init__(self, pipeline_mod):
        """
        Initialize the Glue job execution context and AWS clients.

        This constructor:
          - Calls `GlueJobBase.__init__` to initialize shared Glue runtime components.
          - Creates a Textract client with adaptive retries (max_attempts=10).

        Args:
            pipeline_mod (str): Pipeline module identifier used for telemetry and logging.
        """
        super().__init__(pipeline_mod)
        config = Config(
            retries={
                'max_attempts': 10,
                'mode': 'adaptive'
            }
        )
        self.textract_client = boto3.client('textract', config=config)
        self.pipeline_mod = pipeline_mod

    @log_execution
    def main(self):
        try:
            args = getResolvedOptions(
                sys.argv,
                [
                    'JOB_NAME',
                    'WORKFLOW_NAME',
                    'WORKFLOW_RUN_ID',
                    'CatalogDB',
                    'IncomingCatalogTable',
                    'EnrichmentCatalogTable',
                    'TimestreamDB',
                    'TimestreamTable',
                    'TimestreamSQSQueue'
                ]
            )

            job_vars = JobVariables(args)
            self.job.init(job_vars.job_name, args)
            self.log.info(f"Job Variables:\n{job_vars}")

            workflow_params = self.get_workflow_params(job_vars)
            self.log.info(f"Workflow params:\n{workflow_params}")

            ocr_extractor = FactoryOcrExtractor.get_ocr_extractor(
                job_vars=job_vars,
                workflow_params=workflow_params,
                textract_client=self.textract_client,
                spark=self.spark,
                glue_client=self.glue_client,
                pipeline_mod=self.pipeline_mod
            )
            ocr_extractor.extract_text()
            log.info("Job completed successfully")

        except Exception as e:
            log.error(f"Critical error in the 'main' function:\n{str(e)}")
            log.error(f"Error type: {type(e).__name__}")

            Onboarding_Timestream_Manager.timestream_insert_data(
                db=job_vars.timestream_db,
                table=job_vars.timestream_table,
                timestream_queue=job_vars.timestream_sqs_queue,
                measure_name=f'OnboardingWF_{self.pipeline_mod}',
                content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                    source=workflow_params.source,
                    source_device=workflow_params.source_device,
                    transaction_id=workflow_params.transaction_id,
                    batch_name=workflow_params.batch_name,
                    pipeline_mod=self.pipeline_mod,
                    error=str(e),
                    error_code="42",
                    record_count="0"
                )
            )
            raise RuntimeError("General process failed") from e

        finally:
            self.job.commit()


if __name__ == "__main__":
    glue_job = GlueJobExecution("OcrImage")
    glue_job.main()
