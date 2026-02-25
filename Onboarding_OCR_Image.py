from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit
from awsglue.utils import getResolvedOptions
from datetime import datetime
import json
import sys
import boto3
from botocore.config import Config
import concurrent.futures
import Onboarding_Log_Manager
import pandas as pd
import Onboarding_Timestream_Manager
from Onboarding_Glue_Job_Base import GlueJobBase, JobVariables, WorkflowParams
from Onboarding_Configuration_Template import GlueCatalog
from pyspark.sql import SparkSession, DataFrame
from botocore.client import BaseClient
from abc import ABC, abstractmethod
from typing import Callable

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 200)

pipeline_mod = 'OcrImage'
log = Onboarding_Log_Manager.get_module_logger(pipeline_mod)
log.info('OnboardingOcrImage module')

step_name = "OCR_Image"
execution_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
next_step = "Extract_Text"

log.info(f"""
Step Name: {step_name}
Execution Date: {execution_date}
Next Step: {next_step}
""")

class OcrExtractor(ABC):
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
        pass

    @staticmethod
    def find_polygon_center(polygon_points: list[dict[str, float]]) -> tuple[float, float]:
        """Find the center point of a polygon defined by 4 coordinates"""

        if not polygon_points or len(polygon_points) != 4:
            raise ValueError("Polygon must have exactly 4 points")
        
        # Calculate the average of all X coordinates
        center_x = sum(point['X'] for point in polygon_points) / len(polygon_points)
        
        # Calculate the average of all Y coordinates
        center_y = sum(point['Y'] for point in polygon_points) / len(polygon_points)

        return (round(center_x, 4), round(center_y, 4))

    def textract_api_call(
            textract_client: BaseClient, incoming_bucket: str, image_key: str
        ) -> list[tuple[str, tuple[float, float]]]:
        """
        Function to call the 'detect_document_text' Textract API to retrieve OCR liquid text,
        out of the file images from the customers
        """
        response = textract_client.detect_document_text(
            Document={
                'S3Object': {
                    'Bucket': incoming_bucket,
                    'Name': image_key
                }
            }
        )
        blocks = response['Blocks']
        initial_text = [
            (block['Text'], OcrExtractor.find_polygon_center(block['Geometry']['Polygon']))
            for block in blocks if block['BlockType'] == 'LINE'
        ]

        log.info("A response was obtained from the 'detect_document_text' Textract API call...")
        return initial_text

    def get_process_table_from_catalog(self) -> DataFrame:
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

    def get_glue_table_schema(self, glue_catalog: GlueCatalog) -> list:
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
    def validate_and_prepare_dataframe(
        final_sdf: DataFrame,
        expected_columns: list[str]
    ) -> DataFrame:
        """Validate and prepare DataFrame with required columns."""

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

    def write_to_glue_catalog(self, final_sdf: DataFrame, glue_catalog: GlueCatalog) -> None:
        """Write DataFrame to Glue catalog."""
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
    def safe_textract_api_columnar_operation(
            df: pd.DataFrame,
            source_col: str,
            new_col: str,
            func: Callable,
            error_value: str ="Could Not be Processed",
            max_workers: int =10,
            chunk_size: int =50,
            **kwargs
        ) -> pd.DataFrame:

        """Apply safety function wrapping Textract API call with multithreading"""

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

        values = df[source_col].tolist()
        ocr_liquid_text_list = [None] * len(values)
        ocr_raw_results_list = [None] * len(values)
        total_chunks = (len(values) - 1) // chunk_size + 1  # Calculate total chunks

        textract_client = kwargs.get('textract_client')
        incoming_bucket = kwargs.get('incoming_bucket')

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
                        liquid_text = ' '.join([text for text, _ in ocr_raw_result])

                        ocr_liquid_text_list[idx] = liquid_text
                        ocr_raw_results_list[idx] = json.dumps(ocr_raw_result)
                    except Exception as e:
                        log.error(
                            f"An error occurred while retrieving the results from futures: {e}"
                        )
                        ocr_liquid_text_list[idx] = error_value
                        ocr_raw_results_list[idx] = error_value

        df[new_col] = ocr_liquid_text_list
        df['ocr_spatial_info'] = ocr_raw_results_list
        return df

    def pd_to_sparf_df(self, process_df: DataFrame) -> DataFrame:
        """Create a Spark DataFrame from a pandas one."""

        all_columns = process_df.columns.tolist()

        log.info("Creating the Spark Dataframe out from the Pandas Dataframe...")

        schema = StructType(
            [StructField(column_name, StringType(), nullable=True) for column_name in all_columns]
        )

        final_spark_df = self.spark.createDataFrame(process_df, schema=schema)

        log.info("Spark Dataframe created.")

        return final_spark_df

    def get_pd_df_and_display_info(self, df: DataFrame) -> pd.DataFrame:
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

    def processed_rows_to_timestream(self, final_df: DataFrame) -> None:
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
            
    def get_final_df(self) -> DataFrame:
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
            incoming_bucket=self.workflow_params.incoming_bucket
        )

        log.info("The OCR function was applied to all targeted documents....")
        log.info(
            f"Showing the first 10 rows out of the processed dataframe:\n"
            f"{process_df.head(10)}"
        )

        final_sdf = self.pd_to_sparf_df(process_df)
        return final_sdf


class RadixOcrExtractor(OcrExtractor):
    def __init__(
        self,
        job_vars: JobVariables,
        workflow_params: WorkflowParams,
        textract_client: BaseClient,
        spark: SparkSession,
        glue_client: BaseClient,
        pipeline_mod: str
    ) -> None:
        super().__init__(
            job_vars,
            workflow_params,
            textract_client,
            spark,
            glue_client,
            pipeline_mod
        )

    def extract_text(self) -> None:
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
    def __init__(
        self,
        job_vars: JobVariables,
        workflow_params: WorkflowParams,
        textract_client: BaseClient,
        spark: SparkSession,
        glue_client: BaseClient,
        pipeline_mod: str
    ) -> None:
        super().__init__(
            job_vars,
            workflow_params,
            textract_client,
            spark,
            glue_client,
            pipeline_mod
        )

    def aggregate_data(self) -> DataFrame:
        log.info("This logic is pending to be implemented by Shivam")
        pass

    def extract_text(self) -> None:
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
    @staticmethod
    def get_ocr_extractor(
        job_vars: JobVariables,
        workflow_params: WorkflowParams,
        textract_client: BaseClient,
        spark: SparkSession,
        glue_client: BaseClient,
        pipeline_mod: str
    ) -> OcrExtractor:
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
    def __init__(self, pipeline_mod):
        super().__init__(pipeline_mod)
        config = Config(
            retries={
                'max_attempts': 10,
                'mode': 'adaptive'
            }
        )
        self.textract_client = boto3.client('textract', config=config)
        self.pipeline_mod = pipeline_mod

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
                measure_name=f'OnboardingWF_{pipeline_mod}',
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
