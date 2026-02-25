
import sys
import pandas as pd
import logging
import json
import re
import difflib
import concurrent.futures
import boto3
import textwrap
import time
import os
import numpy as np
from awsglue.utils import getResolvedOptions
from Onboarding_Glue_Job_Base import GlueJobBase, JobVariables, WorkflowParams
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import datetime, timedelta, timezone
from botocore.config import Config
from botocore.client import BaseClient
from functools import wraps
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
from whoosh import scoring

from Onboarding_Configuration_Template import (
    Transaction,
    S3Location,
    GlueCatalog,
    ConfigurationTemplate,
    CollectionNotFound,
    ConfigurationNotFound,
)
from Onboarding_Timestream_Manager import (
    timestream_insert_data,
    create_timestream_content
)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 200)

def log_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"Starting the execution of {func.__name__} function\n")
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            logging.info(f"Finishing the execution of {func.__name__} function\n")
    return wrapper


class Scorer(ABC):
    @abstractmethod
    def calculate_score(self):
        pass

class WithConfigurationScorer(Scorer):
    def __init__(
            self,
            collection_config: dict,
            batch_name: str,
            transaction_id: str,
            pipeline_mod: str,
            job_vars: JobVariables,
            workflow_vars: WorkflowParams,
            incoming_catalog_data: GlueCatalog,
            enrichment_catalog_data: GlueCatalog,
            spark: SparkSession,
            glue_client: BaseClient,
            # Add explicit args for Reference (SSOT) Catalog
            # Assuming these are passed via job_vars or extended args handling
            reference_catalog_data: GlueCatalog = None
    ):
        self.collection_config = collection_config
        self.batch_name = batch_name
        self.transaction_id = transaction_id
        self.pipeline_mod = pipeline_mod
        self.job_vars = job_vars
        self.workflow_vars = workflow_vars
        self.incoming_catalog_data = incoming_catalog_data
        self.enrichment_catalog_data = enrichment_catalog_data
        self.spark = spark
        self.glue_client = glue_client
        self.reference_catalog_data = reference_catalog_data
        self.index_dir = '/tmp/reference_index'

    @log_execution
    def get_process_table_from_catalog(
        self,
        glue_catalog: GlueCatalog
    ) -> DataFrame:
        """
        Retrieve data from the Glue catalog for the current batch.
        Assuming we read from the 'incoming' table which contains extracted data.
        """
        try:
            db = glue_catalog.catalog_db
            table = glue_catalog.catalog_table
            # Query might need adjustment based on upstream output structure
            # Assuming upstream output (ExtractText) writes key_value_pairs or similar
            query = f"""
            SELECT *
            FROM
                `{db}`.`{table}` AS _source_table
            WHERE
                batch_name = '{self.batch_name}' and
                transaction_id = '{self.transaction_id}'
            """

            logging.info(f"The query to be executed is:\n\n{query}\n\n")
            query_result = self.spark.sql(query)
            logging.info("Success! Data was read from Glue Catalog.")
            return query_result

        except Exception as e:
            logging.error(
                "Critical error in the 'get_process_table_from_catalog'\n"
                f"function querying the table '{db}.{table}':\n{str(e)}\n"
            )
            logging.error(f"Error type: {type(e).__name__}")
            exit("CalculateScore module - Critical error")

    @log_execution
    def get_pd_df_and_display_info(
        self,
        spark_df: DataFrame
    ) -> pd.DataFrame:
        process_df = spark_df.toPandas()
        logging.info(
            f"Successfully read data!\n\n"
            f"Shape for input frame:\n{process_df.shape}\n\n"
            f"First 10 rows:\n{process_df.head(10).to_string()}\n\n"
        )

        if process_df.empty:
            logging.warning("Input DataFrame is empty.")
            # We allow empty frames to flow through to complete the job cleanly

        return process_df

    @log_execution
    def load_and_index_reference_data(self, fields_to_index, customer_id):
        """
        Loads reference data from SSOT Glue Catalog and builds a local Whoosh index.
        """
        if not self.reference_catalog_data:
            logging.warning("Reference Catalog Data not provided. Skipping indexing.")
            return None

        ssot_db = self.reference_catalog_data.catalog_db
        ssot_table = self.reference_catalog_data.catalog_table

        log_execution(logging.info(f"Indexing reference data for customer: {customer_id} from {ssot_db}.{ssot_table}"))

        if not os.path.exists(self.index_dir):
            os.mkdir(self.index_dir)

        try:
            # Load from Catalog
            # Filtering by customer_id is critical
            query = f"""
            SELECT * FROM `{ssot_db}`.`{ssot_table}`
            WHERE tr_customer_id = '{customer_id}'
            """

            try:
                ref_df_spark = self.spark.sql(query)
            except Exception as e:
                logging.warning(f"SSOT table `{ssot_db}`.`{ssot_table}` query failed: {e}. Indexing skipped.")
                return None

            # Select only necessary columns
            id_cols = ['id', 'ID', 'barcode', 'Barcode', 'ref_id']
            # Only select columns that exist in the dataframe to avoid AnalysisException
            existing_cols = ref_df_spark.columns
            available_cols = [c for c in existing_cols if c in fields_to_index or c in id_cols]

            if not available_cols:
                return None

            # Use unpack operator * for select
            ref_df = ref_df_spark.select(*available_cols).toPandas()
            ref_df = ref_df.fillna('')

            schema = Schema()
            schema.add('ref_id', ID(stored=True))
            for field in fields_to_index:
                schema.add(field, TEXT(stored=True))

            ix = create_in(self.index_dir, schema)
            writer = ix.writer()

            for _, row in ref_df.iterrows():
                doc = {}
                ref_id = 'unknown'
                for col in id_cols:
                    if col in row and row[col]:
                        ref_id = str(row[col])
                        break
                doc['ref_id'] = ref_id

                for field in fields_to_index:
                    if field in row:
                        doc[field] = str(row[field])
                writer.add_document(**doc)

            writer.commit()
            return ix
        except Exception as e:
            logging.error(f"Error indexing reference data: {e}")
            return None

    def find_match(self, ix, record, lookup_fields):
        if not ix:
            return "Skipped", 0.0, None

        searcher = ix.searcher()
        try:
            query_parts = []
            for field in lookup_fields:
                if field in record and record[field]:
                    val = str(record[field])
                    clean_val = val.replace(':', '').replace('"', r'\"')
                    if clean_val:
                        query_parts.append(f'{field}:"{clean_val}"')

            if not query_parts:
                return "NoMatch", 0.0, None

            query_string = " OR ".join(query_parts)
            from whoosh.qparser import MultifieldParser
            parser = MultifieldParser(lookup_fields, schema=ix.schema)
            q = parser.parse(query_string)
            results = searcher.search(q, limit=5)

            if not results:
                return "NoMatch", 0.0, None

            best_score = -1
            best_match_id = None

            for hit in results:
                total_sim = 0
                field_count = 0
                for field in lookup_fields:
                    val_rec = str(record.get(field, '')).lower()
                    val_ref = str(hit.get(field, '')).lower()
                    if val_rec and val_ref:
                        sim = difflib.SequenceMatcher(None, val_rec, val_ref).ratio()
                        total_sim += sim
                        field_count += 1

                avg_score = total_sim / field_count if field_count > 0 else 0
                if avg_score > best_score:
                    best_score = avg_score
                    best_match_id = hit.get('ref_id')

            # Use threshold from config if available (future), default to 0.85
            # Assuming 'match_threshold' might exist in collection config or global config
            # Using get() with default 0.85 on collection_config
            threshold = self.collection_config.get('match_threshold', 0.85)

            if best_score >= threshold:
                return "Matched", best_score, best_match_id
            else:
                return "NoMatch", best_score, None
        finally:
            searcher.close()

    @log_execution
    def process_scoring(self, pd_df, index, lookup_fields):
        results = []
        for _, row in pd_df.iterrows():
            record = row.to_dict()
            # If data is in key_value_pairs column (JSON string), parse it
            if 'key_value_pairs' in record and isinstance(record['key_value_pairs'], str):
                try:
                    kv_data = json.loads(record['key_value_pairs'])
                    # Merge KV data into record for matching
                    record.update(kv_data)
                except:
                    pass

            match_status, match_score, linked_id = self.find_match(index, record, lookup_fields)

            # Enrich
            record['MatchStatus'] = match_status
            record['MatchScore'] = match_score
            record['LinkedLegacyID'] = linked_id

            results.append(record)

        return pd.DataFrame(results)

    @log_execution
    def pd_to_spark(self, pd_df: pd.DataFrame) -> DataFrame:
        # Convert all to string to be safe, except score/status if needed
        # Or rely on Spark inference.
        # Using string for simplicity as per template
        schema_cols = []
        for col in pd_df.columns:
            schema_cols.append(StructField(col, StringType(), True))

        schema = StructType(schema_cols)

        # Replace NaN/None with explicit None to ensure they are treated as nulls in Spark,
        # instead of casting 'nan' or 'None' strings.
        pd_df = pd_df.replace({np.nan: None})

        # Convert to string ONLY where value is not None, preserving None
        # Since astype(str) converts None to 'None', we must handle carefully.
        # Map values to str if not None.
        pd_df = pd_df.applymap(lambda x: str(x) if x is not None else None)

        return self.spark.createDataFrame(pd_df, schema=schema)

    @staticmethod
    @log_execution
    def get_glue_table_schema(
        glue_client: BaseClient,
        glue_catalog: GlueCatalog
    ) -> list:
        try:
            logging.info(
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

            logging.info(
                "Retrieved Glue schema order for table\n"
                f"'{glue_catalog.catalog_db}.{glue_catalog.catalog_table}':\n"
                f"{ordered_columns}"
            )
            return ordered_columns

        except Exception as e:
            logging.error(
                "Critical error in the 'get_glue_table_schema' function for\n"
                f"the table '{glue_catalog.catalog_db}.\n"
                f"{glue_catalog.catalog_table}':\n{str(e)}\n"
                f"Error type: {type(e).__name__}"
            )
            exit("CalculateScore - Critical error")

    @staticmethod
    @log_execution
    def validate_and_prepare_dataframe(
        result_df: DataFrame,
        expected_columns: list
    ) -> DataFrame:
        set_expected_cols = set(expected_columns)
        set_input_cols = set(result_df.columns)

        missing_columns = set_expected_cols - set_input_cols

        if missing_columns:
            logging.warning(f"Missing columns in input DataFrame:\n{missing_columns}")
            for col in missing_columns:
                result_df = result_df.withColumn(
                    col,
                    F.lit("null").cast(StringType()),
                )

        return result_df.select(*expected_columns)

    @log_execution
    def write_to_glue_catalog(self, result_df: DataFrame, glue_catalog: GlueCatalog) -> None:
        target_db = glue_catalog.catalog_db
        target_table_name = glue_catalog.catalog_table

        logging.info(f"Writing to Glue: '{target_db}.{target_table_name}'")

        expected_columns = self.get_glue_table_schema(
            self.glue_client,
            glue_catalog,
        )
        prepared_df = self.validate_and_prepare_dataframe(
            result_df,
            expected_columns,
        )

        logging.info(f"Writing '{prepared_df.count()}' records to '{target_db}.{target_table_name}'")

        (
            prepared_df.coalesce(1)
            .write.mode("overwrite")
            .format("parquet")
            .insertInto(f"`{target_db}`.`{target_table_name}`")
        )

        logging.info("Successfully wrote to Glue catalog")

    def calculate_score(self):
        try:
            # 1. Read Data
            spark_df = self.get_process_table_from_catalog(self.incoming_catalog_data)
            pd_df = self.get_pd_df_and_display_info(spark_df)

            if pd_df.empty:
                return

            # 2. Config Parsing
            customer_id = self.collection_config.get('tr_customer_id')

            all_lookup_fields = set()
            for col in self.collection_config.get('collections', []):
                if col.get('indexing_required'):
                    for field in col.get('field_list', []):
                        if field.get('field_to_look_up'):
                            all_lookup_fields.add(field['FieldName'])

            # 3. Build Index
            index = self.load_and_index_reference_data(list(all_lookup_fields), customer_id)

            # 4. Matching
            processed_df = self.process_scoring(pd_df, index, list(all_lookup_fields))

            # 5. Write Output
            result_spark_df = self.pd_to_spark(processed_df)
            self.write_to_glue_catalog(result_spark_df, self.enrichment_catalog_data)

            # 6. Timestream
            timestream_insert_data(
                db=self.job_vars.timestream_db,
                table=self.job_vars.timestream_table,
                timestream_queue=self.job_vars.timestream_sqs_queue,
                measure_name=f"OnboardingWF_{self.pipeline_mod}",
                content=create_timestream_content(
                    source=self.workflow_vars.source,
                    source_device=self.workflow_vars.source_device,
                    transaction_id=self.transaction_id,
                    batch_name=self.batch_name,
                    pipeline_mod=self.pipeline_mod,
                    state="Complete",
                    no_of_asset=str(processed_df.shape[0]),
                ),
            )
            logging.info("Job completed successfully")

        except Exception as e:
            logging.error(f"Critical error: {e}")
            timestream_insert_data(
                db=self.job_vars.timestream_db,
                table=self.job_vars.timestream_table,
                timestream_queue=self.job_vars.timestream_sqs_queue,
                measure_name=f"OnboardingWF_{self.pipeline_mod}",
                content=create_timestream_content(
                    source=self.workflow_vars.source,
                    source_device=self.workflow_vars.source_device,
                    transaction_id=self.transaction_id,
                    batch_name=self.batch_name,
                    pipeline_mod=self.pipeline_mod,
                    state="Failed",
                    no_of_asset="-",
                ),
            )
            raise

class WithoutConfigurationScorer(Scorer):
    def calculate_score(self):
        logging.warning("Skipping CalculateScore execution - No Configuration")
        return

class FactoryScorer:
    @staticmethod
    def get_scorer(
        batch_name: str,
        transaction_id: str,
        pipeline_mod: str,
        job_vars: JobVariables,
        workflow_vars: WorkflowParams,
        incoming_catalog_data: GlueCatalog,
        enrichment_catalog_data: GlueCatalog,
        reference_catalog_data: GlueCatalog, # Add Reference Catalog
        spark: SparkSession,
        glue_client: BaseClient,
    ) -> Scorer:

        try:
            # Reusing the logic to fetch config from catalog/S3
            location = S3Location(
                workflow_vars.incoming_bucket,
                "Onb_Customer_Conf/",
                "",
            )
            transaction = Transaction(transaction_id, batch_name)
            tr_account_name, collection_config = (
                ConfigurationTemplate.get_collection_config_from_catalog(
                    location,
                    incoming_catalog_data, # Using incoming DB as config source usually
                    transaction,
                    spark,
                    logging,
                )
            )
            logging.info(f"Configuration found: {collection_config is not None}")

            if collection_config:
                return WithConfigurationScorer(
                    collection_config=collection_config,
                    batch_name=batch_name,
                    transaction_id=transaction_id,
                    pipeline_mod=pipeline_mod,
                    job_vars=job_vars,
                    workflow_vars=workflow_vars,
                    incoming_catalog_data=incoming_catalog_data,
                    enrichment_catalog_data=enrichment_catalog_data,
                    reference_catalog_data=reference_catalog_data,
                    spark=spark,
                    glue_client=glue_client,
                )
            else:
                return WithoutConfigurationScorer()

        except (CollectionNotFound, ConfigurationNotFound):
            return WithoutConfigurationScorer()
        except Exception as e:
            logging.error(f"Error determining scorer: {e}")
            # Fail safe to without config or raise?
            return WithoutConfigurationScorer()

class GlueJobExecution(GlueJobBase):
    def __init__(self, pipeline_mod: str):
        super().__init__(pipeline_mod)
        self.spark.conf.set(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic",
        )

    def main(self):
        try:
            # Added ReferenceCatalogDB and ReferenceCatalogTable to args
            args = getResolvedOptions(
                sys.argv,
                [
                    "JOB_NAME",
                    "WORKFLOW_NAME",
                    "WORKFLOW_RUN_ID",
                    "CatalogDB",
                    "IncomingCatalogTable",
                    "EnrichmentCatalogTable",
                    "ReferenceCatalogDB",
                    "ReferenceCatalogTable",
                    "TimestreamDB",
                    "TimestreamTable",
                    "TimestreamSQSQueue",
                ],
            )

            job_vars = JobVariables(args)
            self.job.init(job_vars.job_name, args)
            self.log.info(f"Job Variables:\n{job_vars}")

            workflow_params = self.get_workflow_params(job_vars)

            timestream_insert_data(
                db=job_vars.timestream_db,
                table=job_vars.timestream_table,
                timestream_queue=job_vars.timestream_sqs_queue,
                measure_name=f"OnboardingWF_{self.pipeline_mod}",
                content=create_timestream_content(
                    source=workflow_params.source,
                    source_device=workflow_params.source_device,
                    transaction_id=workflow_params.transaction_id,
                    batch_name=workflow_params.batch_name,
                    state="Ready",
                    no_of_asset="-",
                    pipeline_mod=self.pipeline_mod,
                ),
            )

            incoming_catalog_data = GlueCatalog(
                job_vars.catalog_db,
                job_vars.incoming_catalog_table,
            )
            enrichment_catalog_data = GlueCatalog(
                job_vars.catalog_db,
                job_vars.enrichment_catalog_table,
            )

            # Accessing from 'args' dict directly for new params
            reference_catalog_data = GlueCatalog(
                args.get('ReferenceCatalogDB'),
                args.get('ReferenceCatalogTable')
            )

            scorer = FactoryScorer.get_scorer(
                batch_name=workflow_params.batch_name,
                transaction_id=workflow_params.transaction_id,
                pipeline_mod=self.pipeline_mod,
                job_vars=job_vars,
                workflow_vars=workflow_params,
                incoming_catalog_data=incoming_catalog_data,
                enrichment_catalog_data=enrichment_catalog_data,
                reference_catalog_data=reference_catalog_data,
                spark=self.spark,
                glue_client=self.glue_client,
            )

            scorer.calculate_score()
            self.log.info("Job completed successfully")

        except Exception as e:
            self.log.error(f"Critical error in the 'main' function:\n{str(e)}")
            # Error handling managed inside Scorer or here for global failures
            raise RuntimeError("General process failed") from e
        finally:
            self.job.commit()

if __name__ == "__main__":
    pipeline_mod = 'CalculateScore'
    glue_job_execution = GlueJobExecution(pipeline_mod)
    glue_job_execution.main()
