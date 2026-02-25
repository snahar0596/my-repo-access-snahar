
import sys
import boto3
import json
import logging
import pandas as pd
import difflib
import io
import os
import shutil
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser

# Configure logging to stdout for immediate feedback during testing
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

def get_job_args():
    """Parses arguments passed to the Glue job."""
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'config_bucket',
        'config_key',
        'CatalogDB',
        'IncomingCatalogTable', # Table with extracted text to score
        'ReferenceCatalogDB',   # SSOT Database
        'ReferenceCatalogTable' # SSOT Table
    ])
    return args

def load_config_from_s3(bucket, key):
    """Loads JSON configuration directly from S3 using Boto3."""
    s3 = boto3.client('s3')
    try:
        logger.info(f"Loading config from s3://{bucket}/{key}")
        response = s3.get_object(Bucket=bucket, Key=key)
        config_str = response['Body'].read().decode('utf-8')
        return json.loads(config_str)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        raise

def get_lookup_fields(config):
    """Extracts all fields marked with 'field_to_look_up: true' across all collections."""
    lookup_fields = set()
    for col in config.get('collections', []):
        if col.get('indexing_required'):
            for field in col.get('field_list', []):
                if field.get('field_to_look_up'):
                    lookup_fields.add(field['FieldName'])
    return list(lookup_fields)

def load_reference_data(db, table, customer_id, fields_to_index):
    """
    Loads reference data from Glue Catalog, filtered by customer_id.
    Returns a Pandas DataFrame with selected columns.
    """
    logger.info(f"Loading reference data from {db}.{table} for customer_id: {customer_id}")

    query = f"SELECT * FROM `{db}`.`{table}` WHERE tr_customer_id = '{customer_id}'"

    try:
        # Use Spark SQL to filter efficiently
        ref_df_spark = spark.sql(query)

        # Identify columns to keep (IDs + lookup fields)
        id_cols = ['id', 'ID', 'barcode', 'Barcode', 'ref_id']
        existing_cols = ref_df_spark.columns
        available_cols = [c for c in existing_cols if c in fields_to_index or c in id_cols]

        if not available_cols:
            logger.warning("No matching columns found in reference table.")
            return None

        # Collect to Pandas for local indexing
        # NOTE: Be mindful of memory limits for large datasets!
        ref_df = ref_df_spark.select(*available_cols).toPandas()
        ref_df = ref_df.fillna('')
        logger.info(f"Loaded {len(ref_df)} reference records.")
        return ref_df

    except Exception as e:
        logger.error(f"Error querying reference data: {e}")
        return None

def build_local_index(ref_df, fields_to_index, index_dir='/tmp/test_ref_index'):
    """Builds a Whoosh index from the reference DataFrame."""
    if os.path.exists(index_dir):
        shutil.rmtree(index_dir)
    os.mkdir(index_dir)

    schema = Schema()
    schema.add('ref_id', ID(stored=True))
    for field in fields_to_index:
        schema.add(field, TEXT(stored=True))

    ix = create_in(index_dir, schema)
    writer = ix.writer()

    id_cols = ['id', 'ID', 'barcode', 'Barcode', 'ref_id']

    count = 0
    for _, row in ref_df.iterrows():
        doc = {}
        # Resolve ID
        ref_id = 'unknown'
        for col in id_cols:
            if col in row and row[col]:
                ref_id = str(row[col])
                break
        doc['ref_id'] = ref_id

        # Add index fields
        for field in fields_to_index:
            if field in row:
                doc[field] = str(row[field])

        writer.add_document(**doc)
        count += 1

    writer.commit()
    logger.info(f"Built index with {count} documents.")
    return ix

def perform_matching(ix, record, lookup_fields, threshold=0.85):
    """Searches the index and calculates a match score."""
    if not ix:
        return "Skipped", 0.0, None

    searcher = ix.searcher()
    try:
        # Build Query
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
        # logger.info(f"Query: {query_string}") # Debug logging

        from whoosh.qparser import MultifieldParser
        parser = MultifieldParser(lookup_fields, schema=ix.schema)
        q = parser.parse(query_string)
        results = searcher.search(q, limit=5)

        if not results:
            return "NoMatch", 0.0, None

        # Score Candidates
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

        if best_score >= threshold:
            return "Matched", best_score, best_match_id
        else:
            return "NoMatch", best_score, None

    except Exception as e:
        logger.error(f"Match error: {e}")
        return "Error", 0.0, None
    finally:
        searcher.close()

def main():
    args = get_job_args()
    job.init(args['JOB_NAME'], args)

    # 1. Load Config
    config = load_config_from_s3(args['config_bucket'], args['config_key'])
    customer_id = config.get('tr_customer_id')
    logger.info(f"Customer ID: {customer_id}")

    # 2. Determine Fields to Index
    all_lookup_fields = get_lookup_fields(config)
    logger.info(f"Global lookup fields: {all_lookup_fields}")

    if not all_lookup_fields:
        logger.warning("No lookup fields defined in config. Exiting.")
        return

    # 3. Load Reference Data & Build Index
    ref_df = load_reference_data(args['ReferenceCatalogDB'], args['ReferenceCatalogTable'], customer_id, all_lookup_fields)

    if ref_df is None or ref_df.empty:
        logger.warning("No reference data loaded. Exiting.")
        return

    index = build_local_index(ref_df, all_lookup_fields)

    # 4. Load Input Data (from Incoming Catalog Table)
    input_db = args['CatalogDB']
    input_table = args['IncomingCatalogTable']
    logger.info(f"Loading input data from {input_db}.{input_table}")

    input_df_spark = glueContext.create_dynamic_frame.from_catalog(database=input_db, table_name=input_table).toDF()
    input_df = input_df_spark.toPandas() # Convert to Pandas for row-by-row logic

    if input_df.empty:
        logger.warning("Input data is empty.")
        return

    # 5. Process & Score
    results = []
    logger.info("Starting scoring process...")

    for _, row in input_df.iterrows():
        record = row.to_dict()

        # Expand 'key_value_pairs' JSON if present
        if 'key_value_pairs' in record and isinstance(record['key_value_pairs'], str):
            try:
                kv = json.loads(record['key_value_pairs'])
                record.update(kv)
            except: pass

        # Determine lookup fields for *this* record's collection
        collection_name = record.get('collection', '') # Assumption: 'collection' column exists

        # Simple lookup in config for collection specific fields
        col_lookup_fields = []
        for col in config.get('collections', []):
            if col.get('collection') == collection_name and col.get('indexing_required'):
                 col_lookup_fields = [f['FieldName'] for f in col.get('field_list', []) if f.get('field_to_look_up')]
                 break

        # Default to all fields if collection specific logic fails or collection not found
        current_lookup_fields = col_lookup_fields if col_lookup_fields else all_lookup_fields

        status, score, linked_id = perform_matching(index, record, current_lookup_fields)

        record['MatchStatus'] = status
        record['MatchScore'] = score
        record['LinkedLegacyID'] = linked_id
        results.append(record)

    # 6. Print Results
    result_df = pd.DataFrame(results)
    print("\n" + "="*50)
    print("FINAL RESULTS (First 20 rows):")
    print("="*50)
    # Display specific columns if they exist
    cols_to_show = ['MatchStatus', 'MatchScore', 'LinkedLegacyID'] + [c for c in all_lookup_fields if c in result_df.columns]
    print(result_df[cols_to_show].head(20).to_string())
    print("="*50 + "\n")

    job.commit()

if __name__ == '__main__':
    main()
