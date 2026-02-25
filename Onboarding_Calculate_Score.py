
import sys
import boto3
import json
import logging
from datetime import datetime
import pandas as pd
import difflib
import io
import os
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
from whoosh import scoring

# --- Import Onboarding Modules ---
# Assuming these are available in the python path or zipped with the job
try:
    from Onboarding_Configuration_Template import S3Location, GlueCatalog, ConfigurationTemplate
except ImportError:
    logging.warning("Onboarding modules not found. Using local mocks or failing.")

# --- Logger Setup ---
log = logging.getLogger('Onboarding_Calculate_Score')
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
log.addHandler(handler)

# --- Argument Parsing ---
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'config_bucket',
    'config_key',
    'input_bucket',
    'input_key',
    'output_bucket',
    'output_prefix',
    'transaction_id',
    'source_catalog_db',
    'source_catalog_table'
])

# --- Glue Context ---
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- S3 Client ---
s3_client = boto3.client('s3')

class ConfigHandler:
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key
        self.config = self._load_config()

    def _load_config(self):
        # Prefer using ConfigurationTemplate if available (as requested)
        if 'ConfigurationTemplate' in globals():
            try:
                # Assuming ConfigurationTemplate usage pattern based on user hint
                # NOTE: The actual implementation details of ConfigurationTemplate are not visible,
                # but typically it abstracts S3 reading.
                # If it takes arguments like bucket/key or constructs an S3Location object:
                s3_loc = S3Location(bucket=self.bucket, key=self.key)
                # Mocking the call structure if get_config is static or instance method
                # This is a best-effort usage without seeing the helper code.
                # If this fails, we fall back to manual loading.
                config_template = ConfigurationTemplate()
                return config_template.get_config(s3_loc)
            except Exception as e:
                log.warning(f"ConfigurationTemplate failed: {e}. Falling back to manual load.")

        # Fallback manual load
        try:
            log.info(f"Loading configuration from s3://{self.bucket}/{self.key}")
            response = s3_client.get_object(Bucket=self.bucket, Key=self.key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            log.error(f"Failed to load configuration: {e}")
            raise

    def get_collection_config(self, collection_name):
        """Retrieves the configuration for a specific collection."""
        for col in self.config.get('collections', []):
            if col.get('collection') == collection_name:
                return col
        return None

    def get_customer_id(self):
        return self.config.get('tr_customer_id')

class ReferenceDataManager:
    def __init__(self, database, table, local_index_path='/tmp/reference_index'):
        self.database = database
        self.table = table
        self.index_dir = local_index_path
        if not os.path.exists(self.index_dir):
            os.mkdir(self.index_dir)

    def load_and_index(self, fields_to_index, customer_id=None):
        """
        Loads data from Glue Catalog and builds a local Whoosh index.
        fields_to_index: List of field names to include in the index.
        customer_id: Optional ID to filter reference data.
        """
        log.info(f"Loading reference data from Glue Catalog {self.database}.{self.table}")

        try:
            # Create DynamicFrame from Catalog
            dyf = glueContext.create_dynamic_frame.from_catalog(
                database=self.database,
                table_name=self.table
            )

            # Convert to DataFrame for filtering and selection
            df_spark = dyf.toDF()

            # Filter by Customer ID if provided (CRITICAL for data isolation/performance)
            if customer_id:
                log.info(f"Filtering reference data for Customer ID: {customer_id}")
                # Assuming the catalog table has a column 'customer_id' or 'tr_customer_id'
                # Adjust column name based on actual schema, falling back to no filter if col missing
                if 'tr_customer_id' in df_spark.columns:
                    df_spark = df_spark.filter(df_spark.tr_customer_id == customer_id)
                elif 'customer_id' in df_spark.columns:
                    df_spark = df_spark.filter(df_spark.customer_id == customer_id)
                else:
                    log.warning("Customer ID column not found in reference table. Loading all data (Potential Performance/Privacy Risk).")

            # Select only necessary columns + ID
            # Ensure ID/Barcode is present. Checks for common ID column names.
            id_candidates = ['id', 'ID', 'barcode', 'Barcode', 'ref_id']
            cols_to_select = list(set(fields_to_index + id_candidates))

            # Filter for columns that actually exist in the table
            available_cols = [c for c in cols_to_select if c in df_spark.columns]

            if not available_cols:
                log.warning("No matching columns found in reference table. Indexing skipped.")
                return None

            # Optimization: Select only needed columns before collecting
            # Fix: Unpack list for select(*cols)
            df_ref = df_spark.select(*available_cols).toPandas()
            df_ref = df_ref.fillna('')

            # Build Schema
            schema = Schema()
            schema.add('ref_id', ID(stored=True))
            for field in fields_to_index:
                schema.add(field, TEXT(stored=True))

            ix = create_in(self.index_dir, schema)
            writer = ix.writer()

            log.info(f"Indexing {len(df_ref)} reference records...")
            count = 0
            for _, row in df_ref.iterrows():
                doc = {}
                # Determine ID (first non-empty match from candidates)
                ref_id = 'unknown'
                for col in id_candidates:
                    if col in row and row[col]:
                        ref_id = str(row[col])
                        break
                doc['ref_id'] = ref_id

                for field in fields_to_index:
                    if field in row:
                        doc[field] = str(row[field])

                writer.add_document(**doc)
                count += 1

            writer.commit()
            log.info("Indexing complete.")
            return ix

        except Exception as e:
            log.error(f"Failed to load/index reference data: {e}")
            raise

class MatchingEngine:
    def __init__(self, index):
        self.ix = index
        self.searcher = self.ix.searcher() if self.ix else None

    def find_match(self, record, lookup_fields):
        """
        Attempts to find a match for the record using the specified lookup fields.
        Returns (MatchStatus, MatchScore, LinkedID)
        """
        if not self.searcher:
            return "Skipped", 0.0, None

        # 1. Blocking / Candidate Retrieval
        # Construct query from all lookup fields
        query_parts = []
        for field in lookup_fields:
            if field in record and record[field]:
                val = str(record[field])
                # Sanitize query
                clean_val = val.replace(':', '').replace('"', r'\"')
                if clean_val:
                    query_parts.append(f'{field}:"{clean_val}"')

        if not query_parts:
            return "NoMatch", 0.0, None

        query_string = " OR ".join(query_parts)
        log.debug(f"Query: {query_string}")

        try:
            # Use MultifieldParser for robustness
            from whoosh.qparser import MultifieldParser
            parser = MultifieldParser(lookup_fields, schema=self.ix.schema)
            q = parser.parse(query_string)
            results = self.searcher.search(q, limit=5) # Top 5 candidates
        except Exception as e:
            log.warning(f"Search error: {e}")
            return "Error", 0.0, None

        if not results:
            return "NoMatch", 0.0, None

        # 2. Scoring (Fuzzy)
        best_score = -1
        best_match_id = None

        for hit in results:
            # Calculate similarity across all lookup fields
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

        # 3. Decision
        # Threshold could be configurable, defaulting to 0.85
        threshold = 0.85
        if best_score >= threshold:
            return "Matched", best_score, best_match_id
        else:
            return "NoMatch", best_score, None

def run_job():
    # 1. Load Config
    config = ConfigHandler(args['config_bucket'], args['config_key'])
    customer_id = config.get_customer_id()

    # 2. Identify all potential lookup fields across all collections for indexing
    all_lookup_fields = set()
    for col in config.config.get('collections', []):
        if col.get('indexing_required'):
            for field in col.get('field_list', []):
                if field.get('field_to_look_up'):
                    all_lookup_fields.add(field['FieldName'])

    # 3. Load & Index Reference Data (Global)
    ref_mgr = ReferenceDataManager(args['source_catalog_db'], args['source_catalog_table'])
    # Only index if we have fields to look up
    # Pass customer_id to filter reference data
    index = ref_mgr.load_and_index(list(all_lookup_fields), customer_id) if all_lookup_fields else None
    engine = MatchingEngine(index)

    # 4. Load Input Data
    log.info(f"Reading input data from s3://{args['input_bucket']}/{args['input_key']}")
    obj = s3_client.get_object(Bucket=args['input_bucket'], Key=args['input_key'])
    df_input = pd.read_csv(io.BytesIO(obj['Body'].read()))
    df_input = df_input.fillna('')

    results = []

    # 5. Process Records
    for _, row in df_input.iterrows():
        record = row.to_dict()
        collection_name = record.get('collection', '') # Assuming 'collection' column exists

        # Get config for this collection
        col_config = config.get_collection_config(collection_name)

        match_status = "Skipped"
        match_score = 0.0
        linked_id = None

        if col_config and col_config.get('indexing_required'):
            # Determine lookup fields for this specific collection
            lookup_fields = [f['FieldName'] for f in col_config['field_list'] if f.get('field_to_look_up')]

            if lookup_fields and index:
                match_status, match_score, linked_id = engine.find_match(record, lookup_fields)
            else:
                match_status = "ConfigMissingFields"

        # Enrich Record
        record['MatchStatus'] = match_status
        record['MatchScore'] = match_score
        record['LinkedLegacyID'] = linked_id

        results.append(record)

    # 6. Output Results
    df_output = pd.DataFrame(results)
    output_key = f"{args['output_prefix']}/scored_results_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"

    log.info(f"Saving results to s3://{args['output_bucket']}/{output_key}")
    csv_buffer = io.StringIO()
    df_output.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=args['output_bucket'], Key=output_key, Body=csv_buffer.getvalue())

if __name__ == '__main__':
    run_job()
    job.commit()
