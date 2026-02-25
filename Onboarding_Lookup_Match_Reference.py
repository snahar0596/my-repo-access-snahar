
import sys
import boto3
import json
import logging
from datetime import datetime
import pandas as pd
import difflib
import io
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
from whoosh import scoring
import os

# --- Import Onboarding Modules ---
# Fail fast if modules are missing, as they are required for this job
import Onboarding_OpsIQ_Manager
import Onboarding_Timestream_Manager

# --- Logger Setup ---
log = logging.getLogger('LookupMatchReference')
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
    'opsiq_queue_name',
    'timestream_queue_name',
    'timestream_db',
    'timestream_table'
])

# --- Glue Context ---
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- S3 Client ---
s3_client = boto3.client('s3')

class ConfigurationManager:
    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key
        self.config = self._load_config()

    def _load_config(self):
        try:
            log.info(f"Loading configuration from s3://{self.bucket}/{self.key}")
            response = s3_client.get_object(Bucket=self.bucket, Key=self.key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            log.error(f"Failed to load configuration: {e}")
            raise

    def get_reference_schema(self):
        # Build Whoosh Schema from JSON config
        fields = self.config['reference_data']['schema']
        schema = Schema()
        for field in fields:
            if field['type'] == 'TEXT':
                schema.add(field['name'], TEXT(stored=field['stored']))
            elif field['type'] == 'ID':
                schema.add(field['name'], ID(stored=field['stored']))
        return schema

    def get_matching_rules(self):
        return self.config['matching_rules']

    def get_reference_path(self):
        return self.config['reference_data']['s3_path']

    def get_local_index_path(self):
        return self.config['reference_data'].get('index_local_path', '/tmp/reference_index')

    def get_output_config(self):
        return self.config.get('output', {})

class ReferenceDataManager:
    def __init__(self, config_manager):
        self.config = config_manager
        self.index_dir = self.config.get_local_index_path()
        if not os.path.exists(self.index_dir):
            os.mkdir(self.index_dir)

    def load_and_index(self):
        # 1. Download Reference CSV
        ref_s3_path = self.config.get_reference_path()
        bucket, key = ref_s3_path.replace("s3://", "").split("/", 1)
        local_csv = "/tmp/reference.csv"

        log.info(f"Downloading reference data from {ref_s3_path}")
        s3_client.download_file(bucket, key, local_csv)

        # 2. Read into Pandas
        df_ref = pd.read_csv(local_csv)
        df_ref = df_ref.fillna('') # Handle NaNs

        # 3. Create Whoosh Index
        schema = self.config.get_reference_schema()
        ix = create_in(self.index_dir, schema)
        writer = ix.writer()

        log.info("Indexing reference data...")
        # Add documents to index
        # We need to map dataframe columns to schema fields dynamically
        schema_fields = [f['name'] for f in self.config.config['reference_data']['schema']]

        count = 0
        for _, row in df_ref.iterrows():
            doc = {}
            for field in schema_fields:
                if field in row:
                    doc[field] = str(row[field])
            writer.add_document(**doc)
            count += 1

        writer.commit()
        log.info(f"Indexed {count} records.")
        return ix

class MatchingEngine:
    def __init__(self, index, rules):
        self.ix = index
        self.rules = rules
        self.searcher = self.ix.searcher()
        self.parser = QueryParser(self.rules['blocking_strategy']['search_fields'][0], schema=self.ix.schema) # Default to first field

    def find_candidates(self, record):
        # Strategy: Construct a query string from the 'blocking_strategy' fields
        search_fields = self.rules['blocking_strategy']['search_fields']
        query_parts = []
        for field in search_fields:
            if field in record and record[field]:
                # Construct query: field:"value" (quoted for multi-word phrases)
                # Removing colons (special char) and escaping double quotes
                val_str = str(record[field])
                clean_val = val_str.replace(':', '').replace('"', r'\"')
                if clean_val:
                    query_parts.append(f'{field}:"{clean_val}"')

        if not query_parts:
            return []

        query_string = " OR ".join(query_parts)
        log.debug(f"Search Query: {query_string}")

        # Parse and Search
        # Note: Using multifield parser or manual construction is safer
        from whoosh.qparser import MultifieldParser
        mparser = MultifieldParser(search_fields, schema=self.ix.schema)

        try:
            q = mparser.parse(query_string)
            results = self.searcher.search(q, limit=self.rules['blocking_strategy']['top_k_candidates'])
            return [dict(r) for r in results]
        except Exception as e:
            log.warning(f"Search failed for {query_string}: {e}")
            return []

    def calculate_score(self, candidate, record):
        # Weighted average scoring
        total_score = 0
        total_weight = 0

        score_rules = self.rules['scoring']['attributes']

        for rule in score_rules:
            field = rule['field']
            weight = rule['weight']
            algo = rule.get('algo', 'exact')

            val1 = str(record.get(field, '')).lower()
            val2 = str(candidate.get(field, '')).lower()

            sim = 0
            if algo == 'exact':
                sim = 1.0 if val1 == val2 else 0.0
            elif algo == 'sequence_matcher': # Updated from jaro_winkler
                sim = difflib.SequenceMatcher(None, val1, val2).ratio()

            total_score += sim * weight
            total_weight += weight

        return total_score / total_weight if total_weight > 0 else 0

    def process_record(self, record):
        candidates = self.find_candidates(record)
        best_match = None
        best_score = -1

        for cand in candidates:
            score = self.calculate_score(cand, record)
            if score > best_score:
                best_score = score
                best_match = cand

        threshold = self.rules['scoring']['threshold']
        status = "MATCH_FOUND" if best_score >= threshold else "NO_MATCH"

        return {
            "record": record,
            "best_match": best_match,
            "score": best_score,
            "status": status
        }

def run_job():
    # 1. Load Config
    config_mgr = ConfigurationManager(args['config_bucket'], args['config_key'])
    output_config = config_mgr.get_output_config()
    failure_action = output_config.get('failure_action', 'route_to_opsiq') # Default behavior

    # 2. Prepare Reference Index
    ref_mgr = ReferenceDataManager(config_mgr)
    index = ref_mgr.load_and_index()

    # 3. Initialize Matching Engine
    engine = MatchingEngine(index, config_mgr.get_matching_rules())

    # Initialize OpsIQ Manager if needed
    opsiq_manager = None
    if 'opsiq_queue_name' in args and failure_action == 'route_to_opsiq':
        opsiq_manager = Onboarding_OpsIQ_Manager.OpsIQMessageManager(
            opsiq_queue=args['opsiq_queue_name'],
            batch_name=f"match_job_{args['JOB_NAME']}",
            field_schema=[], # Schema could be passed if known
            transaction_id=args['transaction_id'],
            system="LookupMatchReference",
            source="Onboarding",
            source_bucket=args['input_bucket'],
            config_template="generic_match_review" # Placeholder template
        )

    # 4. Load Input Data
    input_path = f"s3://{args['input_bucket']}/{args['input_key']}"
    log.info(f"Reading input data from {input_path}")

    # Use Boto3 to read input to avoid s3fs dependency if not present
    obj = s3_client.get_object(Bucket=args['input_bucket'], Key=args['input_key'])
    df_input = pd.read_csv(io.BytesIO(obj['Body'].read()))
    df_input = df_input.fillna('')

    results = []
    success_count = 0
    failure_count = 0

    # 5. Process Records
    for _, row in df_input.iterrows():
        record = row.to_dict()
        result = engine.process_record(record)

        # Add metadata
        output_row = result['record'].copy()
        output_row['match_score'] = result['score']
        output_row['match_status'] = result['status']
        if result['best_match']:
            # Append match details with prefix
            for k, v in result['best_match'].items():
                output_row[f"matched_{k}"] = v

        results.append(output_row)

        # OpsIQ Handling for Failures
        item_code = str(record.get('id', record.get('ID', 'unknown')))
        if result['status'] == 'NO_MATCH':
            failure_count += 1
            if opsiq_manager:
                # Construct OpsIQ payload
                opsiq_manager.save_item_message_content(
                    ItemCode=item_code,
                    Status="Failed",
                    Error="No match found above threshold",
                    ErrorCode="MATCH_FAILED",
                    EventName="ReferenceLookup",
                    FieldList=record # Pass the full record for review
                )
        else:
            success_count += 1

    # Send OpsIQ Batch if any failures were recorded
    if opsiq_manager and opsiq_manager.list_payloads:
        log.info(f"Sending {len(opsiq_manager.list_payloads)} records to OpsIQ.")
        opsiq_manager.send_opsiq_message()

    # Timestream Logging
    if 'timestream_queue_name' in args:
        try:
            ts_content = Onboarding_Timestream_Manager.create_timestream_content(
                source="Onboarding",
                source_device="LookupMatchReference",
                transaction_id=args['transaction_id'],
                batch_name=f"match_job_{args['JOB_NAME']}",
                pipeline_mod="MatchingEngine",
                state="Complete",
                no_of_asset=str(len(results)),
                file_path=f"s3://{args['output_bucket']}/{args['output_prefix']}"
            )
            # Add specific metrics for match rate
            ts_content['Measures'].append({"Name": "MatchSuccessCount", "Value": str(success_count), "Type": "BIGINT"})
            ts_content['Measures'].append({"Name": "MatchFailureCount", "Value": str(failure_count), "Type": "BIGINT"})

            Onboarding_Timestream_Manager.timestream_insert_data(
                db=args['timestream_db'],
                table=args['timestream_table'],
                measure_name="ReferenceMatchingStats",
                content=ts_content,
                timestream_queue=args['timestream_queue_name']
            )
            log.info("Sent metrics to Timestream.")
        except Exception as e:
            log.error(f"Failed to send Timestream metrics: {e}")

    # 6. Save Results
    df_output = pd.DataFrame(results)
    output_key = f"{args['output_prefix']}/processed_results_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    output_path = f"s3://{args['output_bucket']}/{output_key}"

    log.info(f"Saving results to {output_path}")

    # Use Boto3 to write output to avoid s3fs dependency
    csv_buffer = io.StringIO()
    df_output.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=args['output_bucket'], Key=output_key, Body=csv_buffer.getvalue())

if __name__ == '__main__':
    run_job()
    job.commit()
