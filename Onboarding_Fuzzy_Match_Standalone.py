import sys
import json
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, concat_ws, udf, from_json, explode, lit, coalesce
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, FloatType
from pyspark.ml.feature import HashingTF, MinHashLSH, Tokenizer, CountVectorizer
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors, VectorUDT
import pyspark.sql.functions as F

# --- Logger Setup ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def get_spark_glue_context():
    """Initializes Spark and Glue contexts."""
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    return spark, glueContext, job

def parse_args():
    """Parses Glue job arguments."""
    # Define the arguments we expect
    args_list = [
        'JOB_NAME',
        'database_name',            # Glue Database Name
        'table1_name',              # Reference Table Name
        'table2_name',              # Input/OCR Table Name
        'json_column',              # Name of the JSON column in input table (e.g., "key_value_pairs")
        'matching_columns',         # Comma-separated list of columns to match (e.g., "name,mrn")
        'composite_match',          # "true" or "false" - whether to combine columns for matching
        'match_threshold',          # Similarity threshold (0.0 to 1.0), e.g., 0.8
        'output_path'               # S3 path to save results
    ]

    # Try to get arguments, handle failure if running locally/interactively without them
    try:
        args = getResolvedOptions(sys.argv, args_list)
    except Exception as e:
        logger.warning(f"Could not resolve arguments: {e}. Using default/mock values for testing if needed.")
        args = {}

    return args

def extract_json_fields(df, json_col_name, fields_to_extract):
    """
    Extracts specific fields from a JSON string column.

    Args:
        df: Input DataFrame.
        json_col_name: Name of the column containing JSON strings.
        fields_to_extract: List of field names to extract from the JSON.

    Returns:
        DataFrame with new columns extracted from the JSON.
    """
    # Dynamic Schema Construction for JSON parsing
    # We assume all extracted fields are Strings for matching purposes
    schema = StructType([StructField(f, StringType(), True) for f in fields_to_extract])

    # Parse JSON into a struct column
    df_parsed = df.withColumn("parsed_json", from_json(col(json_col_name), schema))

    # Expand struct columns to top level
    for field in fields_to_extract:
        # Alias them to avoid conflict if needed, or keep original name.
        # Using "input_" prefix to distinguish from Reference table if names collide.
        df_parsed = df_parsed.withColumn(f"input_{field}", col(f"parsed_json.{field}"))

    return df_parsed.drop("parsed_json")

def preprocess_text(df, columns):
    """
    Cleans text columns: lowercase, remove special chars, trim.

    Args:
        df: DataFrame.
        columns: List of column names to clean.

    Returns:
        DataFrame with cleaned columns (overwriting originals or creating new ones).
    """
    for c in columns:
        # Check if column exists
        if c in df.columns:
            # simple cleaning: lowercase, keep only alphanumeric and spaces
            # Ensure no nulls by coalescing to empty string
            df = df.withColumn(c, lower(trim(regexp_replace(coalesce(col(c), lit("")), r"[^a-zA-Z0-9\s]", ""))))
    return df

def calculate_similarity(col1, col2, metric="levenshtein"):
    """
    Calculates similarity between two columns.
    For this task, we want > 80% match.

    Levenshtein distance returns the number of edits.
    Similarity = 1 - (distance / max(len(s1), len(s2)))
    """
    # Using Spark's native levenshtein function
    dist_col = F.levenshtein(col(col1), col(col2))
    max_len_col = F.greatest(F.length(col(col1)), F.length(col(col2)))

    # Avoid division by zero
    similarity = F.when(max_len_col == 0, 1.0).otherwise(1.0 - (dist_col / max_len_col))

    return similarity

def run_fuzzy_matching(df_ref, df_input, json_col, matching_cols, composite_match, threshold):
    """
    Core business logic for fuzzy matching.
    Takes dataframes and config, returns matches dataframe.
    """
    # 3.1 Extract JSON fields from Table 2 (Input)
    df_input_parsed = extract_json_fields(df_input, json_col, matching_cols)

    # 3.2 Clean Data
    df_ref_clean = preprocess_text(df_ref, matching_cols)
    input_cols_cleaned = [f"input_{c}" for c in matching_cols]
    df_input_clean = preprocess_text(df_input_parsed, input_cols_cleaned)

    matches_df = None

    if composite_match:
        logger.info("Running Composite Matching Mode")

        # Create a single column "join_key"
        df_ref_ready = df_ref_clean.withColumn("join_key", concat_ws(" ", *[col(c) for c in matching_cols]))
        df_ref_ready = df_ref_ready.filter(col("join_key").isNotNull() & (F.length(col("join_key")) > 0))

        df_input_ready = df_input_clean.withColumn("join_key", concat_ws(" ", *[col(f"input_{c}") for c in matching_cols]))
        df_input_ready = df_input_ready.filter(col("join_key").isNotNull() & (F.length(col("join_key")) > 0))

        lsh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=3)

        tokenizer = Tokenizer(inputCol="join_key", outputCol="tokens")
        hashingTF = HashingTF(inputCol="tokens", outputCol="features")
        pipeline = Pipeline(stages=[tokenizer, hashingTF, lsh])

        model_ref = pipeline.fit(df_ref_ready)
        df_ref_hashed = model_ref.transform(df_ref_ready)
        df_input_hashed = model_ref.transform(df_input_ready)

        LSH_THRESHOLD = 0.8
        candidates = model_ref.stages[-1].approxSimilarityJoin(
            df_ref_hashed,
            df_input_hashed,
            LSH_THRESHOLD,
            distCol="jaccard_dist"
        )

        final_matches = candidates.withColumn("similarity_score", calculate_similarity("datasetA.join_key", "datasetB.join_key"))
        matches_df = final_matches.filter(col("similarity_score") >= threshold)

    else:
        logger.info("Running Individual Column Matching Mode")
        accumulated_matches = None

        for col_name in matching_cols:
            ref_col = col_name
            input_col = f"input_{col_name}"

            logger.info(f"Matching column: {col_name}")

            df_ref_ready = df_ref_clean.withColumn("match_col", col(ref_col)).filter(col("match_col").isNotNull())
            df_input_ready = df_input_clean.withColumn("match_col", col(input_col)).filter(col("match_col").isNotNull())

            tokenizer = Tokenizer(inputCol="match_col", outputCol="tokens")
            hashingTF = HashingTF(inputCol="tokens", outputCol="features")
            lsh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=3)
            pipeline = Pipeline(stages=[tokenizer, hashingTF, lsh])

            model = pipeline.fit(df_ref_ready)
            df_ref_hashed = model.transform(df_ref_ready)
            df_input_hashed = model.transform(df_input_ready)

            LSH_THRESHOLD = 0.8
            candidates = model.stages[-1].approxSimilarityJoin(
                df_ref_hashed,
                df_input_hashed,
                LSH_THRESHOLD,
                distCol="jaccard_dist"
            )

            scored = candidates.withColumn(
                f"{col_name}_score",
                calculate_similarity("datasetA.match_col", "datasetB.match_col")
            )

            valid_matches = scored.filter(col(f"{col_name}_score") >= threshold)

            select_cols = [
                col("datasetA.match_col").alias("ref_value"),
                col("datasetB.match_col").alias("input_value"),
                lit(col_name).alias("matched_on_field"),
                col(f"{col_name}_score").alias("score"),
                col("datasetB.transaction_id").alias("transaction_id")
            ]

            clean_matches = valid_matches.select(*select_cols)

            if accumulated_matches is None:
                accumulated_matches = clean_matches
            else:
                accumulated_matches = accumulated_matches.union(clean_matches)

        matches_df = accumulated_matches

    return matches_df

def main():
    # 1. Init
    spark, glueContext, job = get_spark_glue_context()
    args = parse_args()

    # --- Configuration (using args or defaults) ---
    DATABASE_NAME = args.get('database_name', 'test_db')
    TABLE1_NAME = args.get('table1_name', 'table1') # Reference
    TABLE2_NAME = args.get('table2_name', 'table2') # Input
    JSON_COL = args.get('json_column', 'key_value_pairs')
    MATCHING_COLS = args.get('matching_columns', 'name,mrn').split(',')
    COMPOSITE_MATCH = args.get('composite_match', 'true').lower() == 'true'
    THRESHOLD = float(args.get('match_threshold', 0.8))
    OUTPUT_PATH = args.get('output_path', 's3://temp-bucket/matches/')

    logger.info(f"Starting Match Job with Config: DB={DATABASE_NAME}, Ref={TABLE1_NAME}, Input={TABLE2_NAME}, JSON_Col={JSON_COL}, Cols={MATCHING_COLS}, Composite={COMPOSITE_MATCH}")

    # 2. Load Data
    try:
        df_ref = spark.table(f"{DATABASE_NAME}.{TABLE1_NAME}")
        df_input = spark.table(f"{DATABASE_NAME}.{TABLE2_NAME}")
    except Exception as e:
        logger.error(f"Error reading tables from catalog: {e}")
        sys.exit(1)

    # 3. Run Logic
    matches_df = run_fuzzy_matching(df_ref, df_input, JSON_COL, MATCHING_COLS, COMPOSITE_MATCH, THRESHOLD)

    # 4. Save Output
    if matches_df:
        logger.info(f"Found matches. Saving to {OUTPUT_PATH}")
        matches_df.write.mode("overwrite").parquet(OUTPUT_PATH)
        matches_df.show(5, truncate=False)
    else:
        logger.info("No matches found.")

    job.commit()

if __name__ == '__main__':
    main()
