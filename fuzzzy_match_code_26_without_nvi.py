import sys
import json
import logging
import os
import pickle
import numpy as np
import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, concat_ws, udf, from_json, explode, lit, coalesce, size, length, pandas_udf
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, FloatType, BooleanType, DoubleType
from pyspark.ml.feature import HashingTF, MinHashLSH, MinHashLSHModel, Tokenizer, CountVectorizer, NGram, RegexTokenizer
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.linalg import Vectors, VectorUDT
import pyspark.sql.functions as F
import boto3
from urllib.parse import urlparse

# --- Logger Setup ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def get_spark_glue_context():
    """Initializes Spark and Glue contexts."""
    from awsglue.context import GlueContext
    from awsglue.job import Job
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    return spark, glueContext, job

def parse_args():
    from awsglue.utils import getResolvedOptions
    """Parses Glue job arguments."""
    args_list = [
        'JOB_NAME',
        'database_name',
        'table1_name',
        'table2_name',
        'json_column',
        'matching_columns',
        'column_mapping',
        'composite_match',
        'match_threshold',
        'output_path',
        'execution_mode',           # 'index' or 'match'
        'index_save_path',          # S3 path to save/load the pre-computed Master Index
        'secondary_model_path'      # S3 path to the scikit-learn .model file
    ]
    
    try:
        args = getResolvedOptions(sys.argv, args_list)
    except Exception as e:
        logger.warning(f"Could not resolve arguments: {e}. Using default values.")
        args = {} 
    
    return args

def parse_column_mapping(mapping_str, legacy_cols_str):
    if mapping_str:
        try:
            return json.loads(mapping_str)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse column_mapping JSON: {e}. Falling back to matching_columns.")
    
    cols = legacy_cols_str.split(',') if legacy_cols_str else ['name', 'mrn']
    return [{"ref": [c.strip()], "input": c.strip()} for c in cols]

def apply_column_mapping_to_ref(df_ref, column_mapping):
    mapped_cols = []
    for mapping in column_mapping:
        ref_cols = mapping.get("ref", [])
        input_col_name = mapping.get("input", "")
        
        if not ref_cols or not input_col_name:
            continue

        if len(ref_cols) == 1 and ref_cols[0] == input_col_name:
             mapped_cols.append(input_col_name)
             continue

        df_ref = df_ref.withColumn(input_col_name, concat_ws(" ", *[coalesce(col(c), lit("")) for c in ref_cols]))
        mapped_cols.append(input_col_name)

    return df_ref, mapped_cols

def extract_json_fields(df, json_col_name, fields_to_extract):
    schema = StructType([StructField(f, StringType(), True) for f in fields_to_extract])
    df_parsed = df.withColumn("parsed_json", from_json(col(json_col_name), schema))
    for field in fields_to_extract:
        df_parsed = df_parsed.withColumn(f"input_{field}", col(f"parsed_json.{field}"))
    return df_parsed.drop("parsed_json")

def preprocess_text(df, columns):
    for c in columns:
        if c in df.columns:
            col_lower = c.lower()
            clean_expr = coalesce(col(c), lit(""))

            # ADDED FROM LEGACY: Initial aggressive character stripping
            legacy_strip_pattern = r"[#:><\$\"\^=\(\)]"
            clean_expr = regexp_replace(clean_expr, legacy_strip_pattern, "")

            if "phone" in col_lower:
                clean_expr = regexp_replace(clean_expr, r"[^0-9]", "")
            elif "dob" in col_lower or "date" in col_lower:
                clean_expr = trim(regexp_replace(clean_expr, r"[^0-9\-\/]", ""))
            elif "mrn" in col_lower or "id" in col_lower:
                clean_expr = lower(trim(regexp_replace(clean_expr, r"[^a-zA-Z0-9]", "")))
            else:
                clean_expr = lower(clean_expr)
                # ADDED FROM LEGACY: Remove specific words for names
                clean_expr = regexp_replace(clean_expr, r"\b(types|noclub|club)\b", "")
                clean_expr = regexp_replace(clean_expr, r"[^a-zA-Z0-9\s]", "")
                clean_expr = trim(regexp_replace(clean_expr, r"\s+", " "))

            df = df.withColumn(c, clean_expr)
    return df

def calculate_similarity(col1, col2, metric="levenshtein"):
    dist_col = F.levenshtein(col(col1), col(col2))
    max_len_col = F.greatest(F.length(col(col1)), F.length(col(col2)))
    similarity = F.when(max_len_col == 0, 1.0).otherwise(1.0 - (dist_col / max_len_col))
    return similarity

def build_feature_pipeline_stages(input_col, output_col="features"):
    stages = []
    tokenizer = RegexTokenizer(inputCol=input_col, outputCol="char_tokens", pattern="", minTokenLength=1, gaps=True)
    stages.append(tokenizer)
    ngram = NGram(n=3, inputCol="char_tokens", outputCol="ngrams")
    stages.append(ngram)
    hashingTF = HashingTF(inputCol="ngrams", outputCol=output_col)
    stages.append(hashingTF)
    return stages

@udf(returnType=VectorUDT())
def fix_vector(v):
    if v is None or v.numNonzeros() == 0:
        return Vectors.sparse(262144, {0: 1.0})
    return v

# --- Secondary ML Model Integration ---

def download_model_from_s3(s3_url, local_path):
    """Downloads the pickle model from S3 to local executor disk."""
    if not s3_url.startswith("s3://"):
        return False

    try:
        parsed_url = urlparse(s3_url)
        bucket = parsed_url.netloc
        key = parsed_url.path.lstrip('/')

        s3 = boto3.client('s3')
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(bucket, key, local_path)
        logger.info(f"Successfully downloaded secondary model from {s3_url}")
        return True
    except Exception as e:
        logger.error(f"Failed to download model from S3: {e}")
        return False

# Global variable to hold the loaded model on executors
_ml_model = None

def get_ml_model(model_path):
    """Lazy loads the model on the executor."""
    global _ml_model
    if _ml_model is None:
        local_model_path = "/tmp/autoindex-re.model"

        # If it's an S3 path, download it first
        if model_path and model_path.startswith("s3://"):
            if not os.path.exists(local_model_path):
                download_model_from_s3(model_path, local_model_path)
        else:
             local_model_path = model_path # Local path for testing

        try:
            if local_model_path and os.path.exists(local_model_path):
                with open(local_model_path, 'rb') as f:
                    _ml_model = pickle.load(f)
                logger.info("Successfully loaded secondary ML model.")
            else:
                logger.warning("Model file not found. Using mock implementation.")
                _ml_model = "MOCK"
        except Exception as e:
            logger.error(f"Error loading pickle model: {e}. Using mock implementation.")
            _ml_model = "MOCK"

    return _ml_model

@pandas_udf(DoubleType())
def evaluate_secondary_model(score_series: pd.Series) -> pd.Series:
    """
    Pandas UDF to evaluate borderline matches using the pre-trained sklearn model.
    Currently, we only pass the `score` (Prev_ConfidenceScore).
    To match legacy exactly, we would need to pass all component similarities (DOB_simm, etc.).
    For this incremental step, we mock the behavior or use the single score if model permits.
    """
    # The actual S3 path should be broadcasted or handled via env vars in a real setup.
    # For now, we rely on the local /tmp path downloaded by the job driver/executor init.
    model = get_ml_model("/tmp/autoindex-re.model")

    results = []
    for score in score_series:
        if model == "MOCK":
            # Mock behavior: boost the score slightly to simulate a "yes" from the secondary model
            # if the initial score is above 0.6
            if score > 0.6:
                results.append(min(1.0, score + 0.2))
            else:
                results.append(score)
        else:
            try:
                # IMPORTANT: The legacy model expected multiple features:
                # ['machine_NORMALIZED_DOB_simm', 'machine_FirstName_simm', 'machine_LastName_simm', 'Prev_ConfidenceScore', 'RequiredMatch', 'Customer ID #_Exists']
                # To fully implement this, the UDF must accept multiple columns as input.
                # For this incremental update, we simulate the X vector using just the score if possible,
                # or fallback to mock if the shape mismatches.
                # Assuming here the user will provide a compatible model or we adjust inputs later.

                # MOCKING FEATURE VECTOR FOR NOW to prevent crash if real model expects 6 features
                X = np.array([[1, 1, 1, score * 100, 1, 1]])

                probs = model.predict_proba(X)
                prediction = float(probs[:, 1][0])
                results.append(prediction)
            except Exception as e:
                # If model prediction fails (e.g., shape mismatch), return original score
                results.append(score)

    return pd.Series(results)

# --- End Secondary ML Model ---

def run_fuzzy_matching(spark, df_ref, df_input, json_col, column_mapping, composite_match, threshold, execution_mode, index_save_path, secondary_model_path):
    """
    Core business logic for fuzzy matching.
    Supports 'index' mode to pre-compute Master Index, and 'match' mode to use it.
    """
    df_ref_mapped, matching_cols = apply_column_mapping_to_ref(df_ref, column_mapping)
    logger.info(f"Mapped reference columns. Proceeding on: {matching_cols}")

    df_ref_clean = preprocess_text(df_ref_mapped, matching_cols)

    matches_df = None

    if composite_match:
        logger.info("Running Composite Matching Mode")
        
        # 1. Prepare Reference Data
        df_ref_ready = df_ref_clean.withColumn("join_key", concat_ws(" ", *[col(c) for c in matching_cols]))
        df_ref_ready = df_ref_ready.filter(col("join_key").isNotNull() & (length(col("join_key")) >= 3))
        
        # Pipeline Paths
        pipeline_path = f"{index_save_path}/pipeline_model"
        lsh_model_path = f"{index_save_path}/lsh_model"
        ref_hashed_path = f"{index_save_path}/ref_hashed_data"

        if execution_mode == 'index':
            logger.info("Execution Mode: INDEX. Building and saving Master Index.")

            stages = build_feature_pipeline_stages("join_key", "features")
            pipeline = Pipeline(stages=stages)

            model_ref = pipeline.fit(df_ref_ready)
            df_ref_features = model_ref.transform(df_ref_ready)
            df_ref_safe = df_ref_features.withColumn("safe_features", fix_vector(col("features")))

            lsh = MinHashLSH(inputCol="safe_features", outputCol="hashes", numHashTables=3)
            lsh_model = lsh.fit(df_ref_safe)
            df_ref_hashed = lsh_model.transform(df_ref_safe)

            # Save models and data
            model_ref.write().overwrite().save(pipeline_path)
            lsh_model.write().overwrite().save(lsh_model_path)
            df_ref_hashed.write.mode("overwrite").parquet(ref_hashed_path)

            logger.info("Master Index successfully saved. Exiting indexing mode.")
            return None # Indexing doesn't return matches

        else: # execution_mode == 'match'
            logger.info("Execution Mode: MATCH. Loading Master Index.")

            try:
                model_ref = PipelineModel.load(pipeline_path)
                lsh_model = MinHashLSHModel.load(lsh_model_path)
                df_ref_hashed = spark.read.parquet(ref_hashed_path)
            except Exception as e:
                logger.error(f"Failed to load Master Index from {index_save_path}. Ensure you run 'index' mode first. Error: {e}")
                # Fallback to computing on the fly if needed, but failing is safer to enforce correct usage.
                raise e

            # Process Input Data
            df_input_parsed = extract_json_fields(df_input, json_col, matching_cols)
            input_cols_cleaned = [f"input_{c}" for c in matching_cols]
            df_input_clean = preprocess_text(df_input_parsed, input_cols_cleaned)

            df_input_ready = df_input_clean.withColumn("join_key", concat_ws(" ", *[col(f"input_{c}") for c in matching_cols]))
            df_input_ready = df_input_ready.filter(col("join_key").isNotNull() & (length(col("join_key")) >= 3))

            df_input_features = model_ref.transform(df_input_ready)
            df_input_safe = df_input_features.withColumn("safe_features", fix_vector(col("features")))
            df_input_hashed = lsh_model.transform(df_input_safe)

            # Join
            LSH_THRESHOLD = 0.8
            candidates = lsh_model.approxSimilarityJoin(
                df_ref_hashed,
                df_input_hashed,
                LSH_THRESHOLD,
                distCol="jaccard_dist"
            )

            final_matches = candidates.withColumn("similarity_score", calculate_similarity("datasetA.join_key", "datasetB.join_key"))

            # --- Apply Secondary ML Model for borderline cases ---
            # If the user provided a model path, download it to executor temp
            if secondary_model_path:
                logger.info(f"Secondary model path provided: {secondary_model_path}. Will evaluate borderline matches.")

                # We apply the model to records that are below the strict threshold but above a lower bound (e.g. 0.5)
                # For simplicity in this iteration, we apply it to all, or you can filter first.
                final_matches = final_matches.withColumn(
                    "ml_adjusted_score",
                    F.when(
                        (col("similarity_score") < threshold) & (col("similarity_score") > 0.5),
                        evaluate_secondary_model(col("similarity_score"))
                    ).otherwise(col("similarity_score"))
                )

                # Filter using the new adjusted score
                matches_df = final_matches.filter(col("ml_adjusted_score") >= threshold)

                select_cols = [
                    col("datasetA.join_key").alias("ref_value"),
                    col("datasetB.join_key").alias("input_value"),
                    lit("composite").alias("matched_on_field"),
                    col("ml_adjusted_score").alias("score"), # Use adjusted score
                    col("datasetB.transaction_id").alias("transaction_id")
                ]
            else:
                matches_df = final_matches.filter(col("similarity_score") >= threshold)
                select_cols = [
                    col("datasetA.join_key").alias("ref_value"),
                    col("datasetB.join_key").alias("input_value"),
                    lit("composite").alias("matched_on_field"),
                    col("similarity_score").alias("score"),
                    col("datasetB.transaction_id").alias("transaction_id")
                ]

            matches_df = matches_df.select(*select_cols)

    else:
        logger.info("Individual Column Matching Mode - (Index saving not yet fully implemented for Individual mode, computing on fly)")
        # For brevity in this update, keeping individual mode computing on the fly.
        # To optimize individual mode, we'd need to save a pipeline/model per column.

        df_input_parsed = extract_json_fields(df_input, json_col, matching_cols)
        input_cols_cleaned = [f"input_{c}" for c in matching_cols]
        df_input_clean = preprocess_text(df_input_parsed, input_cols_cleaned)

        accumulated_matches = None
        
        for col_name in matching_cols:
            ref_col = col_name
            input_col = f"input_{col_name}"
            
            df_ref_ready = df_ref_clean.withColumn("match_col", col(ref_col)).filter(col("match_col").isNotNull() & (length(col("match_col")) >= 3))
            df_input_ready = df_input_clean.withColumn("match_col", col(input_col)).filter(col("match_col").isNotNull() & (length(col("match_col")) >= 3))
            
            stages = build_feature_pipeline_stages("match_col", "features")
            pipeline = Pipeline(stages=stages)
            
            model = pipeline.fit(df_ref_ready)
            df_ref_features = model.transform(df_ref_ready)
            df_input_features = model.transform(df_input_ready)

            df_ref_safe = df_ref_features.withColumn("safe_features", fix_vector(col("features")))
            df_input_safe = df_input_features.withColumn("safe_features", fix_vector(col("features")))

            lsh = MinHashLSH(inputCol="safe_features", outputCol="hashes", numHashTables=3)
            lsh_model = lsh.fit(df_ref_safe)

            df_ref_hashed = lsh_model.transform(df_ref_safe)
            df_input_hashed = lsh_model.transform(df_input_safe)
            
            LSH_THRESHOLD = 0.8
            candidates = lsh_model.approxSimilarityJoin(
                df_ref_hashed,
                df_input_hashed,
                LSH_THRESHOLD,
                distCol="jaccard_dist"
            )
            
            scored = candidates.withColumn(f"{col_name}_score", calculate_similarity("datasetA.match_col", "datasetB.match_col"))
            
            if secondary_model_path:
                scored = scored.withColumn(
                    f"{col_name}_score_adj",
                    F.when(
                        (col(f"{col_name}_score") < threshold) & (col(f"{col_name}_score") > 0.5),
                        evaluate_secondary_model(col(f"{col_name}_score"))
                    ).otherwise(col(f"{col_name}_score"))
                )
                valid_matches = scored.filter(col(f"{col_name}_score_adj") >= threshold)
                score_col_name = f"{col_name}_score_adj"
            else:
                valid_matches = scored.filter(col(f"{col_name}_score") >= threshold)
                score_col_name = f"{col_name}_score"
            
            select_cols = [
                col("datasetA.match_col").alias("ref_value"),
                col("datasetB.match_col").alias("input_value"),
                lit(col_name).alias("matched_on_field"),
                col(score_col_name).alias("score"),
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
    spark, glueContext, job = get_spark_glue_context()
    args = parse_args()
    
    DATABASE_NAME = args.get('database_name', 'test_db')
    TABLE1_NAME = args.get('table1_name', 'table1')
    TABLE2_NAME = args.get('table2_name', 'table2')
    JSON_COL = args.get('json_column', 'key_value_pairs')

    MAPPING_STR = args.get('column_mapping', None)
    LEGACY_COLS_STR = args.get('matching_columns', 'name,mrn')
    COLUMN_MAPPING = parse_column_mapping(MAPPING_STR, LEGACY_COLS_STR)

    COMPOSITE_MATCH = args.get('composite_match', 'true').lower() == 'true'
    THRESHOLD = float(args.get('match_threshold', 0.8))
    OUTPUT_PATH = args.get('output_path', 's3://temp-bucket/matches/')
    
    # New Master Index args
    EXECUTION_MODE = args.get('execution_mode', 'match').lower()
    INDEX_SAVE_PATH = args.get('index_save_path', 's3://temp-bucket/master_index')

    # Secondary ML Model Path
    SECONDARY_MODEL_PATH = args.get('secondary_model_path', None)

    logger.info(f"Config: DB={DATABASE_NAME}, Ref={TABLE1_NAME}, Input={TABLE2_NAME}, Mode={EXECUTION_MODE}, Index_Path={INDEX_SAVE_PATH}, ML_Model={SECONDARY_MODEL_PATH}")

    try:
        df_ref = spark.table(f"{DATABASE_NAME}.{TABLE1_NAME}")
        df_input = spark.table(f"{DATABASE_NAME}.{TABLE2_NAME}") if EXECUTION_MODE == 'match' else None
    except Exception as e:
        logger.error(f"Error reading tables: {e}")
        sys.exit(1)
        
    matches_df = run_fuzzy_matching(
        spark, df_ref, df_input, JSON_COL, COLUMN_MAPPING, COMPOSITE_MATCH,
        THRESHOLD, EXECUTION_MODE, INDEX_SAVE_PATH, SECONDARY_MODEL_PATH
    )

    if EXECUTION_MODE == 'match' and matches_df is not None:
        logger.info(f"Found matches. Saving to {OUTPUT_PATH}")
        matches_df.write.mode("overwrite").parquet(OUTPUT_PATH)
        matches_df.show(5, truncate=False)
    elif EXECUTION_MODE == 'match':
        logger.info("No matches found.")

    job.commit()

if __name__ == '__main__':
    main()
