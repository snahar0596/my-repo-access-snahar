import sys
import logging
import re
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, concat_ws, udf, from_json, lit, coalesce, when, length, levenshtein, greatest, abs as abs_func, array, array_contains, create_map, to_date, date_format
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, FloatType, BooleanType, ArrayType
from pyspark.ml.feature import HashingTF, MinHashLSH, Tokenizer, NGram
from pyspark.ml import Pipeline
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
    # Import AWS Glue only inside the function to allow PySpark workers to run UDFs without Glue lib
    from awsglue.context import GlueContext
    from awsglue.job import Job

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    return spark, glueContext, job

def parse_args():
    """Parses Glue job arguments."""
    from awsglue.utils import getResolvedOptions

    args_list = [
        'JOB_NAME',
        'database_name',
        'table1_name',              # Reference Table
        'table2_name',              # Input/OCR Table
        'json_column',              # Name of JSON column in input table
        'output_path'
    ]
    try:
        args = getResolvedOptions(sys.argv, args_list)
    except Exception as e:
        logger.warning(f"Could not resolve arguments: {e}. Using defaults.")
        args = {}
    return args

# --- Business Logic: Normalization (Ported from User Code) ---

def normalize_phone_udf(phone):
    if not phone: return ""
    # Remove non-digits
    return re.sub(r'[^0-9]', '', str(phone))

def normalize_name_udf(name):
    if not name: return ""
    # Remove special chars and spaces, lowercase
    # Based on user's replace chain: # : > < $ " ^ = ( ) types noclub club
    val = str(name).lower()
    val = re.sub(r'[#:> <\$\"\^=\(\)]', '', val)
    val = val.replace('types', '').replace('noclub', '').replace('club', '')
    return val.strip()

def normalize_date_udf(date_str):
    if not date_str: return ""
    # User logic: remove '/'
    # Also tries to parse date. simplified here.
    return str(date_str).replace('/', '').strip()

# Register UDFs for complex logic, but use Spark SQL for performance where possible
normalize_phone = udf(normalize_phone_udf, StringType())
normalize_name = udf(normalize_name_udf, StringType())
normalize_date = udf(normalize_date_udf, StringType())

def clean_dataframe(df):
    """
    Applies normalization to DataFrame columns based on user's logic.
    """
    # Define columns to clean if they exist
    cols_to_clean = ['FirstName', 'LastName', 'PrevLastName', 'Phone1', 'DOB']

    # Check which exist
    existing_cols = [c for c in cols_to_clean if c in df.columns]

    if 'FirstName' in existing_cols:
        df = df.withColumn('FirstName', normalize_name(col('FirstName')))

    if 'LastName' in existing_cols:
        df = df.withColumn('LastName', normalize_name(col('LastName')))

    if 'PrevLastName' in existing_cols:
        df = df.withColumn('PrevLastName', normalize_name(col('PrevLastName')))
    else:
        # If missing, add empty column for logic consistency
        df = df.withColumn('PrevLastName', lit(""))

    if 'Phone1' in existing_cols:
        df = df.withColumn('NORMALIZED_PHONE', normalize_phone(col('Phone1')))
    else:
        df = df.withColumn('NORMALIZED_PHONE', lit(""))

    if 'DOB' in existing_cols:
        # Simple normalization: remove slashes
        df = df.withColumn('NORMALIZED_DOB', normalize_date(col('DOB')))
    else:
        df = df.withColumn('NORMALIZED_DOB', lit(""))

    return df

# --- Business Logic: Scoring & Error Codes ---

def calculate_similarity_score(col1, col2):
    """
    Calculates similarity (0-100) based on Levenshtein distance.
    Matches user's isSimilarValue logic (SequenceMatcher ratio * 100).
    Spark Levenshtein is close approximation for edit distance.
    """
    dist = levenshtein(col1, col2)
    max_len = greatest(length(col1), length(col2))
    # Avoid div by zero
    ratio = when(max_len == 0, 100.0).otherwise((1.0 - (dist / max_len)) * 100.0)
    return ratio

def apply_matching_rules(df_joined):
    """
    Implements the complex decision tree from user's `query_merge_results`.
    Assumes df_joined has 'ref' (Master) and 'input' (OCR) columns.
    """

    # Calculate Similarities
    df = df_joined.withColumn("sim_firstname", calculate_similarity_score(col("ref_FirstName"), col("input_FirstName")))
    df = df.withColumn("sim_lastname", calculate_similarity_score(col("ref_LastName"), col("input_LastName")))
    df = df.withColumn("sim_prev_lastname", calculate_similarity_score(col("ref_PrevLastName"), col("input_LastName"))) # Check Input Last against Ref Prev
    df = df.withColumn("sim_dob", calculate_similarity_score(col("ref_NORMALIZED_DOB"), col("input_NORMALIZED_DOB")))
    df = df.withColumn("sim_phone", calculate_similarity_score(col("ref_NORMALIZED_PHONE"), col("input_NORMALIZED_PHONE")))

    # Exact Matches (User logic: isSimilarExact >= 90)
    # Fuzzy Matches (User logic: isSimilarFuzzy > 66 or > 61)

    # Define Match Conditions based on user's logic flow

    # Condition 1: Perfect Match on all fields (Score > 90 for all)
    # User's logic: if len(result) == len(vp_fields) -> Error = "", MatchFound = "Yes"
    # We approximate this by checking if high similarity exists on core fields

    # Phone Match Logic:
    # if(isSimilarExact(phone) and isSimilarExact(FirstName))
    cond_phone_match = (col("sim_phone") >= 90) & (col("sim_firstname") >= 90)

    # Name + DOB Match Logic:
    # (First == First) AND (Last == Last OR Last == PrevLast) AND (DOB == DOB OR Phone == Phone)
    cond_name_dob_match = (
        (col("sim_firstname") >= 90) &
        ((col("sim_lastname") >= 90) | (col("sim_prev_lastname") >= 90)) &
        ((col("sim_dob") >= 90) | (col("sim_phone") >= 90))
    )

    # Assign Status
    df = df.withColumn("MatchFound",
        when(cond_phone_match | cond_name_dob_match, lit("Yes"))
        .otherwise(lit("No"))
    )

    # Error Codes (mismatch fields)
    # User logic: "FirstName, LastName, " if mismatch
    # We construct this string dynamically

    error_expr = concat_ws(", ",
        when(col("sim_firstname") < 90, lit("FirstName")).otherwise(lit(None)),
        when(col("sim_lastname") < 90, lit("LastName")).otherwise(lit(None)),
        when(col("sim_dob") < 90, lit("DOB")).otherwise(lit(None)),
        when(col("sim_phone") < 90, lit("Phone")).otherwise(lit(None))
    )

    df = df.withColumn("Error", error_expr)

    # Confidence Score (Placeholder logic based on average similarity)
    df = df.withColumn("ConfidenceScore",
        (col("sim_firstname") + col("sim_lastname") + col("sim_dob")) / 3.0
    )

    return df

def run_fuzzy_matching_pipeline(df_ref, df_input, json_col):
    """
    Main pipeline:
    1. Parse Input JSON
    2. Normalize Ref and Input
    3. Blocking (LSH) on 'FirstName + LastName' (Composite Key)
    4. Fuzzy Scoring & Logic Application
    """

    # 1. Parse JSON
    # We assume standard keys: FirstName, LastName, Phone1, DOB
    schema = StructType([
        StructField("FirstName", StringType(), True),
        StructField("LastName", StringType(), True),
        StructField("Phone1", StringType(), True),
        StructField("DOB", StringType(), True),
        StructField("PrevLastName", StringType(), True)
    ])

    df_input_parsed = df_input.withColumn("parsed", from_json(col(json_col), schema)) \
        .select(col("*"), col("parsed.*")).drop("parsed", json_col)

    # 2. Normalize
    df_ref_clean = clean_dataframe(df_ref).withColumnRenamed("FirstName", "ref_FirstName") \
                                          .withColumnRenamed("LastName", "ref_LastName") \
                                          .withColumnRenamed("PrevLastName", "ref_PrevLastName") \
                                          .withColumnRenamed("NORMALIZED_PHONE", "ref_NORMALIZED_PHONE") \
                                          .withColumnRenamed("NORMALIZED_DOB", "ref_NORMALIZED_DOB")

    df_input_clean = clean_dataframe(df_input_parsed).withColumnRenamed("FirstName", "input_FirstName") \
                                                     .withColumnRenamed("LastName", "input_LastName") \
                                                     .withColumnRenamed("PrevLastName", "input_PrevLastName") \
                                                     .withColumnRenamed("NORMALIZED_PHONE", "input_NORMALIZED_PHONE") \
                                                     .withColumnRenamed("NORMALIZED_DOB", "input_NORMALIZED_DOB")

    # 3. Blocking Strategy (MinHash LSH)
    # We block on concatenated FirstName + LastName to find candidates

    # Create Blocking Keys
    # We use character n-grams to improve recall on fuzzy names (e.g., Reyes vs Rayes)
    # Splitting by character requires a UDF or regex split

    split_chars = udf(lambda x: list(str(x)) if x else [], ArrayType(StringType()))

    df_ref_ready = df_ref_clean.withColumn("blocking_key", concat_ws(" ", col("ref_FirstName"), col("ref_LastName")))
    df_input_ready = df_input_clean.withColumn("blocking_key", concat_ws(" ", col("input_FirstName"), col("input_LastName")))

    # Filter empty keys
    df_ref_ready = df_ref_ready.filter(length(col("blocking_key")) > 0)
    df_input_ready = df_input_ready.filter(length(col("blocking_key")) > 0)

    # Convert string to array of characters for NGram input
    # Note: Spark NGram expects array of strings.
    # If we pass "Jeffrey Murphy" -> ["J", "e", "f", "f"...] -> 3-grams: "Jef", "eff", "ffr"...

    df_ref_ready = df_ref_ready.withColumn("char_tokens", split_chars(col("blocking_key")))
    df_input_ready = df_input_ready.withColumn("char_tokens", split_chars(col("blocking_key")))

    # Pipeline
    ngram = NGram(n=3, inputCol="char_tokens", outputCol="ngrams")
    hashingTF = HashingTF(inputCol="ngrams", outputCol="features")
    lsh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)

    pipeline = Pipeline(stages=[ngram, hashingTF, lsh])

    # Fit on Reference
    model = pipeline.fit(df_ref_ready)
    df_ref_hashed = model.transform(df_ref_ready)

    # Use the SAME model to transform Input.
    # Important: NGram and HashingTF are stateless transformers (mostly),
    # but fit() ensures consistency if we used CountVectorizer. HashingTF is fine.
    df_input_hashed = model.transform(df_input_ready)

    # Join (Threshold 0.8 Jaccard distance = loose match to get candidates)
    candidates = model.stages[-1].approxSimilarityJoin(
        df_ref_hashed,
        df_input_hashed,
        0.8,
        distCol="jaccard_dist"
    )

    # Flatten columns (datasetA = Ref, datasetB = Input)
    # Select all relevant columns

    # Helper to select cols
    sel_cols = []
    for c in df_ref_clean.columns:
        sel_cols.append(col(f"datasetA.{c}"))
    for c in df_input_clean.columns:
        sel_cols.append(col(f"datasetB.{c}"))

    candidates_flat = candidates.select(*sel_cols)

    # 4. Apply Business Logic Rules
    results = apply_matching_rules(candidates_flat)

    # Filter to keep only "MatchFound = Yes" or similar logic?
    # User code keeps everything but flags it. We will return all candidates processed.

    return results

def main():
    spark, glueContext, job = get_spark_glue_context()
    args = parse_args()

    DATABASE_NAME = args.get('database_name', 'test_db')
    TABLE1_NAME = args.get('table1_name', 'table1') # Reference
    TABLE2_NAME = args.get('table2_name', 'table2') # Input
    JSON_COL = args.get('json_column', 'key_value_pairs')
    OUTPUT_PATH = args.get('output_path', 's3://temp-bucket/matches/')

    logger.info(f"Starting Custom Logic Match Job...")

    try:
        df_ref = spark.table(f"{DATABASE_NAME}.{TABLE1_NAME}")
        df_input = spark.table(f"{DATABASE_NAME}.{TABLE2_NAME}")
    except Exception as e:
        logger.error(f"Error reading tables: {e}")
        sys.exit(1)

    matches_df = run_fuzzy_matching_pipeline(df_ref, df_input, JSON_COL)

    if matches_df:
        logger.info(f"Saving results to {OUTPUT_PATH}")
        matches_df.write.mode("overwrite").parquet(OUTPUT_PATH)
        matches_df.select("ref_FirstName", "input_FirstName", "MatchFound", "Error", "ConfidenceScore").show(10, truncate=False)

    job.commit()

if __name__ == '__main__':
    main()
