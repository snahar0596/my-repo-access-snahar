from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit
from awsglue.context import GlueContext
from awsglue.job import Job
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

df = pd.DataFrame({
    'Step': [step_name],
    'Timestamp': [execution_date],
    'Next': [next_step]
})

log.info("Sample DataFrame:\n"
         f"{df}")

sc = SparkContext.getOrCreate()  # sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")  # Enable dynamic partitioning for flexible DataFrame writes.


def find_polygon_center(polygon_points):
    """Find the center point of a polygon defined by 4 coordinates"""

    if not polygon_points or len(polygon_points) != 4:
        raise ValueError("Polygon must have exactly 4 points")
    
    # Calculate the average of all X coordinates
    center_x = sum(point['X'] for point in polygon_points) / len(polygon_points)
    
    # Calculate the average of all Y coordinates
    center_y = sum(point['Y'] for point in polygon_points) / len(polygon_points)

    return (round(center_x, 4), round(center_y, 4))


def textract_api_call(textract_client, incoming_bucket, image_key):
    """Function to call the 'detect_document_text' Textract API to retrieve OCR liquid text, out of the file images from the customers"""

    response = textract_client.detect_document_text(Document={
                                                            'S3Object': {
                                                                'Bucket': incoming_bucket,
                                                                'Name': image_key
                                                            }
                                                        })
    blocks = response['Blocks']
    initial_text = [(block['Text'], find_polygon_center(block['Geometry']['Polygon'])) for block in blocks if block['BlockType'] == 'LINE']

    log.info("A response was obtained from the 'detect_document_text' Textract API call...")
    return initial_text


def get_process_table_from_catalog(catalog_db, incoming_catalog_table, batch_name, transaction_id):
    """Function to query the AWS Glue Data Catalog, given the 'catalog_db' and 'incoming_catalog_table' to retrieve the needed information to process a.k.a. image paths"""

    try:
        query = f"""
        SELECT
            _incoming_table.image_path,
            _incoming_table.batch_name,
            _incoming_table.transaction_id
        FROM
            `{catalog_db}`.`{incoming_catalog_table}` AS _incoming_table
        WHERE
            _incoming_table.batch_name = '{batch_name}' and
            _incoming_table.transaction_id = '{transaction_id}'
        """

        log.info(f"The query to be executed is:\n\n{query}\n\n")
        query_result = spark.sql(query)

        log.info("Success! Data was read from Glue Catalog.")
        return query_result

    except Exception as e:
        log.error(f"Critical error in the 'get_process_table_from_catalog' function querying the table '{catalog_db}.{incoming_catalog_table}':\n{str(e)}\n")
        log.error(f"Error type: {type(e).__name__}")
        exit("OCR - Critical error")
        # Timestream call
        # raise RuntimeError("Failed to read from Glue Catalog.") from e


def get_glue_table_schema(glue_client, database_name, table_name):
    """Fetch the schema of a Glue table dynamically."""

    log.info(f"Fetching schema for table: '{database_name}.{table_name}'")

    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    columns = response["Table"]["StorageDescriptor"]["Columns"]
    ordered_columns = [_col["Name"] for _col in columns]

    log.info(f"Retrieved Glue schema order for table '{database_name}.{table_name}':\n{ordered_columns}")
    return ordered_columns


def validate_and_prepare_dataframe(final_sdf, expected_columns):
    """Validate and prepare DataFrame with required columns."""

    set_expected_cols = set(expected_columns)
    set_input_cols = set(final_sdf.columns)

    log.info(f"set_expected_cols:\n{set_expected_cols}\n"
          f"set_input_cols:\n{set_input_cols}")

    missing_columns = set_expected_cols - set_input_cols
    present_columns = set_expected_cols & set_input_cols
    extra_columns = set_input_cols - set_expected_cols

    log.info(f"missing_columns:\n{missing_columns}\n"
          f"present_columns:\n{present_columns}\n"
          f"extra_columns:\n{extra_columns}\n")

    if missing_columns:
        log.warning(f"Missing columns in input DataFrame:\n{missing_columns}")

        for col in missing_columns:  # Add missing columns as null columns
            final_sdf = final_sdf.withColumn(col, lit('null').cast(StringType()))

    else:
        log.info("There are not missing columns in the input dataframe with respect to the required or expected columns.")

    return final_sdf.select(*expected_columns)  # Select only the expected columns


def write_to_glue_catalog(glue_client, final_sdf, target_db, target_table_name):
    """Write DataFrame to Glue catalog."""

    log.info(f"Writing to Glue: '{target_db}.{target_table_name}'")

    # The expected column names are:
    #   - image_path
    #   - batch_name
    #   - transaction_id
    #   - ocr_liquid_text
    #   - key_value_pairs

    expected_columns = get_glue_table_schema(glue_client, target_db, target_table_name) + ['batch_name']
    prepared_df = validate_and_prepare_dataframe(final_sdf, expected_columns)

    log.info(f"Writing '{prepared_df.count()}' records to '{target_db}.{target_table_name}'")
    log.info(f"Final columns are:\n{prepared_df.columns}")

    # Write to Glue catalog
    prepared_df.coalesce(1).write.mode("append").format('parquet').insertInto(f"`{target_db}`.`{target_table_name}`")

    log.info("Successfully wrote to Glue catalog")


def safe_textract_api_columnar_operation(df,
                          source_col,
                          new_col,
                          func,
                          error_value="Could Not be Processed",
                          max_workers=10,
                          chunk_size=50,
                          **kwargs):

    """Apply safety function wrapping Textract API call with multithreading"""

    def safe_wrapper(textract_client, incoming_bucket, value):

        try:
            result = func(textract_client, incoming_bucket, value)
            return result if result is not None else error_value

        except Exception as e:
            log.error(f"Error in the 'textract_api_call':\n{str(e)}\n")
            log.error(f"Error type: {type(e).__name__}\n")
            # Possible Timestream call
            return error_value

    log.info("Applying the OCR function to all the rows using multithreading...")

    values = df[source_col].tolist()  # Extract the image_paths to process
    ocr_liquid_text_list = [None] * len(values)  # Initialize 'ocr_liquid_text_list' list with same length as values
    ocr_raw_results_list = [None] * len(values)  # Initialize 'ocr_raw_results_list' list with same length as values
    total_chunks = (len(values) - 1) // chunk_size + 1  # Calculate total chunks

    # Get the client and bucket from kwargs once
    textract_client = kwargs.get('textract_client')
    incoming_bucket = kwargs.get('incoming_bucket')

    # Process in chunks
    for chunk_start in range(0, len(values), chunk_size):
        chunk_end = min(chunk_start + chunk_size, len(values))
        chunk_values = values[chunk_start:chunk_end]
        
        current_chunk = chunk_start // chunk_size + 1
        rows_in_chunk = chunk_end - chunk_start
        
        log.info(f"Processing chunk {current_chunk}/{total_chunks}\n"
                f"(rows {chunk_start}-{chunk_end-1}, {rows_in_chunk} rows)\n")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

            future_to_index = {executor.submit(safe_wrapper, textract_client, incoming_bucket, value): chunk_start + idx for idx, value in enumerate(chunk_values)}

            # Collecting results as they complete
            for future in concurrent.futures.as_completed(future_to_index):

                try:
                    idx = future_to_index[future]
                    ocr_raw_result = future.result()  # Get the raw result
                    liquid_text = ' '.join([text for text, _ in ocr_raw_result])

                    ocr_liquid_text_list[idx] = liquid_text
                    ocr_raw_results_list[idx] = json.dumps(ocr_raw_result)


                except Exception as e:
                    log.error(f"An error occurred while retrieving the results from futures: {e}")
                    ocr_liquid_text_list[idx] = error_value
                    ocr_raw_results_list[idx] = error_value

    df[new_col] = ocr_liquid_text_list  # Assign back
    df['ocr_spatial_info'] = ocr_raw_results_list  # Assign back

    return df


def process_file(process_df):
    """Create a Spark DataFrame from a pandas one."""

    all_columns = process_df.columns.tolist()

    log.info("Creating the Spark Dataframe out from the Pandas Dataframe...")

    schema = StructType(
        [StructField(column_name, StringType(), nullable=True) for column_name in all_columns]
        )

    final_spark_df = spark.createDataFrame(process_df, schema=schema)  # Create the Spark Dataframe

    log.info("Spark Dataframe created.")

    return final_spark_df


def main():
    """Main function"""
    try:

        args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                             'WORKFLOW_NAME',
                                             'WORKFLOW_RUN_ID',
                                             'CatalogDB',
                                             'IncomingCatalogTable',
                                             'EnrichmentCatalogTable',
                                             'TimestreamDB',
                                             'TimestreamTable',
                                             'TimestreamSQSQueue'])

        # Coming variables from the Job itself
        job_name = args['JOB_NAME']
        workflow_name = args['WORKFLOW_NAME']
        workflow_run_id = args['WORKFLOW_RUN_ID']
        catalog_db = args['CatalogDB']  # 'prod-csp-glue-onb-processing-db'
        incoming_catalog_table = args['IncomingCatalogTable']  # onb_incoming_data
        enrichment_catalog_table = args['EnrichmentCatalogTable']  # onb_enrichment_data

        job.init(job_name, args)

        log.info(f"job_name: '{job_name}'\n"
                f"workflow_name: '{workflow_name}'\n"
                f"workflow_run_id: '{workflow_run_id}'\n"
                f"catalog_db: '{catalog_db}'\n"
                f"incoming_catalog_table: '{incoming_catalog_table}'\n")

        # Declaration of clients
        config = Config(
            retries={
                'max_attempts': 10,
                'mode': 'adaptive'
            }
        )
        
        glue_client = boto3.client('glue')
        textract_client = boto3.client('textract', config=config)

        workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)["RunProperties"]

        # Coming variables from the Workflow
        transaction_id = workflow_params['transaction_id']
        source = workflow_params['source']
        zip_key = workflow_params['zip_key']  # Radix/alegentsample-20250806035111.zip  # Radix/casc00252581-20250714034706.zip
        incoming_bucket = workflow_params['incoming_bucket']  # prod-csp-s3-etl-source-stg
        source_device = workflow_params['source_device']  # Eg: Radix, tr_image (handheld)

        log.info(f"transaction_id: '{transaction_id}'\n"
                f"source: '{source}'\n"
                f"zip_key: '{zip_key}'\n"
                f"incoming_bucket: '{incoming_bucket}'\n"
                f"source_device: '{source_device}'\n")

        batch_name = zip_key.rsplit('/', 1)[-1].rsplit('.', 1)[0]  # casc00252581-20250714034706
        log.info(f"batch_name: '{batch_name}'")

        input_sdf = get_process_table_from_catalog(catalog_db, incoming_catalog_table, batch_name, transaction_id)

        process_df = input_sdf.toPandas()  # Using pandas for simplicity instead of Spark in all operations

        Onboarding_Timestream_Manager.timestream_insert_data(
            db=args["TimestreamDB"],
            table=args["TimestreamTable"],
            timestream_queue=args["TimestreamSQSQueue"],
            measure_name=f'OnboardingWF_{pipeline_mod}',
            content=Onboarding_Timestream_Manager.create_timestream_content(
                source=source,
                source_device=source_device,
                transaction_id=transaction_id,
                batch_name=batch_name,
                pipeline_mod=pipeline_mod,
                state="Ready",
                no_of_asset=str(len(process_df))
            )
        )

        log.info(f"Successfully read data!\n\n"
            f"Shape for input frame:\n{process_df.shape}\n\n"
            f"First 10 rows:\n{process_df.head(10).to_string()}\n\n"
            f"Input types:\n{process_df.dtypes}\n"
            f"Original columns:\n{process_df.columns.tolist()}\n")

        log.info("Checking if the Pandas Dataframe is empty or not...")

        if process_df.empty:
            raise ValueError("¡No data to write or DataFrame is empty or invalid!...")
        else:
            log.info("Pandas Dataframe is not empty.")

        process_df = safe_textract_api_columnar_operation(process_df,
                                   'image_path',
                                   'ocr_liquid_text',
                                   textract_api_call,
                                   error_value="Could Not be Processed",
                                   max_workers=10,
                                   chunk_size=50,
                                   textract_client=textract_client,
                                   incoming_bucket=incoming_bucket)

        log.info("The OCR function was applied to all targeted documents....")

        log.info(f"Showing the first 10 rows out of the processed dataframe:\n{process_df.head(10)}")
        

        final_sdf = process_file(process_df)

        processed_rows = final_sdf.sort("image_path").collect()
        for row in processed_rows:
            Onboarding_Timestream_Manager.timestream_insert_data(
                db=args["TimestreamDB"],
                table=args["TimestreamTable"],
                timestream_queue=args["TimestreamSQSQueue"],
                measure_name=f'OnboardingWF_{pipeline_mod}',
                content=Onboarding_Timestream_Manager.create_timestream_content(
                    source=source,
                    source_device=source_device,
                    transaction_id=transaction_id,
                    batch_name=batch_name,
                    pipeline_mod=pipeline_mod,
                    state="Complete",
                    no_of_asset="1",
                    file_path=str(row['image_path']),
                    event_name="ImageTextExtract"
                )
            )

        write_to_glue_catalog(glue_client, final_sdf, catalog_db, enrichment_catalog_table)

        final_record_count = final_sdf.count()
        log.info(f"Records Written in the OcrImage Module: {final_record_count}")
        Onboarding_Timestream_Manager.timestream_insert_data(
            db=args["TimestreamDB"],
            table=args["TimestreamTable"],
            timestream_queue=args["TimestreamSQSQueue"],
            measure_name=f'OnboardingWF_{pipeline_mod}',
            content=Onboarding_Timestream_Manager.create_timestream_content(
                source=source,
                source_device=source_device,
                transaction_id=transaction_id,
                batch_name=batch_name,
                pipeline_mod=pipeline_mod,
                state="Complete",
                no_of_asset=str(final_record_count)
            )
        )

        log.info("Job completed successfully")

    except Exception as e:
        log.error(f"Critical error in the 'main' function:\n{str(e)}")
        log.error(f"Error type: {type(e).__name__}")

        Onboarding_Timestream_Manager.timestream_insert_data(
            db=args["TimestreamDB"],
            table=args["TimestreamTable"],
            timestream_queue=args["TimestreamSQSQueue"],
            measure_name=f'OnboardingWF_{pipeline_mod}',
            content=Onboarding_Timestream_Manager.create_timestream_failure_content(
                source=source,
                source_device=source_device,
                transaction_id=transaction_id,
                batch_name=batch_name,
                pipeline_mod=pipeline_mod,
                error=str(e),
                error_code="42",
                record_count="0"
            )
        )
        raise RuntimeError("General process failed") from e

    finally:
        job.commit()


if __name__ == "__main__":
    main()
