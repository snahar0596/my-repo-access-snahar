import sys
import pandas as pd
import logging
import json
import re
import concurrent.futures
import boto3
import textwrap
import time
from awsglue.utils import getResolvedOptions
from Onboarding_Glue_Job_Base import GlueJobBase, JobVariables, WorkflowParams
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime, timedelta, timezone
from botocore.config import Config
from botocore.client import BaseClient
from pyspark.sql.types import StructType, StructField, StringType
from functools import wraps
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


class PatternExtractorHelper:
    def __init__(self, patterns_dict: dict):
        """
        Initialize the helper with regex patterns for value extraction.

        Args:
            patterns_dict (dict): Dictionary defining extraction rules, where
                each key maps to a configuration containing a regex pattern.

        Returns:
            None
        """
        self.patterns_dict = patterns_dict
        self.compiled_patterns = self._compile_patterns(patterns_dict)

    def _compile_patterns(self, patterns_dict: dict) -> dict:
        """
        Precompile all regex patterns for improved runtime performance.

        Args:
            patterns_dict (dict): Dictionary containing regex patterns to
                compile for each extraction key.

        Returns:
            dict: Dictionary mapping keys to compiled regex patterns.
        """
        compiled = {}

        for key, config in patterns_dict.items():
            pattern = config.get("pattern", "")

            if pattern:
                try:
                    compiled[key] = re.compile(pattern)
                except Exception as e:
                    logging.error(
                        f"Invalid regex pattern for key '{key}':\n"
                        f"{pattern}. Error: {e}"
                    )
        return compiled

    @log_execution
    def extract_and_replace(self, corpus_text: str, json_str: str) -> dict:
        """
        Extract values from text using regex and update JSON data.

        For each configured pattern, this method searches the corpus text
        when the corresponding JSON field is empty or null, then fills it
        with the extracted value and updates its score if present.

        Args:
            corpus_text: OCR or raw text used for pattern matching.
            json_str (str): JSON string containing fields to enrich.

        Returns:
            dict: Updated dictionary with extracted values applied.
        """
        data = json.loads(json_str)

        for key, regex in self.compiled_patterns.items():
            if key in data:
                value = data.get(key)

                if value is None or value == "":
                    match = regex.search(
                        ' '.join(string_value for string_value, _boundaries_ in corpus_text)
                    )

                    if match:
                        extracted_value = match.group()
                        logging.info(f"Helper matched key '{key}' in corpus text.")
                        logging.info(f"Before helper update: '{data}'")

                        data[key] = extracted_value

                        score_key = f"{key}_s"
                        if score_key in data:
                            data[score_key] = 10

                        logging.info(f"After helper update: '{data}'")

        return data


class TextExtractor(ABC):
    @abstractmethod
    def extract_text(self):
        pass


class WithConfigurationExtractor(TextExtractor):
    def __init__(
            self,
            collection_config: dict,
            batch_name: str,
            transaction_id: str,
            pipeline_mod: str,
            model_id: str,
            job_vars: JobVariables,
            workflow_vars: WorkflowParams,
            incoming_catalog_data: GlueCatalog,
            enrichment_catalog_data: GlueCatalog,
            spark: SparkSession,
            glue_client: BaseClient,
    ):
        self.collection_config = collection_config
        self.batch_name = batch_name
        self.transaction_id = transaction_id
        self.pipeline_mod = pipeline_mod
        self.model_id = model_id
        self.job_vars = job_vars
        self.workflow_vars = workflow_vars
        self.incoming_catalog_data = incoming_catalog_data
        self.enrichment_catalog_data = enrichment_catalog_data
        self.spark = spark
        self.glue_client = glue_client

    @log_execution
    def get_process_table_from_catalog(
        self,
        glue_catalog: GlueCatalog
    ) -> DataFrame:
        """
        Retrieve OCR processing data from the Glue catalog for the current batch.

        This method queries the Glue catalog table associated with OCR enrichment
        data and filters records by batch name and transaction id.

        Args:
            glue_catalog (GlueCatalog): Glue catalog database and table containing
                OCR enrichment data.

        Returns:
            DataFrame: Spark DataFrame with image path, batch name, transaction id,
                and OCR liquid text columns.

        Raises:
            SystemExit: If a critical error occurs while querying the catalog.
        """
        try:
            db = glue_catalog.catalog_db
            table = glue_catalog.catalog_table
            query = f"""
            SELECT
                _ocr_table.image_path,
                _ocr_table.batch_name,
                _ocr_table.transaction_id,
                _ocr_table.ocr_liquid_text,
                _ocr_table.ocr_spatial_info
            FROM
                `{db}`.`{table}` AS _ocr_table
            WHERE
                _ocr_table.batch_name = '{self.batch_name}' and
                _ocr_table.transaction_id = '{self.transaction_id}'
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
            exit("Extract module - Critical error")

    @log_execution
    def get_pd_df_and_display_info(
        self,
        spark_df: DataFrame
    ) -> pd.DataFrame:
        """
        Convert a Spark DataFrame to Pandas and log inspection information.

        This method logs structural and sample information about the data and
        emits a Timestream event indicating the dataset is ready for processing.

        Args:
            spark_df (DataFrame): Input Spark DataFrame to convert.

        Returns:
            pd.DataFrame: Converted Pandas DataFrame.

        Raises:
            ValueError: If the resulting DataFrame is empty or invalid.
        """
        process_df = spark_df.toPandas()
        logging.info(
            f"Successfully read data!\n\n"
            f"Shape for input frame:\n{process_df.shape}\n\n"
            f"First 10 rows:\n{process_df.head(10).to_string()}\n\n"
            f"Input types:\n{process_df.dtypes}\n"
            f"Original columns:\n{process_df.columns.tolist()}\n"
        )
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
                state="Ready",
                no_of_asset=str(process_df.shape[0]),
                pipeline_mod=self.pipeline_mod,
            ),
        )

        if process_df.empty:
            raise ValueError(
                "No data to write or DataFrame is empty or invalid"
            )

        logging.info("Pandas Dataframe is not empty.")
        return process_df

    @staticmethod
    @log_execution
    def format_validation_date_rules(
        field_list: list[dict]
    ) -> list[dict]:
        """
        Normalize date validation rules by resolving dynamic 'today' values.

        This method scans the validation rules for each field and replaces any
        occurrence of the string value 'today' with the current date formatted
        according to the date format specified in the configuration template.

        Supported formats include:
        - mm/dd/yyyy
        - dd/mm/yyyy
        - yyyy-mm-dd

        Args:
            field_list (list[dict]): List of field configuration dictionaries
                containing validation rules for a customer-collection pair.

        Returns:
            list[dict]: Updated field configuration list with resolved date
                values applied.
        """
        logging.info("Extracting the validation template for the customer...")

        format_mapping = {
            "mm/dd/yyyy": "%m/%d/%Y",
            "dd/mm/yyyy": "%d/%m/%Y",
            "yyyy-mm-dd": "%Y-%m-%d",
        }
        utc_minus_5 = timezone(timedelta(hours=-5))

        for dict_struct in field_list:
            if "Validation" in dict_struct:
                validation = dict_struct["Validation"]

                for key, value in validation.items():
                    if isinstance(value, str) and value.lower() == "today":
                        date_format = validation.get("Format", "mm/dd/yyyy")
                        python_format = format_mapping.get(
                            date_format,
                            "%m/%d/%Y",
                        )
                        validation[key] = datetime.now(
                            utc_minus_5
                        ).strftime(python_format)

        logging.info(
            f"The validation template parsing all 'Today' occurrences is:\n"
            f"\n\n{json.dumps(field_list, indent=4)}\n\n"
        )

        return field_list

    @staticmethod
    @log_execution
    def preprocess_validation_template(
        validation_template: list[dict]
    ) -> dict:
        """
        Transform a validation template into an extraction configuration.

        This method converts the field-level validation rules into a normalized
        configuration dictionary that can be used by the key-value extraction
        logic and LLM prompt generation.

        Args:
            validation_template (list[dict]): List of field configuration
                dictionaries defining validation rules and extraction metadata.

        Returns:
            dict: Dictionary keyed by field name, where each value contains
                pattern, type, alternative labels, and force-case rules.
        """
        extraction_config = {}

        for field_config in validation_template:
            field_name = field_config.get("FieldName")
            validation = field_config.get("Validation", {})
            pattern = validation.get("Pattern", "")
            obj_type = field_config.get("Type", "string")
            altern_labels = field_config.get("AlternativeLabels", [])
            force_case = field_config.get("ForceCase", "AS-IS")

            extraction_config[field_name] = {
                "pattern": pattern,
                "type": obj_type,
                "alternative_labels": altern_labels,
                "force_case": force_case,
            }

        logging.info(
            "Processed Extraction Configuration:\n"
            f"{json.dumps(extraction_config, indent=4)}"
        )
        return extraction_config

    @staticmethod
    @log_execution
    def get_the_keys_to_extract(
        validation_template: list[dict]
    ) -> list[str]:
        """
        Extract the list of field names to be processed for key-value extraction.

        Args:
            validation_template (list[dict]): List of field configuration
                dictionaries defining extraction rules.

        Returns:
            list[str]: List of field names to extract.
        """
        logging.info("Extracting the keys for the given validation template...")
        extracted_keys = [
            item.get("FieldName") for item in validation_template
        ]
        logging.info(f"The extracted_keys are: '{extracted_keys}'\n")
        return extracted_keys

    @staticmethod
    @log_execution
    def get_structural_tokens(
        initial_keys: list
    ) -> tuple[int, list[str]]:
        """
        Calculate the number of structural tokens required for a JSON response.

        This method estimates the token count contributed by the JSON structure
        itself (keys, quotes, colons, commas, and newlines), independently of
        the actual extracted values. Confidence score keys ({field}_s) are also
        included in the calculation.

        Args:
            initial_keys (list): Base list of field names to extract.

        Returns:
            tuple[int, list[str]]: Total number of structural tokens and the
                expanded list of keys including confidence score fields.
        """
        keys_to_extract = initial_keys + [f"{key}_s" for key in initial_keys]
        num_keys = len(keys_to_extract)

        base_tokens = 4  # Opening/closing braces and newline characters

        # Structural tokens per key-value pair:
        # Format: "key": "value",\n
        structural_tokens_per_pair = (
            2 +  # Indentation spaces
            2 +  # Quotes around the key
            1 +  # Colon
            1 +  # Space after colon
            2 +  # Quotes around the value
            1 +  # Comma
            1    # Newline
        )

        # Subtract one comma for the last key-value pair
        total_structural_tokens = (
            base_tokens + (num_keys * structural_tokens_per_pair) - 1
        )

        logging.info(
            f"base_tokens: '{base_tokens}'\n"
            f"structural_tokens_per_pair: '{structural_tokens_per_pair}'\n"
        )

        return total_structural_tokens, keys_to_extract

    @staticmethod
    @log_execution
    def get_total_value_chars(
        max_value_lengths: dict | None,
        keys_to_extract: list[str]
    ) -> int:
        """
        Estimate the total number of characters for extracted field values.

        This method calculates an estimated character count for all values
        expected in the extraction output. If explicit maximum lengths are
        provided, they are used; otherwise, default heuristics are applied
        based on common field types and confidence score fields.

        Args:
            max_value_lengths (dict | None): Optional dictionary mapping field
                names to their maximum expected character lengths.
            keys_to_extract (list[str]): List of keys to be included in the
                extraction output, including confidence score fields.

        Returns:
            int: Estimated total number of characters for all extracted values.
        """
        total_value_chars = 0

        if max_value_lengths:
            for key in keys_to_extract:
                total_value_chars += max_value_lengths.get(key, 50)
        else:
            default_lengths = {
                "score_key": 3,
                "patient name": 43,
                "date of birth": 12,
                "provider name": 40,
                "provider address": 50,
                "medications": 50,
                "principal diagnosis": 40,
                "pat's hi claim": 20,
                "sex": 5,
                "name": 43,
                "dob": 12,
                "mrn": 20,
                "acc_num": 20,
                "admit_date": 12,
                "discharge_date": 12,
            }

            for key in keys_to_extract:
                key_lower = key.lower()

                if key_lower.endswith("_s"):
                    total_value_chars += default_lengths.get(
                        "score_key",
                        3,
                    )
                else:
                    total_value_chars += default_lengths.get(
                        key_lower,
                        50,
                    )

        return total_value_chars

    @log_execution
    def calculate_max_tokens(
        initial_keys,
        max_value_lengths=None,
        chars_per_token=3.5
    ):
        """
        Estimate the maximum LLM tokens required for a JSON extraction response.

        The estimate includes:
        - Structural JSON tokens (quotes, colons, commas, newlines).
        - Tokens contributed by keys (including confidence keys {key}_s).
        - Tokens contributed by values using an average chars-per-token ratio.
        A safety buffer is applied to reduce truncation risk.

        Args:
            initial_keys (list): List of keys to extract.
            max_value_lengths (dict | None): Optional mapping of keys to their
                maximum expected value lengths in characters.
            chars_per_token (float): Average characters per token.

        Returns:
            int: Estimated maximum number of tokens for the model response.
        """
        logging.info("Calculating the max tokens per request...")

        total_structural_tokens, keys_to_extract = (
            WithConfigurationExtractor.get_structural_tokens(initial_keys)
        )
        total_value_chars = WithConfigurationExtractor.get_total_value_chars(
            max_value_lengths,
            keys_to_extract,
        )

        value_tokens = total_value_chars / chars_per_token
        key_tokens = sum(len(key) / chars_per_token for key in keys_to_extract)
        total_tokens = total_structural_tokens + value_tokens + key_tokens

        max_tokens = int(total_tokens * 1.10)

        logging.info(
            f"num_keys: '{len(keys_to_extract)}'\n"
            f"chars_per_token: '{chars_per_token}'\n"
            f"total_structural_tokens: '{total_structural_tokens}'\n"
            f"total_value_chars: '{total_value_chars}'\n"
            f"value_tokens: '{value_tokens}'\n"
            f"key_tokens: '{key_tokens}'\n"
            f"total_tokens: '{total_tokens}'\n"
            f"Estimated max tokens: '{max_tokens}'\n"
        )

        return max_tokens

    @log_execution
    def safe_key_value_extraction(
        self,
        df: pd.DataFrame,
        source_col: str,
        new_col: str,
        func,
        extraction_config,
        llm_helper,
        model_id,
        max_tokens,
        error_value: dict = {"error":"Could Not be Processed"},
        max_workers: int = 10,
        chunk_size: int = 50
    ) -> pd.DataFrame:
        """
        Apply key-value extraction to a DataFrame with error handling and threads.

        This method processes the text column in chunks, applies the provided
        extraction function in parallel, optionally enriches results using the
        helper, and writes the output into a new column. A Timestream event is
        emitted per processed row.

        Args:
            df (pd.DataFrame): Input Pandas DataFrame to process.
            source_col (str): Name of the column containing the input text.
            new_col (str): Name of the column to store extraction results.
            func (callable): Extraction function that returns a JSON string.
            extraction_config (dict): Field extraction configuration.
            llm_helper (object): Helper to post-process and fill missing values.
            model_id (str): LLM model identifier.
            max_tokens (int): Maximum tokens allowed for the model response.
            error_value (str): Value to store when extraction fails.
            max_workers (int): Number of threads used for parallel processing.
            chunk_size (int): Number of rows processed per thread batch.

        Returns:
            pd.DataFrame: Updated DataFrame with the new_col populated.
        """
        @log_execution
        def safe_wrapper(corpus_text):
            try:
                json_str_llm_model_result = func(
                    corpus_text,
                    extraction_config,
                    model_id,
                    max_tokens,
                )
                if json_str_llm_model_result is not None:
                    helper_result = llm_helper.extract_and_replace(
                        corpus_text,
                        json_str_llm_model_result,
                    )
                    return helper_result
                return error_value
            except Exception as e:
                logging.error(
                    f"Error in the 'extract_keys_from_text()'\n"
                    f"function call:\n{str(e)}\n"
                    f"Error type: {type(e).__name__}\n"
                )
                return error_value

        logging.info(
            "Applying the Key-Value extractor function to all the rows using multithreading..."
        )

        # df[source_col].tolist() -> List of String representations (for each record str repr containing list of lists).
        # texts -> Then, every element loaded into python objs -> Lists of lists
        texts = [json.loads(item) for item in df[source_col].tolist()]
        logging.info(texts)
        image_paths = df["image_path"].tolist()
        results = [None] * len(texts)
        total_chunks = (len(texts) - 1) // chunk_size + 1

        for chunk_start in range(0, len(texts), chunk_size):
            chunk_end = min(chunk_start + chunk_size, len(texts))
            chunk_texts = texts[chunk_start:chunk_end]

            current_chunk = chunk_start // chunk_size + 1
            rows_in_chunk = chunk_end - chunk_start

            logging.info(
                f"Processing chunk {current_chunk}/{total_chunks}\n"
                f"(rows {chunk_start}-{chunk_end-1}, {rows_in_chunk} rows)\n"
            )

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                future_to_index = {
                    executor.submit(safe_wrapper, text): chunk_start + idx
                    for idx, text in enumerate(chunk_texts)
                }
                for future in concurrent.futures.as_completed(future_to_index):
                    try:
                        idx = future_to_index[future]
                        results[idx] = future.result()
                        state = "Complete"
                    except Exception as e:
                        logging.error(
                            f"An error occurred while retrieving the results\n"
                            f"from futures: {e}"
                        )
                        results[idx] = error_value
                        state = "Failed"

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
                            state=state,
                            no_of_asset="1",
                            pipeline_mod=self.pipeline_mod,
                            file_path=str(image_paths[idx]),
                            event_name="KeyValuePairExtract",
                        ),
                    )

        df[new_col] = results
        return df

    @log_execution
    def deepseek_model_prompt_generation(extraction_config, input_text):
        """
        Build a DeepSeek prompt to extract configured key-value pairs from text.

        Args:
            extraction_config (dict): Extraction configuration keyed by field
                name. Each value may include: type, pattern, alternative_labels.
            input_text (str): Text corpus used as the extraction source.

        Returns:
            str: Fully formatted prompt to send to the DeepSeek model.
        """
        field_instructions = []

        for field_name, config in extraction_config.items():
            field_type = config["type"]
            pattern = config["pattern"]
            alternative_labels = config["alternative_labels"]
            instruction = f"- {field_name} ({field_type})"

            if pattern:
                instruction += f" - Use pattern: {pattern}"

            if alternative_labels:
                instruction += f" - Possible text labels: {alternative_labels}"

            field_instructions.append(instruction)

        fields_text = "\n".join(field_instructions)

        formatted_prompt = textwrap.dedent(
            f"""
            TEXT TO PROCESS:
            {input_text}

            EXTRACTION TASK:
            The text above contains a list of tuples. Each tuple holds two values: the text itself and the center point of its corresponding label's bounding box, extracted from a file.
            Your task is to extract the following key-value pairs from the text above. Follow these rules carefully:

            REQUIRED FIELDS:
            {fields_text}

            EXTRACTION RULES:
            1. Determine if the file is column-based or row-based by reviewing the key-value pairs that may exist.
            2. Only pair values that are spatially close to each other based on their coordinate positions. Reject matches where the keys and values are too far apart in the document layout.
            3. PRIORITY: Process regex-defined fields FIRST. Extract the first match before handling non-regex fields
            4. MANDATORY: Execute regex patterns rigorously
            5. The value for the field can also be found in the context text, using the 'possible text labels' if provided
            6. For fields containing names or full names, output the extracted value as it is, without changing the order
            7. For date fields, output dates in mm/dd/yyyy format only
            8. Extract only the first occurrence for each field. Values should not be repeated across fields
            9. Use null for any keys that are missing in the text
            10. After the main extraction, add confidence scores for each field as {{field_name}}_s
            11. Confidence scores (0-100) should reflect how clearly and unambiguously the value appears in the text

            OUTPUT REQUIREMENTS:
            - Output ONLY ONE valid raw JSON object without any markdown formatting, backticks, or code fences
            - Include all specified fields in the given order
            - Add confidence score keys for each field
            - No additional text or explanations

            EXAMPLE OUTPUT FORMAT:
            {{
                "field1": "extracted_value",
                "field1_s": 85,
                "another_field": "01/15/2023",
                "another_field_s": 90,
                "other_field": null,
                "other_field_s": 0
            }}

            Now extract the key-value pairs from the provided text.
            """
        ).strip()

        return formatted_prompt

    @log_execution
    def extract_reasoning_and_response(response: dict) -> str:
        """
        Extract the model reasoning and final response text from an LLM response.

        This method parses the structured response returned by the model and
        separates the internal reasoning content from the final textual output.
        Only the final response text is returned, while both components are
        logged for inspection and debugging purposes.

        Args:
            response (dict): Raw response dictionary returned by the LLM API.

        Returns:
            str: Extracted response text containing the model's final output.
        """
        reasoning, response_text = "", ""

        for item in response["output"]["message"]["content"]:
            for key, value in item.items():
                if key == "reasoningContent":
                    reasoning = value["reasoningText"]["text"]
                elif key == "text":
                    response_text = value

        logging.info(f"\nReasoning:\n{reasoning}")
        logging.info(f"\nResponse:\n{response_text}")

        return response_text

    @log_execution
    def extract_keys_from_text(text, extraction_config, model_id, max_tokens):
        """
        Extract specified keys using the 'extraction_config' from text using Bedrock API

        # https://docs.aws.amazon.com/bedrock/latest/userguide/service_code_examples.html
        # Documentation:
        # https://docs.aws.amazon.com/bedrock/latest/userguide/bedrock-runtime_example_bedrock
        # -runtime_InvokeModel_MetaLlama3_section.html
        # https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-deepseek.html

        Args:
            text (str): Input text to analyze
            extraction_config (dict): dict of keys to extract and details:
            {
                "field_name": {
                    "pattern": {regex pattern},
                    "type": {data type},
                    "alternative_labels": {list of alternative labels}
                },
                ...
            }

        Returns:
            dict: Extracted key-value pairs
        """

        config = Config(
            connect_timeout=30,
            read_timeout=30,
            retries={
                'max_attempts': 17,
                'mode': 'adaptive'
            }
        )

        bedrock_client = boto3.client('bedrock-runtime', config=config)
        formatted_prompt = WithConfigurationExtractor.deepseek_model_prompt_generation(
            extraction_config, text
        )
        logging.info(
            f"Showing only the first 700 chars out of the user_prompt: '{formatted_prompt}'\n\n"
        )
        conversation = [
            {
                "role": "user",
                "content": [
                    {"text": formatted_prompt}
                ],
            }
        ]

        try:
            response = bedrock_client.converse(
                modelId=model_id,
                messages=conversation,
                inferenceConfig={"maxTokens": 10000, "temperature": 0},
            )
            response_text = WithConfigurationExtractor.extract_reasoning_and_response(response)
            return response_text
        except Exception as e:
            logging.error(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")
            return {}

    @staticmethod
    @log_execution
    def safe_load_json(json_string):
        """
        Safely parse a JSON string by sanitizing known invalid token patterns.

        Args:
            json_string (str): JSON string to sanitize and parse.

        Returns:
            dict: Parsed JSON object.

        Raises:
            ValueError: If the JSON cannot be decoded after sanitization.
        """
        try:
            sanitized_json_string = re.sub(
                r"([:]=[*~\-\.\:\|\\\[\]])",
                r': "invalid_value"',
                json_string,
            )
            return json.loads(sanitized_json_string)
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON encountered: {json_string}")

    @staticmethod
    @log_execution
    def force_case_processing(
        json_data: dict,
        field_name: str,
        force_case: str,
        value: str
    ) -> dict:
        """
        Apply a case transformation rule to a specific field value.

        Args:
            json_data (dict): JSON dictionary containing extracted values.
            field_name (str): Name of the field to update.
            force_case (str): Case transformation rule to apply.
            value (str): Original field value.

        Returns:
            dict: Updated JSON dictionary with the transformed value.

        Raises:
            ValueError: If the ForceCase rule is not supported.
        """
        if force_case.upper() == "UPPER":
            json_data[field_name] = value.upper()
        elif force_case.upper() == "PROPER":
            json_data[field_name] = value.title()
        else:
            raise ValueError(f"Unsupported ForceCase value: {force_case}")
        return json_data

    @staticmethod
    @log_execution
    def validate_and_reformat_date(
        date_string,
        min_date,
        max_date,
        target_format="%m/%d/%Y"
    ):
        """
        Parse and normalize a date string into a target format.

        Args:
            date_string (str): Input date string to parse and normalize.
            min_date (str): Minimum allowed date in mm/dd/yyyy format.
            max_date (str): Maximum allowed date in mm/dd/yyyy format or 'today'.
            target_format (str): Output date format.

        Returns:
            str: Reformatted date string if parsing succeeds, otherwise the
                original date_string.
        """
        supported_date_formats = [
            "%m/%d/%Y",  # MM/DD/YYYY
            "%m/%d/%y",  # MM/DD/YY
            "%m-%d-%Y",  # MM-DD-YYYY
            "%m-%d-%y",  # MM-DD-YY
            "%d-%m-%Y",  # DD-MM-YYYY
            "%Y.%m.%d",  # YYYY.MM.DD
            "%d %B %Y",  # DD Month YYYY
            "%d, %b %Y",  # DD, AbbreviatedMonth YYYY
            "%Y-%m-%d",  # ISO 8601 format YYYY-MM-DD
            "%d/%m/%y",  # DD/MM/YY
            "%d-%b-%y",  # DD-AbbreviatedMonth-YY
            "%b %d, %Y",  # Month Abbreviation DD, YYYY
        ]

        current_date = datetime.now()
        current_year = current_date.year
        is_two_digit_year = bool(re.search(r"[-/]\d{2}$", date_string))

        if max_date and max_date.lower() == "today":
            max_date = current_date.strftime("%m/%d/%Y")
            logging.info(f"Using 'Today' as MaxDate: {max_date}")

        min_date_obj = (
            datetime.strptime(min_date, "%m/%d/%Y") if min_date else None
        )
        max_date_obj = (
            datetime.strptime(max_date, "%m/%d/%Y") if max_date else None
        )

        for fmt in supported_date_formats:
            try:
                parsed_date = datetime.strptime(date_string, fmt)
                logging.info(f"Parsed date: {parsed_date}, Year: {parsed_date.year}")

                if is_two_digit_year and parsed_date.year > current_year:
                    assumed_date = parsed_date.replace(year=parsed_date.year)
                    logging.info(f"Assumed date in current century: {assumed_date}")

                    if max_date_obj and assumed_date > max_date_obj:
                        corrected_date = parsed_date.replace(
                            year=parsed_date.year - 100
                        )
                        logging.info(f"Corrected to past century: {corrected_date}")
                        parsed_date = corrected_date
                    else:
                        parsed_date = assumed_date
                        logging.info(f"Kept assumed date: {assumed_date}")

                return parsed_date.strftime(target_format)
            except ValueError:
                continue

        return date_string

    @staticmethod
    @log_execution
    def process_fields(fields, json_data):
        """
        Apply post-processing rules to extracted key-value JSON output.

        This method enforces configuration rules on extracted values, including:
        - Parsing JSON strings into dictionaries when needed.
        - Applying ForceCase transformations (UPPER, PROPER).
        - Normalizing date fields according to MinDate/MaxDate constraints.

        Args:
            fields (list[dict]): Field configuration list containing FieldName,
                Type, Validation, and ForceCase rules.
            json_data (dict | str): Extracted key-value data as a dictionary or
                JSON string.

        Returns:
            dict: Updated JSON dictionary with rules applied.

        Raises:
            TypeError: If json_data cannot be parsed into a dictionary.
            ValueError: If invalid JSON is encountered during parsing.
        """
        if isinstance(json_data, str):
            json_data = WithConfigurationExtractor.safe_load_json(json_data)

        if not isinstance(json_data, dict):
            raise TypeError(f"Expected dictionary but got {type(json_data)}")

        if "error" in json_data.keys():
            return json_data

        for field in fields:
            field_name = field.get("FieldName")
            field_type = field.get("Type")
            validation = field.get("Validation", {})
            force_case = field.get("ForceCase", "AS-IS")
            value = json_data.get(field_name)

            if value is None:
                continue

            if force_case != "AS-IS" and isinstance(value, str):
                json_data = WithConfigurationExtractor.force_case_processing(
                    json_data,
                    field_name,
                    force_case,
                    value,
                )

            if field_type == "date" and value:
                min_date = validation.get("MinDate")
                max_date = validation.get("MaxDate")
                reformatted_date = (
                    WithConfigurationExtractor.validate_and_reformat_date(
                        value,
                        min_date=min_date,
                        max_date=max_date,
                    )
                )
                if reformatted_date:
                    json_data[field_name] = reformatted_date

        return json_data

    @staticmethod
    @log_execution
    def remove_chars_from_document(document_content: dict, remove_conf: dict) -> dict:
        """
        Remove configured characters or patterns from document fields.

        This method applies regex-based substitutions for each configured
        field, logging the original and updated values, then serializes the
        updated document back into JSON.

        Args:
            document_content (dict): Parsed JSON document data containing
                fields to sanitize.
            remove_conf (dict): Mapping of field names to regex patterns
                to remove from their values.

        Returns:
            dict: The updated document with removed characters.
        """
        if "error" in document_content.keys():
            return document_content

        for field_name in remove_conf.keys():
            logging.info(f"Removing chars {remove_conf[field_name]} from {field_name}")
            original_value = document_content[field_name]
            new_value = (
                re.sub(fr"{remove_conf[field_name]}", "", original_value)
                if original_value
                else original_value
            )
            document_content[field_name] = new_value
            logging.info(f"- Original value: {original_value}, new value: {new_value}")
        return document_content

    @staticmethod
    @log_execution
    def remove_chars(df: pd.DataFrame, collection_fields_rules: list[dict]) -> pd.DataFrame:
        """
        Apply configured character removal to key-value pairs in a DataFrame.

        This method builds a field-to-regex mapping from the collection
        configuration, applies `remove_chars_from_document` to each row's
        key-value JSON, and returns the updated DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing a 'key_value_pairs' column
                with JSON strings to sanitize.
            collection_fields_rules (list[dict]): Field configuration list that
                may include "RemoveChars" definitions per field.

        Returns:
            pd.DataFrame: DataFrame with sanitized 'key_value_pairs' values when
                removal rules are present.
        """
        structured_field_rules = {
            field_conf["FieldName"]: field_conf["RemoveChars"]
            for field_conf in collection_fields_rules
            if "RemoveChars" in field_conf.keys()
        }
        if structured_field_rules:
            df["key_value_pairs"] = df["key_value_pairs"].apply(
                lambda x: WithConfigurationExtractor.remove_chars_from_document(
                    x,
                    structured_field_rules
                )
            )
        return df

    @log_execution
    def process_key_value_pairs(
        self,
        process_df: pd.DataFrame,
        field_list: list[dict]
    ) -> pd.DataFrame:
        """
        Apply field-level post-processing to extracted key-value pairs.

        This method processes the extracted key-value JSON data by enforcing
        validation rules, case transformations, date normalization, and
        configured character removal. The resulting dictionaries are
        serialized back into formatted JSON strings.

        Args:
            process_df (pd.DataFrame): DataFrame containing a 'key_value_pairs'
                column with extracted data as dictionaries or JSON strings.
            field_list (list[dict]): Field configuration list defining validation
                and transformation rules.

        Returns:
            pd.DataFrame: Updated DataFrame with processed and serialized
                key-value pairs.
        """
        process_df["key_value_pairs"] = process_df["key_value_pairs"].apply(
            lambda row: WithConfigurationExtractor.process_fields(
                field_list,
                row,
            )
        )
        process_df = WithConfigurationExtractor.remove_chars(process_df, field_list)
        process_df["key_value_pairs"] = process_df["key_value_pairs"].apply(
            lambda row: json.dumps(row, indent=4)
            if isinstance(row, dict)
            else row
        )
        logging.info(
            f"Showing the first 10 rows out of the processed dataframe:\n"
            f"{process_df.head(10)}"
        )
        return process_df

    @log_execution
    def pd_to_spark(
        self,
        pd_df: pd.DataFrame
    ) -> DataFrame:
        """
        Convert a Pandas DataFrame into a Spark DataFrame with string schema.

        Args:
            pd_df (pd.DataFrame): Input Pandas DataFrame to convert.

        Returns:
            DataFrame: Spark DataFrame with all columns cast as strings.
        """
        all_columns = pd_df.columns.tolist()

        logging.info("Creating the Spark Dataframe out from the Pandas Dataframe...")

        schema = StructType(
            [
                StructField(
                    column_name,
                    StringType(),
                    nullable=True,
                )
                for column_name in all_columns
            ]
        )

        final_spark_df = self.spark.createDataFrame(
            pd_df,
            schema=schema,
        )

        logging.info("Spark Dataframe created.")

        return final_spark_df

    @staticmethod
    @log_execution
    def get_glue_table_schema(
        glue_client: BaseClient,
        glue_catalog: GlueCatalog
    ) -> list:
        """
        Retrieve the ordered column schema for a Glue catalog table.

        Args:
            glue_client (BaseClient): Boto3 Glue client instance.
            glue_catalog (GlueCatalog): Glue catalog database and table info.

        Returns:
            list: Ordered list of column names from the Glue table schema.
        """
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
            exit("ConnectCrawl - Critical error")

    @staticmethod
    @log_execution
    def validate_and_prepare_dataframe(
        result_df: DataFrame,
        expected_columns: list
    ) -> DataFrame:
        """
        Align a Spark DataFrame with an expected column schema.

        This method compares the input DataFrame schema with the expected
        schema, adds any missing columns as null string columns, and returns
        a DataFrame containing only the expected columns in the correct order.

        Args:
            result_df (DataFrame): Input Spark DataFrame to validate.
            expected_columns (list): Ordered list of required column names.

        Returns:
            DataFrame: Spark DataFrame aligned to the expected schema.
        """
        set_expected_cols = set(expected_columns)
        set_input_cols = set(result_df.columns)

        logging.info(
            f"set_expected_cols:\n{set_expected_cols}\n"
            f"set_input_cols:\n{set_input_cols}"
        )

        missing_columns = set_expected_cols - set_input_cols
        present_columns = set_expected_cols & set_input_cols
        extra_columns = set_input_cols - set_expected_cols

        logging.info(
            f"missing_columns:\n{missing_columns}\n"
            f"present_columns:\n{present_columns}\n"
            f"extra_columns:\n{extra_columns}\n"
        )

        if missing_columns:
            logging.warning(f"Missing columns in input DataFrame:\n{missing_columns}")

            for col in missing_columns:
                result_df = result_df.withColumn(
                    col,
                    F.lit("null").cast(StringType()),
                )

        else:
            logging.info(
                "There are not missing columns in the input dataframe with respect\n"
                "to the required or expected columns."
            )

        return result_df.select(*expected_columns)

    @log_execution
    def write_to_glue_catalog(
        self,
        result_df: DataFrame,
        glue_catalog: GlueCatalog
    ) -> None:
        """
        Validate, align, and write a Spark DataFrame to a Glue catalog table.

        This method retrieves the target table schema from the Glue catalog,
        ensures the input DataFrame matches the expected columns and order,
        and writes the data in Parquet format using overwrite mode.

        Args:
            result_df (DataFrame): Spark DataFrame containing the processed
                key-value extraction results.
            glue_catalog (GlueCatalog): Glue catalog database and table
                destination for the write operation.

        Returns:
            None
        """
        target_db = glue_catalog.catalog_db
        target_table_name = glue_catalog.catalog_table

        logging.info(f"Writing to Glue: '{target_db}.{target_table_name}'")

        expected_columns = WithConfigurationExtractor.get_glue_table_schema(
            self.glue_client,
            glue_catalog,
        )
        prepared_df = WithConfigurationExtractor.validate_and_prepare_dataframe(
            result_df,
            expected_columns,
        )

        logging.info(
            f"Writing '{prepared_df.count()}' records to '{target_db}.{target_table_name}'"
        )
        logging.info(f"Final columns are:\n{prepared_df.columns}")

        (
            prepared_df.coalesce(1)
            .write.mode("overwrite")
            .format("parquet")
            .insertInto(f"`{target_db}`.`{target_table_name}`")
        )

        logging.info("Successfully wrote to Glue catalog")

    @log_execution
    def extract_text(self):
        """
        Execute configuration-driven key-value extraction and persist results.

        This method orchestrates the end-to-end enrichment workflow:
        - Loads collection configuration and derives the field schema.
        - Reads OCR liquid text rows for the current batch and transaction.
        - Builds extraction configuration and estimates max response tokens.
        - Extracts key-value pairs using the configured LLM with safe execution.
        - Post-processes extracted values (dates and force-case rules).
        - Writes the enriched dataset back to the Glue catalog.
        - Sends Timestream events for job completion or failure.

        Args:
            None

        Returns:
            None

        Raises:
            RuntimeError: If the extraction process fails.
            ValueError: If the input dataset is empty or invalid.
        """
        try:
            field_list = ConfigurationTemplate.get_field_list(self.collection_config)
            logging.info(f"Project's Field Schema is: {field_list}")
            process_df = self.get_process_table_from_catalog(
                self.enrichment_catalog_data
            )
            pd_df = self.get_pd_df_and_display_info(process_df)

            logging.info("\nThe process execution timer has started...\n")
            total_start_time = time.time()

            field_list = WithConfigurationExtractor.format_validation_date_rules(
                field_list
            )
            extraction_config = (
                WithConfigurationExtractor.preprocess_validation_template(
                    field_list
                )
            )
            llm_helper = PatternExtractorHelper(extraction_config)
            keys_to_extract = WithConfigurationExtractor.get_the_keys_to_extract(
                field_list
            )
            max_tokens = WithConfigurationExtractor.calculate_max_tokens(
                keys_to_extract
            )
            process_df = self.safe_key_value_extraction(
                pd_df,
                "ocr_spatial_info",
                "key_value_pairs",
                WithConfigurationExtractor.extract_keys_from_text,
                extraction_config,
                llm_helper,
                self.model_id,
                max_tokens,
                error_value={"error":"Could Not be Processed"},
                max_workers=10,
                chunk_size=50,
            )

            total_time = time.time() - total_start_time
            logging.info(
                f"The Key-Value extractor function was applied to all \n"
                f"targeted corpus documents...\n"
                f"Total process time for all corpus document items is: '{total_time:.4f}' seconds"
            )

            process_df = self.process_key_value_pairs(process_df, field_list)
            result_df = self.pd_to_spark(process_df)
            self.write_to_glue_catalog(result_df, self.enrichment_catalog_data)
            final_record_count = result_df.count()

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
                    no_of_asset=str(final_record_count),
                ),
            )
            logging.info("Job completed successfully")

        except Exception as e:
            logging.error(f"Critical error in the 'main' function:\n{str(e)}")
            logging.error(f"Error type: {type(e).__name__}")

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

            raise RuntimeError("General process failed") from e


class WithoutConfigurationExtractor(TextExtractor):
    def extract_text(self):
        logging.warning("Skipping Extract Module execution")
        return


class FactoryTextExtractor:
    def get_collection_config(
        glue_catalog: GlueCatalog,
        workflow_vars: WorkflowParams,
        spark: SparkSession,
        transaction_id: str,
        batch_name: str,
    ) -> tuple[str, dict]:
        """
        Retrieve the collection configuration for the current batch transaction.

        This method resolves the customer and collection configuration by
        querying the configuration data stored in the Glue catalog and S3,
        using the current transaction and batch context.

        Args:
            glue_catalog (GlueCatalog): Glue catalog database and table containing
                collection configuration metadata.

        Returns:
            tuple[str, dict]: TR account name and the corresponding collection
                configuration dictionary.
        """
        try:
            location = S3Location(
                workflow_vars.incoming_bucket,
                "Onb_Customer_Conf/",
                "",
            )
            transaction = Transaction(transaction_id, batch_name)
            tr_account_name, collection_config = (
                ConfigurationTemplate.get_collection_config_from_catalog(
                    location,
                    glue_catalog,
                    transaction,
                    spark,
                    logging,
                )
            )
            logging.info(f"Collection Configuration is: {collection_config}")
            return tr_account_name, collection_config, True
        except (CollectionNotFound, ConfigurationNotFound):
            return "", {}, False

    @staticmethod
    @log_execution
    def get_text_extractor(
        source: str,
        batch_name: str,
        transaction_id: str,
        pipeline_mod: str,
        model_id: str,
        job_vars: JobVariables,
        workflow_vars: WorkflowParams,
        incoming_catalog_data: GlueCatalog,
        enrichment_catalog_data: GlueCatalog,
        spark: SparkSession,
        glue_client: BaseClient,
    ) -> TextExtractor:
        _, collection_config, is_config_present = FactoryTextExtractor.get_collection_config(
            incoming_catalog_data,
            workflow_vars,
            spark,
            transaction_id,
            batch_name,
        )
        if "radix-onboarding-indexed" in source or ("tr-image-extract" in source and is_config_present):
            return WithConfigurationExtractor(
                collection_config=collection_config,
                batch_name=batch_name,
                transaction_id=transaction_id,
                pipeline_mod=pipeline_mod,
                model_id=model_id,
                job_vars=job_vars,
                workflow_vars=workflow_vars,
                incoming_catalog_data=incoming_catalog_data,
                enrichment_catalog_data=enrichment_catalog_data,
                spark=spark,
                glue_client=glue_client,
            )
        elif "tr-image-extract" in source and not is_config_present:
            return WithoutConfigurationExtractor()
        else:
            raise ValueError(f"Unsupported source type: {source}")


class GlueJobExecution(GlueJobBase):
    def __init__(self, pipeline_mod: str, model_id: str):
        """
        Initialize the Glue job execution for text extraction and enrichment.

        Args:
            pipeline_mod (str): Name of the pipeline module executing the job.
            model_id (str): Identifier of the model used for text extraction.

        Returns:
            None
        """
        super().__init__(pipeline_mod)
        self.model_id = model_id
        self.spark.conf.set(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic",
        )

    def main(self):
        """
        Execute the Glue job to extract and enrich text data.

        This method:
        - Reads job and workflow parameters.
        - Emits Timestream events for job state transitions.
        - Selects the appropriate text extractor using a factory.
        - Executes the text extraction and enrichment logic.
        - Handles failures and reports them to Timestream.

        Args:
            None

        Returns:
            None

        Raises:
            RuntimeError: If the job fails during execution.
        """
        try:
            args = getResolvedOptions(
                sys.argv,
                [
                    "JOB_NAME",
                    "WORKFLOW_NAME",
                    "WORKFLOW_RUN_ID",
                    "CatalogDB",
                    "IncomingCatalogTable",
                    "EnrichmentCatalogTable",
                    "TimestreamDB",
                    "TimestreamTable",
                    "TimestreamSQSQueue",
                ],
            )

            job_vars = JobVariables(args)

            self.job.init(job_vars.job_name, args)

            self.log.info(f"Job Variables:\n{job_vars}")

            workflow_params = self.get_workflow_params(job_vars)

            customer_name = workflow_params.batch_name.split("-")[0]
            self.log.info(f"customer_name: '{customer_name}'")

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

            extractor = FactoryTextExtractor.get_text_extractor(
                source=workflow_params.zip_key,
                batch_name=workflow_params.batch_name,
                transaction_id=workflow_params.transaction_id,
                pipeline_mod=self.pipeline_mod,
                model_id=self.model_id,
                job_vars=job_vars,
                workflow_vars=workflow_params,
                incoming_catalog_data=incoming_catalog_data,
                enrichment_catalog_data=enrichment_catalog_data,
                spark=self.spark,
                glue_client=self.glue_client,
            )
            extractor.extract_text()
            self.log.info("Job completed successfully")
        except Exception as e:
            self.log.error(f"Critical error in the 'main' function:\n{str(e)}")
            self.log.error(f"Error type: {type(e).__name__}")
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
                    pipeline_mod=self.pipeline_mod,
                    state="Failed",
                    no_of_asset="-",
                ),
            )
            raise RuntimeError("General process failed") from e
        finally:
            self.job.commit()


if __name__ == "__main__":
    pipeline_mod = 'ExtractText'
    model_id = "us.deepseek.r1-v1:0"
    glue_job_execution = GlueJobExecution(pipeline_mod, model_id)
    glue_job_execution.main()
