import json
import logging
from typing import Dict

import boto3
from botocore.utils import ClientError

from pyspark.sql import DataFrame, SparkSession


class S3Location:
    """The S3 location of a file.

    Attributes:
        bucket (str): The source bucket.
        prefix (str): The prefix, inside the `bucket`, where the `key` is.
        key (str): The file key.
    """

    key: str = ""

    def __init__(self, bucket: str, prefix: str, key: str = ""):
        """Initialize the S3Location class.

        Args:
            bucket (str): The source bucket.
            prefix (str): The prefix, inside the `bucket`, where the `key` is.
            key (str): The file key.
        """
        if prefix[-1] != "/":
            prefix += "/"
        self.bucket = bucket
        self.prefix = prefix
        self.key = key

    def set_key(self, key: str):
        """Sets the key."""
        self.key = key


class GlueCatalog:
    """The catalog DB and catalog table to form the fully-qualified name.

    Attributes:
        catalog_db (str): The name of the catalog DB, where the `catalog_table` is defined.
        catalog_table (str): The name of the catalog table to query.
    """

    def __init__(self, catalog_db: str, catalog_table: str):
        """Initialize the Glue catalog, setting the `catalog_db` and `catalog_table`

        Args:
            catalog_db (str): The name of the catalog DB, where the `catalog_table` is defined.
            catalog_table (str): The name of the catalog table to query.
        """
        self.catalog_db = catalog_db
        self.catalog_table = catalog_table


class Transaction:
    """The definition of the workflow transaction.

    Attributes:
        transaction_id (str): The UUID V4, usually set in the resource manager lambda.
        batch_name (str): The batch name of the images that are being processed.
    """

    def __init__(self, transaction_id: str, batch_name: str):
        """Initialize the transaction object.

        Args:
            transaction_id (str): The UUID V4, usually set in the resource manager lambda.
            batch_name (str): The batch name of the images that are being processed.
        """
        self.transaction_id = transaction_id
        self.batch_name = batch_name


class ConfigurationNotFound(Exception):
    """When trying to find the configuration in S3"""

    pass


class CollectionNotFound(Exception):
    """When trying to find the collection inside the configuration template object"""

    pass


class GlueCatalogException(Exception):
    """To handle general exceptions when calling the boto3 Glue Catalog API, or the Spark SQL API."""

    pass


class ConfigurationTemplate:
    """Operations to find/act-upon the configuration template that belongs to a Total Recall Account Name and
    collection (project).

    The purpose of this class, is to define class methods to serve an API for the pipelines that belongs to the
    Digital Enablement layer.

    :param location (S3Location): The bucket, prefix and key (optional), to find the configuration template, for a
                            transaction and collection (project).
    :param glue_catalog_obj (GlueCatalog): The catalog DB and catalog table to form the fully-qualified name to find
                                    the batch metadata.
    :param transaction (Transaction): The tranasction definition for the current workflow run.
    :param spark (SparkSession): The Spark session that is being used by the calling context.
    :param logger (Logger): The logging instance that is being used by the calling context.

    :returns tr_account_name, collection_obj (tuple[str, Dict]):
        1. Total Recall account name that has the configuration template we are looking for.
            This is the name of the file in S3 that has the configuration template defined.
        2. The collection object is the configuration inside the collecion (project) key, inside the configuration
            template for the `tr_account_name`.

    """

    logger: logging.Logger

    @classmethod
    def get_collection_config_from_catalog(
        cls,
        location: S3Location,
        glue_catalog_obj: GlueCatalog,
        transaction: Transaction,
        spark: SparkSession,
        logger: logging.Logger,
    ) -> tuple[str, Dict]:
        """
        :param location (S3Location): The bucket, prefix and key (optional), to find the configuration template, for a
                                transaction and collection (project).
        :param glue_catalog_obj (GlueCatalog): The catalog DB and catalog table to form the fully-qualified name to find
                                        the batch metadata.
        :param transaction (Transaction): The tranasction definition for the current workflow run.
        :param spark (SparkSession): The Spark session that is being used by the calling context.
        :param logger (Logger): The logging instance that is being used by the calling context.

        :returns tr_account_name, collection_obj (tuple[str, Dict]):
            1. Total Recall account name that has the configuration template we are looking for.
                This is the name of the file in S3 that has the configuration template defined.
            2. The collection object is the configuration inside the collecion (project) key, inside the configuration
                template for the `tr_account_name`.

        :raises boto3.ClientError: Error when trying to find the configuration template in S3.
        :raises GlueCatalogException: Exception on processing the Glue table fully-qualified name.
                                      Could be a Glue client or Spark exceptions/errors.
        :raises CollectionNotFound: The collection (project) is not a key inside the configuration template file.
        """

        cls.logger = logger
        df_catalog: DataFrame = cls.get_catalog_data_from_batchname_transaction(spark, glue_catalog_obj, transaction)
        df_catalog.cache()
        cls.logger.info(f"Dataframe cached for {glue_catalog_obj.catalog_table} table")

        tr_row = df_catalog.select("collection").first()
        tr_account_name: str = tr_row[0] if tr_row else ""
        cls.logger.info(f"TR account name is -> {tr_account_name}")

        location.set_key(tr_account_name)

        configuration_object: Dict = {}
        try:
            configuration_object = cls.get_tr_account_config_file(location)
            cls.logger.info(f"Customer Configuration is: {configuration_object}")
        except ClientError as e:
            cls.logger.error(f"ClientError occurred: {e}")
            cls.logger.error(f"Customer Configuration not accessible for: {tr_account_name}")
            raise

        collection_name = df_catalog.select("project").first()
        collection_name = collection_name[0] if collection_name else ""
        cls.logger.info(f"Collection name (project) is -> {collection_name}")

        collection_obj = cls.get_collection_config(configuration_object, collection_name)
        if collection_obj:
            collection_obj["tr_customer_id"] = configuration_object.get("tr_customer_id", '-')
            cls.logger.info(f"Collection Configuration is: {collection_obj}")

        box_barcode = df_catalog.select("box_barcode").first()
        box_barcode = box_barcode[0] if box_barcode else ""
        cls.logger.info(f"Box Barcode is -> {box_barcode}")

        collection_obj.update({"batch_details":{
            "box_barcode": box_barcode,
            "collection": collection_name,
            "tr_customer_id": configuration_object.get("tr_customer_id", ""),
            "tr_account_name": tr_account_name
        }})

        cls.logger.info(f"Updated Collection Config is: {collection_obj}")

        return tr_account_name, collection_obj

    @classmethod
    def get_catalog_data_from_batchname_transaction(
        cls,
        spark: SparkSession,
        glue_catalog_obj: GlueCatalog,
        transaction: Transaction,
    ):
        """Query the catalog table where the batch_name and trannsaction_id are given.

        :param spark (SparkSession): The Spark session the calling context created.
        :param glue_catalog_obj (GlueCatalog): The fully-qualified name of the catalog table to query.
        :param transaction (Transaction): Workflow transaction (transaction_id and batch_name).

        :returns spark.DataFrame representing the result of the query.

        :raises GlueCatalogException: An error occurred inside the Glue API or Spark API.
        """
        log: logging.Logger = cls.logger
        try:
            query = f"""
            SELECT
                *
            FROM
                `{glue_catalog_obj.catalog_db}`.`{glue_catalog_obj.catalog_table}` AS __catalog_table
            WHERE
                __catalog_table.batch_name = '{transaction.batch_name}' and
                __catalog_table.transaction_id = '{transaction.transaction_id}'
            """

            log.info(f"The query to be executed is:\n\n{query}\n\n")
            glue_table_df = spark.sql(query)

            log.info("Success! Data was read from Glue Catalog.")

            glue_table_df.withColumnRenamed("image_path", "image_path_scanner").withColumnRenamed(
                "transaction_id", "transaction_id_scanner"
            )

            return glue_table_df
        except Exception as e:
            log.info(
                f"Critical error in 'get_catalog_data_from_batchname_transaction' function -> table is `{glue_catalog_obj.catalog_db}.{glue_catalog_obj.catalog_table}`\n{str(e)}"
            )
            raise GlueCatalogException(
                f"Critical error in 'get_catalog_data_from_batchname_transaction' function -> table is `{glue_catalog_obj.catalog_db}.{glue_catalog_obj.catalog_table}`\n{str(e)}"
            )

    @classmethod
    def get_tr_account_config_file(cls, location: S3Location):
        """Fetch the Total-Recall-account-name's configuration file in S3.

        :param location (S3Location): The bucket, prefix and key where the configuration file is in S3.

        :returns JSON object (Dict) having the content of the configuration file.

        :raises ConfigurationNotFound, having a ClientError(NoSuchKey) exception under the propagation, signaling the file does not
                exist.
        """
        try:
            s3_client = boto3.client("s3")
            file_key = f"{location.prefix}{location.key}.json"
            response = s3_client.get_object(Bucket=location.bucket, Key=file_key)
            json_content = json.loads(response["Body"].read().decode("utf-8"))
            return json_content
        except ClientError as e:
            cls.logger.error(f"There was an error while reading the configuration template file -> {e}")
            raise ConfigurationNotFound(f"{e}")

    @classmethod
    def get_collection_config(cls, configuration_object: Dict, collection_name: str) -> Dict:
        """Get the collection (project) object, inside the configuration template for the Total Recall Account name.

        :param configuration_object (Dict): The configuration template for the Total Recall Account name.
        :param collection_name (str): Collection (project) name representing the key of the object to retrieve.

        :returns the collection object (Dict) found

        :raises CollectionNotFound if the collection is not a key in the file.
        """
        for collection_dict in configuration_object["collections"]:
            if collection_dict["collection"] == collection_name:
                return collection_dict
        raise CollectionNotFound(f"CollectionNotFound -> {collection_name}")

    @classmethod
    def get_audit_flag(cls, collection_object: Dict) -> bool:
        """Is the audit flag active?

        :param collection_object (Dict): The collection (project) configuration object.

        :returns True if the audit flag is active, False otherwise.
        """
        if "send_all_to_opsiq" not in collection_object:
            cls.logger.warning("No `send_all_to_opsiq` in collection config. Returning `False` as default")
        return collection_object.get("send_all_to_opsiq", False)

    @classmethod
    def get_field_list(cls, collection_object: Dict) -> Dict:
        """Fetch the `field_list` key in the collection (project) configuration object.

        :param collection_object (Dict): The collection (project) configuration object.

        :returns a Dict if the key was found, or an empty Dict otherwise.
        """
        if "field_list" not in collection_object:
            cls.logger.warning("No `field_list` in collection config. Returning `{}` as default")
        return collection_object.get("field_list", {})
