import json
import logging
import re
import awswrangler as wr
import boto3
import botocore
import pandas as pd
import pendulum
from io import BytesIO
from typing import Dict, List, Tuple
from .batch_ingest_config import BatchIngestConfig
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow_custom_operator.operators.objects import SourceS3Config
from airflow_custom_operator.operators.ingress_data_operators import (
    S3GlueLoadPartitionsOperator,
)

LOAD_DATETIME = pendulum.now("UTC")

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BatchIngestor:
    """
     The BatchIngestor class is wrapper class that abstracts the logic behind batch ingestion for partner folders. It takes configurations
     from the BatchIngestConfig class, and it handles the entire batch ingestion operation allowing for single folder to single table
     ingestion and multi-folder to multi-table ingestion.

     What it does:
        • Load data from parquet files in landing buckets
        • Reads each file, adds `load_time` + `year/month/day/hour` columns.
        • Uses `glue_tables` to pick the target schema, casts dtypes, and writes
          partitioned Parquet to the raw bucket.
        • Calls `S3GlueLoadPartitionsOperator` to add the new partitions in Glue.
        • Logs errors per file; only fails if the entire folder fails.

     How to use:

        @task(task_id="batch_ingestion")
        def batch_ingestion(folder_1: str, **ctx):
             config = BatchIngestConfig(
                source_bucket="landing_bucket",
                dest_bucket="raw_bucket",
                aws_region="af-region",
                s3_prefix="partner_1/",                     # e.g "partner_name/"
                source_folders=["folder_1", "folder_2"],    # one folder or many
                glue_tables=GLUE_TABLES,                    # {'folder': {'schema': …}}
                dag_run=ctx.get("dag_run"),
                data_interval_end=ctx.get("data_interval_end"),
                schema_type="avro"                          # Avro or JSON
            )

        BatchIngestor(config).run()

     Usage in your DAG:
         This class can be used in your DAG to create individual tasks for each folder that is
         processed during the batch ingestion for a more detailed DAG graph

        for folder in GLUE_TABLES:
            batch_task = batch_ingestion.override(task_id=f"process_{folder}")(folder)
            batch_task >> complete
     """
    def __init__(self, config: BatchIngestConfig):
        self.config = config
        self._s3_hook = S3Hook()
        self._glue = boto3.client("glue")

    @staticmethod
    def load_data_from_parquet(bucket_name: str, s3_key: str) -> pd.DataFrame:
        """
        Load data from S3 parquet file into pandas DataFrame.

        This function uses AWS Wrangler to directly read parquet files from S3
        without downloading them locally first.

        Args:
            bucket_name (str): S3 bucket name containing the source data
            s3_key (str): S3 key (path) of the file to load

        Returns:
            pd.DataFrame: DataFrame containing the loaded data

        Raises:
            Exception: If there's an error loading the data
        """
        s3_uri = f"s3://{bucket_name}/{s3_key}"
        try:
            data = wr.s3.read_parquet(s3_uri, use_threads=True)
            logger.info("Loaded %s records from %s", len(data), s3_key)
            return data
        except Exception as ex:
            logger.error("Error loading data from %s: %s", s3_key, ex)
            raise

    def load_data_from_txt(self, bucket_name: str, s3_key: str) -> pd.DataFrame:
        """
        Load data from S3 txt file into pandas DataFrame.

        Args:
            bucket_name (str): S3 bucket name containing the source data
            s3_key (str): S3 key (path) of the file to load

        Returns:
            pd.DataFrame: DataFrame containing the loaded data

        Raises:
            Exception: If there's an error loading the data
        """
        try:
            obj = self._s3_hook.get_key(s3_key, bucket_name=bucket_name)
            body = obj.get()["Body"].read()

            data_buffer = BytesIO()
            meta_buffer = BytesIO()

            for line in body.splitlines():
                if line.startswith(b"H") or line.startswith(b"D"):
                    data_buffer.write(line + b"\n")
                elif line.startswith(b"T"):
                    meta_buffer.write(line + b"\n")

            data_buffer.seek(0)
            meta_buffer.seek(0)

            data_df = pd.read_csv(
                data_buffer,
                delimiter="~",
                header=0,
                dtype={},
                parse_dates=[]
            )

            data_df.columns = [col.lower() for col in data_df.columns]
            logger.info("Loaded %s records from %s", len(data_df), s3_key)

            return data_df

        except Exception as ex:
            logger.error("Error loading data from %s: %s", s3_key, ex)
            raise

    @staticmethod
    def map_avro_schema_types_to_pandas(fields) -> dict:
        """
        Map Avro/Glue schema field types to pandas/awswrangler compatible data types.

        This function handles different schema field type formats:
        - Simple string types: "string", "int", etc.
        - Complex dictionary types: {"type": "string", "logicalType": "date"}
        - Union types: ["null", "string"]

        Args:
            fields (list): List of field definitions from schema

        Returns:
            dict: Mapping of field names to pandas data types
        """
        dtypes = {}
        for field in fields:
            field_name = field["name"]
            field_type = field["type"]
            if isinstance(field_type, dict):
                base_type = (
                    field_type["type"]
                    if isinstance(field_type["type"], str)
                    else field_type["type"][1]
                )
                logical_type = field_type.get("logicalType")
                if base_type == "string" and logical_type in [
                    "date",
                    "timestamp-millis",
                    "timestamp-micros",
                    "date-time",
                ]:
                    dtypes[field_name] = "string"
                elif base_type == "int":
                    dtypes[field_name] = "int"
                elif base_type == "long":
                    dtypes[field_name] = "bigint"
                elif base_type == "float":
                    dtypes[field_name] = "float"
                elif base_type == "double":
                    dtypes[field_name] = "double"
                else:
                    dtypes[field_name] = "string"
            elif isinstance(field_type, str):
                if field_type == "string":
                    dtypes[field_name] = "string"
                elif field_type == "int":
                    dtypes[field_name] = "int"
                elif field_type == "long":
                    dtypes[field_name] = "bigint"
                elif field_type == "float":
                    dtypes[field_name] = "float"
                elif field_type == "double":
                    dtypes[field_name] = "double"
                else:
                    dtypes[field_name] = "string"
            elif isinstance(field_type, list):
                non_null_types = [t for t in field_type if t != "null"]
                if len(non_null_types) == 1:
                    non_null_type = non_null_types[0]
                    if non_null_type == "string":
                        dtypes[field_name] = "string"
                    elif non_null_type == "int":
                        dtypes[field_name] = "int"
                    elif non_null_type == "long":
                        dtypes[field_name] = "bigint"
                    elif non_null_type == "float":
                        dtypes[field_name] = "float"
                    elif non_null_type == "double":
                        dtypes[field_name] = "double"
                    else:
                        dtypes[field_name] = "string"
                else:
                    dtypes[field_name] = "string"
        return dtypes

    def get_schema_from_registry(
            self,
            registry_name: str,
            schema_name: str
    ) -> Tuple[Dict[str, str], List[str]]:
        """
        Fetch JSON schema from Glue Schema Registry

        Args:
            registry_name (str): Name of the Glue Schema Registry
            schema_name (str): Name of the schema to fetch

        Returns:
            Tuple[Dict[str, str], List[str]]:
                pandas_dtypes: Mapping of column names to Glue‐compatible read dtypes
                parse_dates: List of column names that should be parsed as datetimes
        """
        try:
            resp = self._glue.get_schema_version(
                SchemaId={"RegistryName": registry_name, "SchemaName": schema_name},
                SchemaVersionNumber={"LatestVersion": True},
            )
            schema_def = json.loads(resp["SchemaDefinition"])
            pandas_dtypes: Dict[str, str] = {}
            parse_dates: List[str] = []

            for field, props in schema_def["properties"].items():
                glue_type = props.get("type")
                fmt = props.get("format")
                if glue_type == "string":
                    if fmt == "date-time":
                        parse_dates.append(field)
                    else:
                        pandas_dtypes[field] = "string"
                elif glue_type == "integer":
                    pandas_dtypes[field] = "int64"
                elif glue_type == "number":
                    pandas_dtypes[field] = "float64"
                else:
                    pandas_dtypes[field] = "object"

            return pandas_dtypes, parse_dates

        except botocore.exceptions.ClientError as error:
            logger.info("Error fetching schema from Glue Schema Registry: %s", error)
            raise error

    @staticmethod
    def map_json_schema_types_to_pandas(fields) -> dict:
        """
        Map Json schema field types to pandas/awswrangler compatible data types.

        Args:
            fields (list): List of field definitions from schema

        Returns:
            dict: Mapping of field names to pandas data types
        """
        dtypes: Dict[str, str] = {}
        for field in fields:
            name = field["name"]
            # if registry returns 'type' and optional 'format'
            if isinstance(field["type"], dict):
                props = field["type"]
                glue_type = props.get("type")
                fmt = props.get("format")
            else:
                props = field
                glue_type = props.get("type")
                fmt = props.get("format")

            if glue_type == "string":
                dtypes[name] = "timestamp" if fmt == "date-time" else "string"
            elif glue_type == "integer":
                dtypes[name] = "bigint"
            elif glue_type == "number":
                dtypes[name] = "float"
            else:
                dtypes[name] = "object"

        return dtypes

    @staticmethod
    def parse_s3_keys(keys):
        """
        Parse a list of S3 keys to determine the earliest and latest timestamps.

        This function extracts year/month/day/hour components from S3 keys and
        calculates the time range covered by the dataset.

        Args:
            keys (List[str]): List of S3 keys with timestamp information

        Returns:
            tuple: (earliest_timestamp, latest_timestamp) as pendulum datetime objects
                  Returns (None, None) if no valid timestamps found
        """
        # Define regex pattern to match the date and time components in the keys
        pattern = re.compile(
            r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})(/hour=(\d{1,2}))?"
        )
        timestamps = []

        for key in keys:
            logger.info(f"Parsing timestamp from key: {key}")
            match = pattern.search(key)
            if match:
                groups = match.groups()
                year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                hour = int(groups[4]) if groups[4] else 0

                logger.info(
                    f"Extracted components: year={year}, month={month}, day={day}, hour={hour}"
                )

                # Create timestamp
                timestamp = pendulum.datetime(year, month, day, hour)
                timestamps.append(timestamp)
            else:
                logger.warning(f"Could not parse timestamp from key: {key}")

        if not timestamps:
            logger.error("No valid timestamps found in any keys")
            return None, None

        earliest_timestamp = min(timestamps)
        latest_timestamp = max(timestamps).add(hours=1).start_of("hour")

        logger.info(f"Determined time range: {earliest_timestamp} to {latest_timestamp}")
        return earliest_timestamp, latest_timestamp

    def load_to_raw(self, data_df: pd.DataFrame, s3_key: str, folder: str) -> str:
        """
        Load data into raw S3 bucket in parquet format with appropriate schema mapping.

        This function:
        1. Adds load_time and partitioning columns
        2. Fetches the Glue schema for the target table
        3. Maps data types based on the schema
        4. Writes data to the raw S3 bucket with partitioning

        Args:
            data_df (pd.DataFrame): DataFrame containing data to load
            s3_key (str): Original S3 key of the file
            folder (str): Folder type identifier

        Returns:
            str: The schema name that was used

        Raises:
            Exception: If there's an error writing the data
        """

        try:
            data_df["load_time"] = LOAD_DATETIME
            match = re.search(
                r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})/hour=(\d{1,2})", s3_key
            )
            if match:
                year, month, day, hour = match.groups()
            else:
                year = str(LOAD_DATETIME.year)
                month = str(LOAD_DATETIME.month)
                day = str(LOAD_DATETIME.day)
                hour = str(LOAD_DATETIME.hour)

            data_df["year"] = f"{int(year):04d}"
            data_df["month"] = f"{int(month):02d}"
            data_df["day"] = f"{int(day):02d}"
            data_df["hour"] = f"{int(hour):02d}"

            table_info = self.config.glue_tables.get(folder)
            if table_info is None:
                raise KeyError(
                    f"No glue table mapping for folder '{folder}'. "
                    "Make sure glue_tables keys match source_folders."
                )

            schema = table_info["schema"]
            glue_table = table_info["glue_table"]

            response = self._glue.get_schema_version(
                SchemaId={"RegistryName": "raw", "SchemaName": schema},
                SchemaVersionNumber={"LatestVersion": True},
            )

            schema_definition = json.loads(response["SchemaDefinition"])
            schema_type = self.config.schema_type
            logger.debug("Glue schema format received: %s", schema_type)

            match schema_type.lower():
                case "avro":
                    dtypes = self.map_avro_schema_types_to_pandas(schema_definition["fields"])
                case "json":
                    json_fields = [
                        {
                            "name": col_name,
                            "type": props
                        }
                        for col_name, props in schema_definition["properties"].items()
                    ]
                    dtypes = self.map_json_schema_types_to_pandas(json_fields)
                case _:
                    raise ValueError(f"Unsupported schema type: {schema_type}")

            s3_path = f"s3://{self.config.dest_bucket}/{self.config.partner}/{schema}"
            wr.s3.to_parquet(
                df=data_df,
                path=s3_path,
                dataset=True,
                mode="overwrite_partitions",
                partition_cols=["year", "month", "day", "hour"],
                dtype=dtypes,
            )
            logger.info(
                "Data successfully loaded to raw bucket for %s at %s/%s",
                folder,
                self.config.partner,
                schema,
            )
            return schema
        except Exception as e:
            logger.error("Error loading to raw: %s", e)
            raise

    def process_folder(self, folder: str):
        """
          Process a specific folder for a given time period.

            This function:
            1. Determines the S3 prefix to use (from params or scheduled interval)
            2. Lists and filters S3 keys for the specific folder
            3. Processes each file by loading, transforming, and writing to raw

            Args:
                folder (str): Name of the folder to process

            Returns:
                Dict: Dictionary with:
                    - processed_keys: List of successfully processed S3 keys
                    - raw_paths: List of output paths in the raw bucket
                    - table_info: Glue table configuration for this folder
            """
        # Determine the S3 prefix to use
        if self.config.override_s3_uri:
            # if the user passed a full URI override, trust it completely
            s3_prefix = self.config.override_s3_uri.rstrip("/") + "/"
            logger.info("Using override URI: %s", s3_prefix)

        elif self.config.dag_run and self.config.dag_run.external_trigger:
            # Manual trigger → Use the provided `s3_path` directly
            s3_prefix = f"{self.config.s3_prefix}{folder}/"
            logger.info(
                "Manual trigger detected. Using provided S3 path: %s", s3_prefix
            )
        else:
            # Scheduled DAG → Construct `year/month/day/hour` dynamically
            data_interval_end = self.config.data_interval_end or pendulum.now("UTC")
            s3_prefix = (
                f"{self.config.s3_prefix}{folder}year={data_interval_end.year}/"
                f"month={data_interval_end.month}/day={data_interval_end.day}/hour={data_interval_end.hour}/"
            )
            logger.info(
                "Scheduled run detected. Constructed S3 prefix: %s", s3_prefix
            )

        logger.info("Listing keys with prefix: %s", s3_prefix)

        processed_keys = []
        raw_paths = []

        s3_keys = self._s3_hook.list_keys(
            bucket_name=self.config.source_bucket,
            prefix=s3_prefix,
        )

        # filter & process
        for key in s3_keys or []:
            if key.endswith(".parquet"):
                df = self.load_data_from_parquet(self.config.source_bucket, key)
            elif key.endswith(".txt"):
                df = self.load_data_from_txt(self.config.source_bucket, key)
            else:
                continue

            try:
                schema_used = self.load_to_raw(df, key, folder)
                processed_keys.append(key)

                raw_path = f"{self.config.partner}/{schema_used}"
                if raw_path not in raw_paths:
                    raw_paths.append(raw_path)

            except Exception as e:
                logger.error("Error processing %s: %s", key, e)

        return processed_keys, raw_paths

    def load_glue_partitions(
        self,
        folder: str,
        raw_path: str,
        processed_keys: List[str],
        airflow_context: dict | None = None,
    ):
        """
        Load Glue partitions for processed data.

        This function uses the S3GlueLoadPartitionsOperator to:
        1. Add new partitions to the Glue catalog
        2. Ensure the catalog is up-to-date with the data lake

        Args:
            folder (str): Name of the processed folder
            raw_path (str): prefix in the raw bucket that now contains the new data
            processed_keys (str): Full S3 object keys that were successfully written (one per file).
            airflow_context: Airflow context variables

        Returns:
            None
        """
        table_info = self.config.glue_tables.get(folder)
        if not table_info:
            logger.info("No Glue table mapping for %s", folder)
            return

        glue_table = table_info["glue_table"]
        earliest, latest = self.parse_s3_keys(processed_keys)
        if not earliest or not latest:
            logger.info("Could not determine time range for %s", folder)
            return

        raw_prefix = f"{raw_path}/"
        logger.info(
            "Loading partitions for %s (Glue table %s) from %s to %s",
            raw_prefix,
            glue_table,
            earliest,
            latest,
        )

        try:
            ingress = S3GlueLoadPartitionsOperator(
                task_id=f"load_{glue_table}_partitions",
                s3_config=SourceS3Config(
                    bucket=self.config.dest_bucket,
                    prefix=raw_prefix,
                    database="raw",
                    table=glue_table,
                    aws_region=self.config.aws_region,
                ),
                load_start_date=earliest,
                load_end_date=latest,
            )
            ingress.execute(context=airflow_context or {})
            logger.info("Successfully loaded partitions for %s", glue_table)

        except Exception as e:
            logger.error("Error loading partitions for %s: %s", glue_table, e)

    def run(self, airflow_context: dict) -> None:
        """
        This is a driver function which drives the entire batch-ingest workflow. Steps
        performed in this function are:
            1. process_folder - which loads data from parquet files and writes to raw
            2. load_glue_partitions - loads the hour level partitions to glue

        Args:
            airflow_context: Pass the airflow context so the underlying S3GlueLoadPartitionsOperator still receives templated variables
        """
        for folder in self.config.source_folders:
            logger.info(
                "Ingesting folder: %s",
                folder)
            processed_keys, raw_paths = self.process_folder(
                folder
            )

            if processed_keys and raw_paths:
                self.load_glue_partitions(
                    folder,
                    raw_paths[0],
                    processed_keys,
                    airflow_context
                )
                logger.info(
                    "Folder %s complete (%d file(s) written to %s)",
                    folder,
                    len(processed_keys),
                    raw_paths[0])
            else:
                logger.info("No data ingested for %s", folder)

        logger.info("Batch ingest finished successfully")
