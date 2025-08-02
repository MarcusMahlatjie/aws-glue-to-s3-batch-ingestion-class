import pendulum
from typing import Dict, List, Optional
from airflow.models import DagRun
from dataclasses import dataclass

@dataclass
class BatchIngestConfig:
    """
    Configuration container for the BatchIngestor class

    Required:
        • source_bucket (str): Landing bucket containing the source data
        • dest_bucket (str): Destination bucket name
        • s3_prefix (str): S3 prefix/folder name to load data from
        • partner (str): Name of the partner folder
        • source_folders (list[str]): A list of folders to load data from
        • glue_tables (dict[str, dict[str, str]): Maps each logical dataset identifier to the name of the AWS Glue table that the batch-ingestion job should write to
        • schema_type (str): The schema format to use in the AWS Glue load operation
        • aws_region (str): AWS region for glue load operation

    Optional:
        • dag_run (DagRun): Trigger type for the DAG run (assumes manual trigger if None)
        • data_interval_end (pendulum.datetime): End datetime for the data interval (assumes current datetime if None)
    """
    source_bucket: str
    dest_bucket: str
    s3_prefix: str
    override_s3_uri: str | None
    partner: str
    source_folders: List[str]
    glue_tables: Dict[str, Dict[str, str]]
    aws_region: str
    dag_run: Optional[DagRun] = None
    data_interval_end: Optional[pendulum.DateTime] = None
    schema_type: str = "avro"
