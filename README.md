# Python Batch Ingestion Class (Batch Ingest Data from a "landing" S3 bucket to a "raw" bucket and load the partitions to your AWS Glue Catalog)

 The BatchIngestor class is wrapper class that abstracts the logic behind batch ingestion for partner folders. It takes configurations
 from the BatchIngestConfig class, and it handles the entire batch ingestion operation allowing for single folder to single table
 ingestion and multi-folder to multi-table ingestion.

 What it does:
    • Load data from parquet files in S3 buckets
    • Reads each file, adds `load_time` + `year/month/day/hour` columns.
    • Uses `glue_tables` to pick the target schema, casts dtypes, and writes
      partitioned Parquet to the raw bucket.
    • Calls `S3GlueLoadPartitionsOperator` to add the new partitions in Glue.
    • Logs errors per file; only fails if the entire folder fails.

How to use:
Example use case -> You have an Airflow DAG that is supposed to handle the batch ingestion of data according to a specified schedule. Now, you can use this class by passing a few configuration parameters and creating a "batch_ingestion" class in your DAG and the class will handle the entire batch ingestion logic for you.

    @task(task_id="batch_ingestion")                     # the task that the DAG will identify
    def batch_ingestion(folder: str, **ctx):
         config = BatchIngestConfig(
            source_bucket="LANDING_BUCKET",
            dest_bucket="RAW_BUCKET",
            aws_region="AWS_REGION",
            s3_prefix="folder_to_ingest/",                     # e.g "transactions/2025/" 
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

     your_other_tasks >> batch_ingestion(transactions_data_folder, **ctx)    # task will show up as one individual task in your airflow UI

     OR ...

    for folder in GLUE_TABLES:                                              # each folder being ingested will have it's own task in your airflow UI
        batch_task = batch_ingestion.override(task_id=f"process_{folder}")(folder)
        batch_task >> complete
