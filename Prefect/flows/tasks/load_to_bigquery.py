import os

from google.cloud import storage, bigquery
from google.cloud.bigquery import LoadJobConfig, SourceFormat
from prefect import get_run_logger, task

from schemas.activities import ACTIVITIES_SCHEMA

client = bigquery.Client(project=os.getenv("PROJECT"))

@task
def create_bq_dataset(dataset_name: str, dataset_location: str="europe-west3"):
        
    dataset_object = bigquery.Dataset(f"{client.project}.{dataset_name}")
    dataset_object.location = dataset_location

    client.create_dataset(
        dataset=dataset_object,
        timeout=30,
        exists_ok=True
    )

@task
def load_to_bronze_table():

    logger = get_run_logger()

    table_id = f"{client.project}.bronze_layer.activities_raw"
    job_config = LoadJobConfig(
        schema=ACTIVITIES_SCHEMA,
        source_format = SourceFormat.NEWLINE_DELIMITED_JSON
    )
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(os.getenv("BUCKET"))
    blobs = storage_client.list_blobs(bucket.name)    

    for blob in blobs:
        try:
            if blob.metadata and blob.metadata.get("Loaded") == "False":
                url = f"gs://{bucket.name}/{blob.name}"
                load_job = client.load_table_from_uri(
                    source_uris=url,
                    destination=table_id,
                    location="europe-west3",
                    job_config=job_config
                )

                load_job.result()

                logger.info(f"Loaded {load_job.output_rows} rows.")

                blob.metadata = {**blob.metadata, "Loaded": "True"}
                blob.patch(client=storage_client)

        except Exception as e:
            logger.warning(f"Something happened while loading a blob {blob.name}... {e}")
            raise