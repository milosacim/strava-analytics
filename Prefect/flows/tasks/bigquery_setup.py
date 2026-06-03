import os
from google.cloud import bigquery

from google.cloud.bigquery import SourceFormat
from prefect import get_run_logger, task
from prefect.cache_policies import NO_CACHE

@task(cache_policy=NO_CACHE)
def create_bq_dataset(bigquery_client, dataset_name: str, dataset_location: str = "europe-west3"):
    """
    Prefect task. Ensures a BigQuery dataset exists. Idempotent — no-ops
    if the dataset is already present.

    Args:
        bigquery_client: An authenticated google.cloud.bigquery.Client.
        dataset_name: Name of the dataset to create within the client's project.
        dataset_location: GCP region for the dataset. Defaults to 'europe-west3'.
    """

    dataset_object = bigquery.Dataset(f"{bigquery_client.project}.{dataset_name}")
    dataset_object.location = dataset_location

    bigquery_client.create_dataset(
        dataset=dataset_object,
        timeout=30,
        exists_ok=True
    )

@task(cache_policy=NO_CACHE)
def create_external_table(bucket, schema, folder: str, name: str, bigquery_client):
    """
    Prefect task. Ensures the bronze external BigQuery table exists, pointing
    at newline-delimited JSON files under 'raw_data/' in the given GCS bucket.
    Idempotent — no-ops if the table is already present.

    Args:
        bucket: A google.cloud.storage.Bucket whose 'raw_data/*.json' objects
                back the external table.
        schema: List of bigquery.SchemaField defining the JSON column layout.
        folder: GCS prefix under the bucket to scan, e.g. 'raw_data/activities'.
        name: Logical name; the resulting table is named 'ext_{name}_raw' in bronze_layer.
        bigquery_client: An authenticated google.cloud.bigquery.Client.
    """

    logger = get_run_logger()
    table_id = f"{bigquery_client.project}.bronze_layer.ext_{name}_raw"

    external_config = bigquery.ExternalConfig(source_format = SourceFormat.NEWLINE_DELIMITED_JSON)
    external_config.source_uris = [f"gs://{bucket.name}/{folder}*.json"]
    external_config.schema = schema
    
    table = bigquery.Table(table_id)
    table.external_data_configuration = external_config
    
    ext_table = bigquery_client.create_table(table, exists_ok=True)

    logger.info(
        f"Created table with external source format {ext_table.external_data_configuration.source_format}"
    )