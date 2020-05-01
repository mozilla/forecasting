from google.cloud import bigquery


SCHEMA = [
    bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
    bigquery.SchemaField('metric', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('deviation', 'FLOAT', mode='REQUIRED'),
    bigquery.SchemaField('ci_deviation', 'FLOAT', mode='REQUIRED'),
    bigquery.SchemaField('geography', 'STRING', mode='REQUIRED'),
]


def write_records(bigquery_client, records, table, write_disposition):
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=SCHEMA,
    )
    load_job = bigquery_client.load_table_from_json(
        records,
        table,
        job_config=job_config,
    )
    # Wait for load job to complete; raises an exception if the job failed.
    load_job.result()
