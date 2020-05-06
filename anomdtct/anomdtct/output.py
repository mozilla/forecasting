import google
from google.cloud import bigquery
import gspread
from googleapiclient.discovery import build
import json


def write_records(bigquery_client, records, table, write_disposition):
    schema = [
        bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('metric', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('deviation', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('ci_deviation', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('geography', 'STRING', mode='REQUIRED'),
    ]

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema,
    )
    load_job = bigquery_client.load_table_from_json(
        records,
        table,
        job_config=job_config,
    )
    # Wait for load job to complete; raises an exception if the job failed.
    load_job.result()

def write_to_spreadsheet(data, spreadsheet_id):
    scopes = [
        'https://www.googleapis.com/auth/drive'
    ]

    credentials, project = google.auth.default(scopes=scopes)

    service = build('sheets', 'v4', credentials=credentials)
    response_date = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        valueInputOption='RAW',
        range="Sheet1",
        body=dict(
            majorDimension='ROWS',
            values=data.T.reset_index().T.values.tolist())
    ).execute()
    print('Sheet successfully Updated')


def write_model(bigquery_client, table, records):
    # writes the pickled models to a BigQuery table which serves as a cache

    schema = [
        bigquery.SchemaField('metric', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('geography', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('model', 'BYTES', mode='REQUIRED'),
    ]

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )

    load_job = bigquery_client.load_table_from_json(
        records,
        table,
        job_config=job_config,
    )

    # Wait for load job to complete; raises an exception if the job failed.
    load_job.result()
