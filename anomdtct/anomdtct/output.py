from google.cloud import bigquery
import gspread
from googleapiclient.discovery import build
from google.oauth2 import service_account
import json


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

def write_to_spreadsheet(data, spreadsheet_id, key):
    scopes = [
        'https://www.googleapis.com/auth/drive'
    ]
    service_account_info = json.loads(key)

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=scopes)
    service = build('sheets', 'v4', credentials=credentials)
    response_date = service.spreadsheets().values().update(
        spreadsheetId="1jHWW9QYAOCNTVwyWF29YiVGDf4uX3TcLgREVrQ1bkHI",
        valueInputOption='RAW',
        range="Sheet1",
        body=dict(
            majorDimension='ROWS',
            values=data.T.reset_index().T.values.tolist())
    ).execute()
    print('Sheet successfully Updated')
