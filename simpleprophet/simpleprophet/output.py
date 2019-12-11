# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tools for writing forecasts to BigQuery.
"""
import json
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import timedelta

from simpleprophet.models import setup_models, data_filter


SCHEMA = [
        bigquery.SchemaField(
            "asofdate", "DATE", mode="REQUIRED",
            description="Latest date of actuals used for this model run"),
        bigquery.SchemaField(
            "datasource", "STRING", mode="REQUIRED",
            description="Identifier capturing data, model, and target metric"),
        bigquery.SchemaField(
            "date", "DATE", mode="REQUIRED",
            description="Date that this particular row describes"),
        bigquery.SchemaField(
            "type", "STRING", mode="REQUIRED",
            description="Type of data this row describes: forecast or actual"),
        bigquery.SchemaField(
            "value", "FLOAT", mode="REQUIRED",
            description="Actual or forecasted value for the target metric"),
        bigquery.SchemaField(
            "low90", "FLOAT", mode="REQUIRED",
            description="Low end of the credible interval around this value"),
        bigquery.SchemaField(
            "high90", "FLOAT", mode="REQUIRED",
            description="High end of the credible interval around this value"),
    ] + [
        bigquery.SchemaField('p{}'.format(q), 'FLOAT', mode='REQUIRED')
        for q in range(10, 100, 10)
    ]


# Delete output table if necessary and create empty table with appropriate schema
def reset_output_table(bigquery_client, project, dataset, table_name):
    table_ref = '.'.join([project, dataset, table_name])
    try:
        bigquery_client.delete_table(table_ref)
    except NotFound:
        pass
    table = bigquery.Table(table_ref, schema=SCHEMA)
    table.time_partitioning = bigquery.table.TimePartitioning(field="asofdate")
    table = bigquery_client.create_table(table)
    return table


def prepare_records(modelDate, forecast_end, data, product):
    minYear = data.ds.min().year
    maxYear = forecast_end.year
    years = range(minYear, maxYear + 1)
    models = setup_models(years)
    forecast_start = modelDate + timedelta(days=1)
    forecast_period = pd.DataFrame({'ds': pd.date_range(forecast_start, forecast_end)})
    data = data_filter(data, product)
    models[product].fit(data.query("ds <= @modelDate"))
    forecast_samples = models[product].sample_posterior_predictive(
        models[product].setup_dataframe(forecast_period)
    )
    forecast = models[product].predict(forecast_period)
    output_data = {
        "asofdate": modelDate,
        "datasource": product,
        "date": forecast.ds,
        "type": "forecast",
        "value": forecast.yhat,
        "low90": forecast.yhat_lower,
        "high90": forecast.yhat_upper,
    }
    output_data.update({
        "p{}".format(q): np.nanpercentile(forecast_samples['yhat'], q, axis=1)
        for q in range(10, 100, 10)
    })
    output_data = pd.DataFrame(output_data)[[
      "asofdate", "datasource", "date", "type", "value", "low90", "high90",
      "p10", "p20", "p30", "p40", "p50", "p60", "p70", "p80", "p90"
    ]]
    # We convert dates to strings here as the BigQuery loading machinery
    # writes out the records as JSON and expects ISO-formatted date strings.
    output_data['asofdate'] = pd.to_datetime(output_data['asofdate']).dt.strftime('%Y-%m-%d')
    output_data['date'] = output_data['date'].dt.strftime('%Y-%m-%d')
    return output_data.to_dict('records')


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


def write_forecasts(bigquery_client, table, modelDate, forecast_end, data, product,
                    write_disposition=bigquery.job.WriteDisposition.WRITE_APPEND):
    records = prepare_records(modelDate, forecast_end, data, product)
    write_records(bigquery_client, records, table, write_disposition)
