# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import timedelta, date
import pandas as pd
from google.cloud.bigquery._helpers import _bytes_to_json
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from anomdtct.utils import s2d
from anomdtct.data import get_raw_data, prepare_data, get_data_for_date, prepare_training_data
from anomdtct.forecast import forecast, fit_model
from anomdtct.output import write_records, write_to_spreadsheet, write_model
import logging


DEFAULT_BQ_PROJECT = "moz-fx-data-shared-prod"
DEFAULT_BQ_DATASET = "telemetry_derived"
DEFAULT_BQ_TABLE = "deviations_anomdtct_v1"

DEFAULT_BQ_MODEL_CACHE_PROJECT = "moz-fx-data-shared-prod"
DEFAULT_BQ_MODEL_CACHE_DATASET = "telemetry_derived"
DEFAULT_BQ_MODEL_CACHE_TABLE = "deviations_model_cache_v1"

METRICS = {
    "light_funnel_dau_city": "desktop_dau",
    "light_funnel_dau_country": "desktop_dau",
    "light_funnel_mean_active_hours_per_profile_city":
        "mean_active_hours_per_client",
    "light_funnel_mean_active_hours_per_profile_country":
        "mean_active_hours_per_client",
}


def fit_models(    
    bq_client,
    bq_storage_client,
    project_id=DEFAULT_BQ_MODEL_CACHE_PROJECT,
    dataset_id=DEFAULT_BQ_MODEL_CACHE_DATASET,
    table_id=DEFAULT_BQ_MODEL_CACHE_TABLE
):
    training_start_date = '2016-04-08'
    training_end_date = '2020-01-30'

    # overwrite existing table for caching models
    table = '.'.join([project_id, dataset_id, table_id])
    bq_client.delete_table(table, not_found_ok=True)

    for metric in METRICS.keys():
        raw_data = get_raw_data(
            bq_client,
            bq_storage_client,
            metric,
            training_start_date,
            training_end_date
        )
        clean_training_data = prepare_training_data(
            raw_data, s2d(training_start_date), s2d(training_end_date)
        )

        records = []

        for c in clean_training_data.keys():
            if (len(clean_training_data[c]) < 600):
                continue

            pickled_model = fit_model(clean_training_data, c)

            record = {
                "metric": metric,
                "geography": c,
                "model": _bytes_to_json(pickled_model)
            }

            records.append(record)

        write_model(bq_client, table, records)


def replace_single_day(
    bq_client,
    bq_storage_client,
    dt,
    project_id=DEFAULT_BQ_PROJECT,
    dataset_id=DEFAULT_BQ_DATASET,
    table_id=DEFAULT_BQ_TABLE,
    model_cache_project_id=DEFAULT_BQ_MODEL_CACHE_PROJECT,
    model_cache_dataset_id=DEFAULT_BQ_MODEL_CACHE_DATASET,
    model_cache_table_id=DEFAULT_BQ_MODEL_CACHE_TABLE,
    spreadsheet_id=None
):
    model_date = date.fromisoformat(dt)
    model_cache_table = '.'.join([model_cache_project_id, model_cache_dataset_id, model_cache_table_id])

    data = pipeline(bq_client, bq_storage_client, model_date, model_cache_table)
    partition_decorator = "$" + model_date.isoformat().replace('-', '')
    table = '.'.join([project_id, dataset_id, table_id]) + partition_decorator

    logging.info("Replacing results for {} in {}".format(model_date, table))
    records = data.to_dict('records')

    write_records(bq_client, records, table,
                  write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE)

    if spreadsheet_id is not None:
        write_to_spreadsheet(data, spreadsheet_id)


# Run the pipeline and calculate the forecast data
def pipeline(bq_client, bq_storage_client, model_date, model_cache_table):
    output_data = pd.DataFrame(
        {
            "date": [],
            "metric": [],
            "deviation": [],
            "ci_deviation": [],
            "geography": [],
        },
        columns=["date", "metric", "deviation", "ci_deviation", "geography"]
    )

    for metric in METRICS.keys():
        raw_data = get_data_for_date(
            bq_client,
            bq_storage_client,
            metric,
            model_date
        )
        clean_data = prepare_data(raw_data)

        forecast_data = forecast(clean_data, bq_client, model_cache_table, metric, model_date)

        for geo in forecast_data:
            output_data = pd.concat(
                [
                    output_data,
                    pd.DataFrame(
                        {
                            "date": pd.to_datetime(
                                forecast_data[geo].ds
                            ).dt.strftime("%Y-%m-%d"),
                            "metric": METRICS[metric],
                            "deviation": forecast_data[geo].delta,
                            "ci_deviation": forecast_data[geo].ci_delta,
                            "geography": geo,
                        },
                        columns=[
                            "date", "metric", "deviation",
                            "ci_deviation", "geography"
                        ]
                    )
                ],
                ignore_index=True
            )

    return output_data
