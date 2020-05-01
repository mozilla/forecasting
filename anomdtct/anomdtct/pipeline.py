# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import timedelta, date
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from anomdtct.utils import s2d
from anomdtct.data import get_raw_data, prepare_data
from anomdtct.forecast import forecast
from anomdtct.output import write_records, write_to_spreadsheet
import logging


DEFAULT_BQ_PROJECT = "moz-fx-data-shared-prod"
DEFAULT_BQ_DATASET = "analysis"
DEFAULT_BQ_TABLE = "deviations"


def replace_single_day(
    bq_client,
    bq_storage_client,
    dt,
    project_id=DEFAULT_BQ_PROJECT,
    dataset_id=DEFAULT_BQ_DATASET,
    table_id=DEFAULT_BQ_TABLE,
    spreadsheet_id=None,
    spreadsheet_key=None
):
    model_date = date.fromisoformat(dt)
    data = pipeline(bq_client, bq_storage_client)
    partition_decorator = "$" + model_date.isoformat().replace('-', '')
    table = '.'.join([project_id, dataset_id, table_id]) + partition_decorator

    logging.info("Replacing results for {} in {}".format(model_date, table))
    records = data.query("date = @model_date").to_dict('records')

    write_records(bq_client, records, table,
                  write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE)

    if spreadsheet_id is not None:
        write_to_spreadsheet(data, spreadsheet_id, spreadsheet_key)


# Run the pipeline and calculate the forecast data
def pipeline(bq_client, bq_storage_client):
    metrics = {
        "light_funnel_dau_city": "desktop_dau",
        "light_funnel_dau_country": "desktop_dau",
        "light_funnel_mean_active_hours_per_profile_city":
            "mean_active_hours_per_client",
        "light_funnel_mean_active_hours_per_profile_country":
            "mean_active_hours_per_client",
    }

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

    for metric in metrics.keys():
        raw_data = get_raw_data(
            bq_client,
            bq_storage_client,
            metric
        )
        (clean_data, clean_training_data) = prepare_data(
            raw_data, s2d('2016-04-08'), s2d('2020-01-30')
        )
        forecast_data = forecast(clean_training_data, clean_data)

        for geo in forecast_data:
            output_data = pd.concat(
                [
                    output_data,
                    pd.DataFrame(
                        {
                            "date": pd.to_datetime(
                                forecast_data[geo].ds
                            ).dt.strftime("%Y-%m-%d"),
                            "metric": metrics[metric],
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
