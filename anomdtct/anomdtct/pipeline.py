# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dscontrib.jmccrosky.forecast.utils import s2d
from dscontrib.jmccrosky.anomdtct.data import get_raw_data, prepare_data
from dscontrib.jmccrosky.anomdtct.forecast import forecast


# Get full table for testing and debugging
def get_data(bq_client, bq_storage_client):
    city_raw_data = get_raw_data(
        bq_client,
        bq_storage_client,
        "light_funnel_dau_city"
    )
    (city_clean_data, city_clean_training_data) = prepare_data(
        city_raw_data, s2d('2016-04-08'), s2d('2020-01-30')
    )
    city_forecast_data = forecast(city_clean_training_data, city_clean_data)

    country_raw_data = get_raw_data(
        bq_client,
        bq_storage_client,
        "light_funnel_dau_country"
    )
    (country_clean_data, country_clean_training_data) = prepare_data(
        country_raw_data, s2d('2016-04-08'), s2d('2020-01-30')
    )
    country_forecast_data = forecast(
        country_clean_training_data, country_clean_data
    )
    city_forecast_data.update(country_forecast_data)
    return city_forecast_data


# Write public data to BigQuery
def pipeline(bq_client, bq_storage_client, output_bq_client):
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
    dataset_ref = output_bq_client.dataset("analysis")
    table_ref = dataset_ref.table("deviations")
    try:
        output_bq_client.delete_table(table_ref)
    except NotFound:
        pass
    schema = [
        bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('metric', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('deviation', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('ci_deviation', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('geography', 'STRING', mode='REQUIRED'),
    ]
    table = bigquery.Table(table_ref, schema=schema)
    table = output_bq_client.create_table(table)
    n = len(output_data)
    for i in range(0, n, 10000):
        errors = output_bq_client.insert_rows(
            table,
            list(output_data[i:min(i + 10000, n)].itertuples(index=False, name=None))
        )
    return (output_data, errors)
