# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Single functions for running the forecasting pipeline.
"""
from datetime import timedelta, date
import logging

from google.cloud import bigquery
import pandas as pd

from simpleprophet.output import reset_output_table, write_forecasts
from simpleprophet.output import prepare_records, write_records
from simpleprophet.data import get_kpi_data, get_nondesktop_data
from simpleprophet.utils import get_latest_date


FIRST_MODEL_DATES = {
    'desktop_global_mau': pd.to_datetime("2019-03-08").date(),
    'desktop_tier1_mau': pd.to_datetime("2019-03-08").date(),
    'mobile_global_mau': pd.to_datetime("2019-03-08").date(),
    'mobile_tier1_mau': pd.to_datetime("2019-03-08").date(),
    'fxa_global_mau': pd.to_datetime("2019-03-08").date(),
    'fxa_tier1_mau': pd.to_datetime("2019-03-08").date(),

    'Fennec iOS MAU': pd.to_datetime("2019-03-08").date(),
    'Firefox Lite MAU': pd.to_datetime("2019-05-20").date(),
    'Focus iOS MAU': pd.to_datetime("2019-03-08").date(),
    'Fenix MAU': pd.to_datetime("2019-07-05").date(),
    'FirefoxConnect MAU': pd.to_datetime("2019-03-08").date(),
    'FirefoxForFireTV MAU': pd.to_datetime("2019-03-08").date(),
    'Fennec Android MAU': pd.to_datetime("2019-03-08").date(),
    'Focus Android MAU': pd.to_datetime("2019-03-08").date(),
    'Lockwise Android MAU': pd.to_datetime("2019-09-01").date(),

    'Fennec iOS tier1 MAU': pd.to_datetime("2019-03-08").date(),
    'Firefox Lite tier1 MAU': pd.to_datetime("2019-05-20").date(),
    'Focus iOS tier1 MAU': pd.to_datetime("2019-03-08").date(),
    'Fenix tier1 MAU': pd.to_datetime("2019-07-05").date(),
    'FirefoxConnect tier1 MAU': pd.to_datetime("2019-03-08").date(),
    'FirefoxForFireTV tier1 MAU': pd.to_datetime("2019-03-08").date(),
    'Fennec Android tier1 MAU': pd.to_datetime("2019-03-08").date(),
    'Focus Android tier1 MAU': pd.to_datetime("2019-03-08").date(),
    'Lockwise Android tier1 MAU': pd.to_datetime("2019-09-01").date(),
}
FORECAST_HORIZON = pd.to_datetime("2020-12-31").date()
DEFAULT_BQ_PROJECT = "moz-fx-data-derived-datasets"
DEFAULT_BQ_DATASET = "analysis"
DEFAULT_BQ_TABLE = "jmccrosky_test"


def replace_single_day(
    bq_client,
    datasource,
    dt,
    project_id=DEFAULT_BQ_PROJECT,
    dataset_id=DEFAULT_BQ_DATASET,
    table_id=DEFAULT_BQ_TABLE,
):
    model_date = date.fromisoformat(dt)
    data = {}
    kpi_data = get_kpi_data(bq_client, types=[datasource])
    data.update(kpi_data)
    if datasource == 'mobile':
        nondesktop_data = get_nondesktop_data(bq_client)
        data.update(nondesktop_data)
    partition_decorator = "$" + model_date.isoformat().replace('-', '')
    table = '.'.join([project_id, dataset_id, table_id]) + partition_decorator
    records = []
    for product in data.keys():
        logging.info("Processing {} forecast for {}".format(product, model_date))
        records += prepare_records(model_date, FORECAST_HORIZON, data[product], product)
    logging.info("Replacing results for {} in {}".format(model_date, table))
    write_records(bq_client, records, table,
                  write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE)


def update_table(
    bq_client, project_id=DEFAULT_BQ_PROJECT, dataset_id=DEFAULT_BQ_DATASET,
    table_id=DEFAULT_BQ_TABLE
):
    kpi_data = get_kpi_data(bq_client)
    nondesktop_data = get_nondesktop_data(bq_client)
    data = kpi_data
    data.update(nondesktop_data)
    dataset = bq_client.dataset(dataset_id)
    tableref = dataset.table(table_id)
    table = bq_client.get_table(tableref)
    for product in data.keys():
        logging.info("Processing forecasts for {}".format(product))
        latest_date = get_latest_date(
            bq_client, project_id, dataset_id, table_id, product, "asofdate"
        )
        if latest_date is not None:
            start_date = latest_date + timedelta(days=1)
        else:
            start_date = FIRST_MODEL_DATES[product]
        model_dates = pd.date_range(
            start_date,
            data[product].ds.max() - timedelta(days=1)
        )
        for model_date in model_dates:
            logging.info("Processing {} forecast for {}".format(product, model_date))
            write_forecasts(
                bq_client, table, model_date.date(),
                FORECAST_HORIZON, data[product], product
            )


def replace_table(
    bq_client, project_id=DEFAULT_BQ_PROJECT, dataset_id=DEFAULT_BQ_DATASET,
    table_id=DEFAULT_BQ_TABLE
):
    kpi_data = get_kpi_data(bq_client)
    nondesktop_data = get_nondesktop_data(bq_client)
    data = kpi_data
    data.update(nondesktop_data)
    table = reset_output_table(bq_client, project_id, dataset_id, table_id)
    for product in data.keys():
        logging.info("Processing forecasts for {}".format(product))
        model_dates = pd.date_range(
            FIRST_MODEL_DATES[product],
            data[product].ds.max() - timedelta(days=1)
        )
        for model_date in model_dates:
            logging.info("Processing {} forecast for {}".format(product, model_date))
            write_forecasts(
                bq_client, table, model_date.date(),
                FORECAST_HORIZON, data[product], product
            )
