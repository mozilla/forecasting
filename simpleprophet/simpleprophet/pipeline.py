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

from simpleprophet.output import (reset_output_table, write_forecasts,
                                  prepare_records, write_records)
from simpleprophet.data import get_kpi_data, get_nondesktop_data, get_fxasub_data
from simpleprophet.utils import get_latest_date


FIRST_MODEL_DATES = {
    'Desktop Global MAU': pd.to_datetime("2019-03-08").date(),
    'Desktop Tier1 MAU': pd.to_datetime("2019-03-08").date(),
    'Mobile Global MAU': pd.to_datetime("2019-03-08").date(),
    'Mobile Tier1 MAU': pd.to_datetime("2019-03-08").date(),
    'FxA Global MAU': pd.to_datetime("2019-03-08").date(),
    'FxA Tier1 MAU': pd.to_datetime("2019-03-08").date(),

    'Fennec iOS Global MAU': pd.to_datetime("2019-03-08").date(),
    'Firefox Lite Global MAU': pd.to_datetime("2019-05-20").date(),
    'Focus iOS Global MAU': pd.to_datetime("2019-03-08").date(),
    'Fenix Global MAU': pd.to_datetime("2019-07-05").date(),
    'FirefoxConnect Global MAU': pd.to_datetime("2019-03-08").date(),
    'FirefoxForFireTV Global MAU': pd.to_datetime("2019-03-08").date(),
    'Fennec Android Global MAU': pd.to_datetime("2019-03-08").date(),
    'Focus Android Global MAU': pd.to_datetime("2019-03-08").date(),
    'Lockwise Android Global MAU': pd.to_datetime("2019-09-01").date(),

    'Fennec iOS Tier1 MAU': pd.to_datetime("2019-03-08").date(),
    'Firefox Lite Tier1 MAU': pd.to_datetime("2019-05-20").date(),
    'Focus iOS Tier1 MAU': pd.to_datetime("2019-03-08").date(),
    'Fenix Tier1 MAU': pd.to_datetime("2019-07-05").date(),
    'FirefoxConnect Tier1 MAU': pd.to_datetime("2019-03-08").date(),
    'FirefoxForFireTV Tier1 MAU': pd.to_datetime("2019-03-08").date(),
    'Fennec Android Tier1 MAU': pd.to_datetime("2019-03-08").date(),
    'Focus Android Tier1 MAU': pd.to_datetime("2019-03-08").date(),
    'Lockwise Android Tier1 MAU': pd.to_datetime("2019-09-01").date(),

    'FxA Registration with Subscription Tier1 DAU': pd.to_datetime("2020-01-01").date(),
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
    if datasource.lower() == 'mobile':
        nondesktop_data = get_nondesktop_data(bq_client)
        data.update(nondesktop_data)
    if datasource.lower() == 'fxa':
        fxasub_data = get_fxasub_data(bq_client)
        data.update(fxasub_data)
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
            data[product].ds.max()
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
            data[product].ds.max()
        )
        for model_date in model_dates:
            logging.info("Processing {} forecast for {}".format(product, model_date))
            write_forecasts(
                bq_client, table, model_date.date(),
                FORECAST_HORIZON, data[product], product
            )
