# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Single functions for running the forecasting pipeline.
"""
from datetime import timedelta
import logging

import pandas as pd

from simpleprophet.output import reset_output_table, write_forecasts
from simpleprophet.data import get_kpi_data, get_nondesktop_data
from simpleprophet.data import get_nondesktop_nofire_data
from simpleprophet.utils import get_latest_date


FIRST_MODEL_DATES = {
      'Fennec iOS': pd.to_datetime("2019-03-08").date(),
      'fxa_global': pd.to_datetime("2019-03-08").date(),
      'Firefox Lite': pd.to_datetime("2019-05-20").date(),
      'Focus iOS': pd.to_datetime("2019-03-08").date(),
      'Fenix': pd.to_datetime("2019-07-05").date(),
      'nondesktop_tier1': pd.to_datetime("2019-03-08").date(),
      'desktop_global': pd.to_datetime("2019-03-08").date(),
      'nondesktop_global': pd.to_datetime("2019-03-08").date(),
      'FirefoxConnect': pd.to_datetime("2019-03-08").date(),
      'FirefoxForFireTV': pd.to_datetime("2019-03-08").date(),
      'Fennec Android': pd.to_datetime("2019-03-08").date(),
      'desktop_tier1': pd.to_datetime("2019-03-08").date(),
      'fxa_tier1': pd.to_datetime("2019-03-08").date(),
      'Focus Android': pd.to_datetime("2019-03-08").date(),
      'nondesktop_nofire_global': pd.to_datetime("2019-03-08").date(),
      'nondesktop_nofire_tier1': pd.to_datetime("2019-03-08").date(),
}
FORECAST_HORIZON = pd.to_datetime("2020-12-31").date()
DEFAULT_BQ_PROJECT = "moz-fx-data-derived-datasets"
DEFAULT_BQ_DATASET = "analysis"
DEFAULT_BQ_TABLE = "jmccrosky_test"


def update_table(
    bq_client, project_id=DEFAULT_BQ_PROJECT, dataset_id=DEFAULT_BQ_DATASET,
    table_id=DEFAULT_BQ_TABLE
):
    kpi_data = get_kpi_data(bq_client)
    nondesktop_data = get_nondesktop_data(bq_client)
    nondesktop_nofire_data = get_nondesktop_nofire_data(bq_client)
    data = kpi_data
    data.update(nondesktop_data)
    data.update(nondesktop_nofire_data)
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
    nondesktop_nofire_data = get_nondesktop_nofire_data(bq_client)
    data = kpi_data
    data.update(nondesktop_data)
    data.update(nondesktop_nofire_data)
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
