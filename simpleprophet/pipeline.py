# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pandas as pd
from datetime import timedelta

from forecasting.simpleprophet.output import resetOuputTable, writeForecasts
from forecasting.simpleprophet.data import getKPIData, getNondesktopData
from forecasting.simpleprophet.data import getNondesktopNoFireData
from forecasting.simpleprophet.utils import getLatestDate


_FIRST_MODEL_DATES = {
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
_FORECAST_HORIZON = pd.to_datetime("2019-12-31").date()
_BQ_PROJECT = "moz-fx-data-derived-datasets"
_BQ_DATASET = "analysis"
_BQ_TABLE = "jmccrosky_test"


def updateTable(bqClient):
    kpiData = getKPIData(bqClient)
    nondesktopData = getNondesktopData(bqClient)
    nondesktopnofireData = getNondesktopNoFireData(bqClient)
    data = kpiData
    data.update(nondesktopData)
    data.update(nondesktopnofireData)
    dataset = bqClient.dataset(_BQ_DATASET)
    tableref = dataset.table(_BQ_TABLE)
    table = bqClient.get_table(tableref)
    for product in data.keys():
        latestDate = getLatestDate(
            bqClient, _BQ_PROJECT, _BQ_DATASET, _BQ_TABLE, product, "asofdate"
        )
        if latestDate is not None:
            startDate = latestDate + timedelta(days=1)
        else:
            startDate = _FIRST_MODEL_DATES[product]
        model_dates = pd.date_range(
            startDate,
            data[product].ds.max() - timedelta(days=1)
        )
        for model_date in model_dates:
            writeForecasts(
                bqClient, table, model_date.date(),
                _FORECAST_HORIZON, data[product], product
            )


def replaceTable(bqClient):
    kpiData = getKPIData(bqClient)
    nondesktopData = getNondesktopData(bqClient)
    nondesktopnofireData = getNondesktopNoFireData(bqClient)
    data = kpiData
    data.update(nondesktopData)
    data.update(nondesktopnofireData)
    table = resetOuputTable(bqClient, _BQ_PROJECT, _BQ_DATASET, _BQ_TABLE)
    for product in data.keys():
        model_dates = pd.date_range(
            _FIRST_MODEL_DATES[product],
            data[product].ds.max() - timedelta(days=1)
        )
        for model_date in model_dates:
            writeForecasts(
                bqClient, table, model_date.date(),
                _FORECAST_HORIZON, data[product], product
            )
