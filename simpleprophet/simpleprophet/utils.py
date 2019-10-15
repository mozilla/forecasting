# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Various utility functions.
"""
import numpy as np
import pandas as pd
import plotly.graph_objs as go
from datetime import timedelta


# Calculate Mean Absolute Percentage Error of forecast
def calc_mape(true, predicted):
    mask = true != 0
    return (np.fabs(true - predicted)/true)[mask].mean() * 100


# Calculate Mean Relative Error of forecast
def calc_mre(true, predicted):
    mask = true != 0
    return ((true - predicted)/true)[mask].mean() * 100


def calc_log_ratio(true, predicted):
    logratio = np.mean(np.log(predicted) - np.log(true))
    return logratio


# Get most recent date in table
def get_latest_date(bq_client, project, dataset, table, product, field):
    query = '''
        SELECT
            MAX({field}) as date
        FROM
            `{project}.{dataset}.{table}`
        WHERE
            datasource="{product}"
    '''.format(
        project=project, dataset=dataset, table=table, field=field, product=product
    )
    data = bq_client.query(query).to_dataframe()
    if len(data) == 0:
        return None
    return bq_client.query(query).to_dataframe()['date'][0]


def split_data(
    data, first_train_date, first_holdout_date, first_test_date, last_test_date
):
    temp = data.set_index('ds')
    split_data = {
        "training": temp[
            first_train_date:(first_holdout_date - timedelta(days=1))
        ].reset_index(),
        "holdout": temp[
            first_holdout_date:(first_test_date - timedelta(days=1))
        ].reset_index(),
        "test": temp[first_test_date:last_test_date].reset_index(),
        "all": temp.reset_index(),
    }
    return split_data


def s2d(date_string):
    return pd.to_datetime(date_string).date()


def match_dates(data, forecast):
    return data.merge(forecast, on="ds", how="inner")


def get_layout(title, xaxis, yaxis):
    return go.Layout(
        title=title,
        xaxis=go.layout.XAxis(
            title=go.layout.xaxis.Title(
                text=xaxis
            )
        ),
        yaxis=go.layout.YAxis(
            title=go.layout.yaxis.Title(
                text=yaxis
            )
        ),
    )


def generate_forecast_data(
    model_gen, metric_data, asofdate_range, target_date_range
):
    data = pd.DataFrame({
        "ds": [],
        "asofdate": [],
        "yhat": [],
        "yhat_lower": [],
        "yhat_upper": [],
    })
    for asofdate in asofdate_range:
        model = model_gen()
        model.fit(metric_data.query("ds <= @asofdate"))
        forecast_period = pd.DataFrame({'ds': target_date_range})
        forecast = model.predict(forecast_period)
        data = pd.concat([data, pd.DataFrame({
            "ds": forecast.ds,
            "asofdate": asofdate,
            "yhat": forecast.yhat,
            "yhat_lower": forecast.yhat_lower,
            "yhat_upper": forecast.yhat_upper,
        })], ignore_index=True)
    data['ds'] = pd.to_datetime(data['ds']).dt.date
    return data
