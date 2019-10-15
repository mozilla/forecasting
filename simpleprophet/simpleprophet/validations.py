# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tools for valdiation and evaluation of forecast models.
"""
import pandas as pd
import numpy as np
from datetime import timedelta
from collections import defaultdict
from plotly.offline import plot
import plotly.graph_objs as go
from simpleprophet.utils import match_dates, get_layout


def _get_single_prediction(forecast_data, asofdate, target_date):
    temp = forecast_data.query("asofdate == @asofdate & ds == @target_date")
    if len(temp.yhat) == 1:
        return (temp.yhat.item(), temp.yhat_lower.item(), temp.yhat_upper.item())
    else:
        return None


def validate_stability(
    forecast_data_dict, asofdate_range, target_date, suppress_ci=False
):
    data = {}
    for forecast_data_key in forecast_data_dict:
        dates = []
        values = []
        for asofdate in asofdate_range:
            prediction = _get_single_prediction(
                forecast_data_dict[forecast_data_key], asofdate, target_date)
            if prediction is not None:
                values.append([prediction])
                dates.append(asofdate)
        data[forecast_data_key] = pd.DataFrame({
            "date": dates,
            "Predicted MAU for {}".format(target_date): [i[0][0] for i in values],
            "upper": [i[0][1] for i in values],
            "lower": [i[0][2] for i in values],
        })
    return plot(
        {
            "data": sum(([
                go.Scatter(
                    x=data[forecast_data_key]['date'],
                    y=data[forecast_data_key]["Predicted MAU for {}".format(target_date)],
                    name="Prediction for {}".format(forecast_data_key),
                ),
                go.Scatter(
                    x=data[forecast_data_key]['date'],
                    y=data[forecast_data_key]['upper'],
                    fill='tonexty',
                    mode='none',
                    name='upper 80% CI for {}'.format(forecast_data_key),
                ) if not suppress_ci else go.Scatter(),
                go.Scatter(
                    x=data[forecast_data_key]['date'],
                    y=data[forecast_data_key]['lower'],
                    fill='tonexty',
                    mode='none',
                    name='lower 80% CI for {}'.format(forecast_data_key),
                ) if not suppress_ci else go.Scatter(),
            ] for forecast_data_key in forecast_data_dict), []),
            "layout": get_layout(
                ("Predictions of MAU for {} using model "
                 "fit on data up to each date").format(target_date),
                "Model Trained On Data Up To Date",
                "Prediction for {}".format(target_date)
            )
        },
        output_type="div"
    )


def _get_metric_for_range(actual_data, forecast_data, asofdate, metric):
    forecast = forecast_data.query("asofdate == @asofdate & ds > @asofdate")
    matched = match_dates(
        actual_data,
        forecast
    )
    metric = metric(
        np.array(matched.y),
        np.array(matched.yhat),
    )
    return metric


def ValidateMetric(actual_data, forecast_data_dict, asofdate_range, metric, metric_name):
    """
    Produce a plot of an evaluation metric for a set of foreasting models over a
    range of model dates.
    """
    data = {}
    for k in forecast_data_dict:
        dates = []
        mapes = []
        for d in asofdate_range:
            mapes.append(
                _get_metric_for_range(
                    actual_data, forecast_data_dict[k], d, metric
                )
            )
            dates.append(d)
        data[k] = pd.DataFrame({"date": dates, metric_name: mapes})
    return plot(
        {
            "data": [go.Scatter(
                x=data[k]['date'],
                y=data[k][metric_name],
                name="{} for {}".format(metric_name, k)
            ) for k in forecast_data_dict],
            "layout": get_layout(
                "{} for model trained up to date".format(metric_name),
                "Model Trained On Data Up To Date",
                "{} Metric Value".format(metric_name)
            )
        },
        output_type="div"
    )


def _get_metric_trace(model, data, training_end_date, metric, metric_name):
    forecast_start = training_end_date + timedelta(days=1)
    forecast_end = data.ds.max()
    model.fit(data.query("ds <= @training_end_date"))
    forecast_period = pd.DataFrame({'ds': pd.date_range(forecast_start, forecast_end)})
    forecast = model.predict(forecast_period)
    matched = match_dates(
        data,
        forecast
    )
    return pd.DataFrame({
        "ds": matched.ds,
        metric_name: [
            metric(
                np.array(matched.query("ds == @d").y),
                np.array(matched.query("ds == @d").yhat)
            )
            for d in matched.ds
        ]
    })


def validate_traces(model_gen, data, training_end_date_range, metric, metric_name):
    """
    Produce a plot of model traces over time, with a seperate trace for a range
    of model dates.
    """
    traces = []
    for d in training_end_date_range:
        traces.append(_get_metric_trace(model_gen(), data, d, metric, metric_name))
    return plot(
        {
            "data":
                [
                    go.Scatter(x=d['ds'], y=d[metric_name])
                    for d in traces
                ],
                "layout": get_layout(
                    "Model Traces of {}".format(metric_name),
                    "Prediction Date",
                    "{} Metric Value".format(metric_name)
                )
        },
        output_type="div",
    )


def _accumulate_horizon_metrics(
    actual_data, forecast_data, asofdate, metric, metric_values
):
    forecast = forecast_data.query("asofdate == @asofdate & ds > @asofdate")
    matched = match_dates(
        actual_data,
        forecast
    )
    for d in matched.ds:
        metricValue = metric(
            np.array(matched.query("ds == @d").y),
            np.array(matched.query("ds == @d").yhat)
        )
        horizon = (d - asofdate).days
        metric_values[horizon].append(metricValue)


def validate_metric_horizon(
    actual_data, forecast_data_dict, training_end_date_range, metric, metric_name
):
    """
    Produce a plot of an evaluation metric for a set of foreasting models over a
    range of model horizons.
    """
    data = {}
    for k in forecast_data_dict:
        metricValues = defaultdict(lambda: [])
        for d in training_end_date_range:
            _accumulate_horizon_metrics(
                actual_data, forecast_data_dict[k], d, metric, metricValues
            )
        data[k] = pd.DataFrame({
            "horizon": [i for i in metricValues.keys()],
            metric_name: [np.mean(metricValues[h]) for h in metricValues.keys()]
        })
    return plot({
        "data": [
            go.Scatter(
                x=data[k]['horizon'],
                y=data[k][metric_name],
                name="{} for {}".format(metric_name, k)
            )
            for k in forecast_data_dict
        ],
        "layout": get_layout(
            "{} Metric By Model Horizon".format(metric_name),
            "Model Horizon",
            "Mean {} Metric Value".format(metric_name)
        )
    }, output_type="div")
