# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pandas as pd
import numpy as np
from datetime import timedelta
from collections import defaultdict
from plotly.offline import plot
import plotly.graph_objs as go
from forecasting.simpleprophet.utils import matchDates, getLayout


def _getSinglePrediction(forecastData, asofdate, targetDate):
    temp = forecastData.query("asofdate == @asofdate & ds == @targetDate")
    if len(temp.yhat) == 1:
        return (temp.yhat.item(), temp.yhat_lower.item(), temp.yhat_upper.item())
    else:
        return None


def ValidateStability(forecastDataDict, asofdateRange, targetDate, suppressCI=False):
    data = {}
    for forecastDataKey in forecastDataDict:
        dates = []
        values = []
        for asofdate in asofdateRange:
            prediction = _getSinglePrediction(
                forecastDataDict[forecastDataKey], asofdate, targetDate)
            if prediction is not None:
                values.append([prediction])
                dates.append(asofdate)
        data[forecastDataKey] = pd.DataFrame({
            "date": dates,
            "Predicted MAU for {}".format(targetDate): [i[0][0] for i in values],
            "upper": [i[0][1] for i in values],
            "lower": [i[0][2] for i in values],
        })
    return plot(
        {
            "data": sum(([
                go.Scatter(
                    x=data[forecastDataKey]['date'],
                    y=data[forecastDataKey]["Predicted MAU for {}".format(targetDate)],
                    name="Prediction for {}".format(forecastDataKey),
                ),
                go.Scatter(
                    x=data[forecastDataKey]['date'],
                    y=data[forecastDataKey]['upper'],
                    fill='tonexty',
                    mode='none',
                    name='upper 80% CI for {}'.format(forecastDataKey),
                ) if not suppressCI else go.Scatter(),
                go.Scatter(
                    x=data[forecastDataKey]['date'],
                    y=data[forecastDataKey]['lower'],
                    fill='tonexty',
                    mode='none',
                    name='lower 80% CI for {}'.format(forecastDataKey),
                ) if not suppressCI else go.Scatter(),
            ] for forecastDataKey in forecastDataDict), []),
            "layout": getLayout(
                ("Predictions of MAU for {} using model "
                 "fit on data up to each date").format(targetDate),
                "Model Trained On Data Up To Date",
                "Prediction for {}".format(targetDate)
            )
        },
        output_type="div"
    )


def _getMetricForRange(actualData, forecastData, asofdate, metric):
    forecast = forecastData.query("asofdate == @asofdate & ds > @asofdate")
    matched = matchDates(
        actualData,
        forecast
    )
    metric = metric(
        np.array(matched.y),
        np.array(matched.yhat),
    )
    return metric


def ValidateMetric(actualData, forecastDataDict, asofdateRange, metric, metricName):
    data = {}
    for k in forecastDataDict:
        dates = []
        mapes = []
        for d in asofdateRange:
            mapes.append(_getMetricForRange(actualData, forecastDataDict[k], d, metric))
            dates.append(d)
        data[k] = pd.DataFrame({"date": dates, metricName: mapes})
    return plot(
        {
            "data": [go.Scatter(
                x=data[k]['date'],
                y=data[k][metricName],
                name="{} for {}".format(metricName, k)
            ) for k in forecastDataDict],
            "layout": getLayout(
                "{} for model trained up to date".format(metricName),
                "Model Trained On Data Up To Date",
                "{} Metric Value".format(metricName)
            )
        },
        output_type="div"
    )


def _getMetricTrace(model, data, trainingEndDate, metric, metricName):
    forecastStart = trainingEndDate + timedelta(days=1)
    forecastEnd = data.ds.max()
    model.fit(data.query("ds <= @trainingEndDate"))
    forecastPeriod = pd.DataFrame({'ds': pd.date_range(forecastStart, forecastEnd)})
    forecast = model.predict(forecastPeriod)
    matched = matchDates(
        data,
        forecast
    )
    return pd.DataFrame({
        "ds": matched.ds,
        metricName: [
            metric(
                np.array(matched.query("ds == @d").y),
                np.array(matched.query("ds == @d").yhat)
            )
            for d in matched.ds
        ]
    })


def ValidateTraces(modelGen, data, trainingEndDateRange, metric, metricName):
    traces = []
    for d in trainingEndDateRange:
        traces.append(_getMetricTrace(modelGen(), data, d, metric, metricName))
    return plot(
        {
            "data":
                [
                    go.Scatter(x=d['ds'], y=d[metricName])
                    for d in traces
                ],
                "layout": getLayout(
                    "Model Traces of {}".format(metricName),
                    "Prediction Date",
                    "{} Metric Value".format(metricName)
                )
        },
        output_type="div",
    )


def _accumulateHorizonMetrics(actualData, forecastData, asofdate, metric, metricValues):
    forecast = forecastData.query("asofdate == @asofdate & ds > @asofdate")
    matched = matchDates(
        actualData,
        forecast
    )
    for d in matched.ds:
        metricValue = metric(
            np.array(matched.query("ds == @d").y),
            np.array(matched.query("ds == @d").yhat)
        )
        horizon = (d - asofdate).days
        metricValues[horizon].append(metricValue)


def ValidateMetricHorizon(
    actualData, forecastDataDict, trainingEndDateRange, metric, metricName
):
    data = {}
    for k in forecastDataDict:
        metricValues = defaultdict(lambda: [])
        for d in trainingEndDateRange:
            _accumulateHorizonMetrics(
                actualData, forecastDataDict[k], d, metric, metricValues
            )
        data[k] = pd.DataFrame({
            "horizon": [i for i in metricValues.keys()],
            metricName: [np.mean(metricValues[h]) for h in metricValues.keys()]
        })
    return plot({
        "data": [
            go.Scatter(
                x=data[k]['horizon'],
                y=data[k][metricName],
                name="{} for {}".format(metricName, k)
            )
            for k in forecastDataDict
        ],
        "layout": getLayout(
            "{} Metric By Model Horizon".format(metricName),
            "Model Horizon",
            "Mean {} Metric Value".format(metricName)
        )
    }, output_type="div")
