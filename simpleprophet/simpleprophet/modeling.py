# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tools for model development and evaluation.
"""
from plotly.offline import plot
import plotly.graph_objs as go
from fbprophet.plot import add_changepoints_to_plot

from simpleprophet.utils import calc_mape


def evaluate_model(model, data, end_date=None, title=None):
    model.fit(data["training"])
    holdout_period = model.make_future_dataframe(
        periods=len(data["holdout"]), include_history=False
    )
    holdout_forecast = model.predict(holdout_period)
    if end_date is None:
        periods = len(data["holdout"])
    else:
        periods = (end_date - data["training"].ds.max()).days
    all_period = model.make_future_dataframe(periods=periods, include_history=True)
    all_forecast = model.predict(all_period)
    text = '{}Holdout MAPE: {:,.2f}%'.format(
        "{} - ".format(title) if title is not None else "",
        calc_mape(data["holdout"].y, holdout_forecast.yhat)
    )
    plot_html = plot({"data": [
        go.Scatter(x=data["all"]['ds'], y=data["all"]['y'], name='y'),
        go.Scatter(x=all_forecast['ds'], y=all_forecast['yhat'], name='yhat'),
        go.Scatter(
            x=all_forecast['ds'], y=all_forecast['yhat_upper'], fill='tonexty',
            mode='none', name='upper'
        ),
        go.Scatter(
            x=all_forecast['ds'], y=all_forecast['yhat_lower'], fill='tonexty',
            mode='none', name='lower'
        ),
    ], "layout": go.Layout(title=text)}, output_type='div')
    plot_prophet = model.plot(all_forecast)
    add_changepoints_to_plot(plot_prophet.gca(), model, all_forecast)
    plot_components = model.plot_components(all_forecast)
    return {
        "plot": plot_html,
        "prophetplot": plot_prophet,
        "plot_components": plot_components
    }
