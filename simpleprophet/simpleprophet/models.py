# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
The current models and training data specification.
"""
from fbprophet import Prophet
import pandas as pd
from datetime import date, timedelta
from simpleprophet.utils import s2d


# Get easter dates
def get_easters(year):
    a = year % 19
    b = year // 100
    c = year % 100
    d = (19 * a + b - b // 4 - ((b - (b + 8) // 25 + 1) // 3) + 15) % 30
    e = (32 + 2 * (b % 4) + 2 * (c // 4) - d - (c % 4)) % 7
    f = d + e - 7 * ((a + 11 * d + 22 * e) // 451) + 114
    month = f // 31
    day = f % 31 + 1
    easter = date(year, month, day)
    return [("easter{}".format(x), easter + timedelta(days=x)) for x in range(-3, 2)]


# Get holidays dataframe in prophet's format
def get_holidays(years):
    easters = pd.DataFrame({
        'ds': [e[1] for i in years for e in get_easters(i)],
        'holiday': [e[0] for i in years for e in get_easters(i)],
        'lower_window': 0,
        'upper_window': 0,
    })
    return easters


def setup_models(years):
    models = {}
    models["desktop_global"] = Prophet(
        yearly_seasonality=20,
        changepoint_range=0.7,
        seasonality_mode='multiplicative',
        changepoint_prior_scale=0.015,
        seasonality_prior_scale=0.25,
        holidays=get_holidays(years)
    )
    models["nondesktop_global"] = Prophet()
    models["fxa_global"] = Prophet(
        changepoint_range=0.8,
        changepoint_prior_scale=0.02,
    )
    models["desktop_tier1"] = Prophet()
    models["nondesktop_tier1"] = Prophet()
    models["fxa_tier1"] = Prophet(
        changepoint_range=0.8,
        changepoint_prior_scale=0.02,
    )
    models["Fennec Android"] = Prophet(
        changepoint_prior_scale=0.0005,
        seasonality_prior_scale=0.001,
        seasonality_mode='multiplicative'
    )
    models["Focus iOS"] = Prophet(changepoint_prior_scale=0.0005)
    models["Focus Android"] = Prophet(changepoint_prior_scale=0.005)
    models["Fennec iOS"] = Prophet(
        changepoint_prior_scale=0.005,
        seasonality_prior_scale=0.001,
        seasonality_mode='multiplicative'
    )
    models["Fenix"] = Prophet(changepoint_prior_scale=0.0005)
    models["Firefox Lite"] = Prophet(changepoint_prior_scale=0.0005)
    models["FirefoxForFireTV"] = Prophet(
        changepoint_prior_scale=0.0005,
        seasonality_prior_scale=0.005,
        seasonality_mode='multiplicative',
        yearly_seasonality=True
    )
    models["FirefoxConnect"] = Prophet(changepoint_prior_scale=0.0005)
    models["nondesktop_nofire_global"] = Prophet(
        yearly_seasonality=20,
        changepoint_range=0.75,
        seasonality_mode='multiplicative',
        changepoint_prior_scale=0.008,
        seasonality_prior_scale=0.20,
        holidays=get_holidays(years)
    )
    models["nondesktop_nofire_tier1"] = Prophet(
        yearly_seasonality=20,
        changepoint_range=0.75,
        seasonality_mode='multiplicative',
        changepoint_prior_scale=0.008,
        seasonality_prior_scale=0.20,
        holidays=get_holidays(years)
    )
    return models


def data_filter(data, product):
    start_dates = {
        "desktop_global": s2d('2016-04-08'),
        "fxa_global": s2d('2018-03-20'),
        "fxa_tier1": s2d('2018-03-20'),
        "Fennec Android": s2d('2017-03-04'),
        "Focus iOS": s2d('2017-12-06'),
        "Focus Android": s2d('2017-07-17'),
        "Fennec iOS": s2d('2017-03-03'),
        "Fenix": s2d('2019-07-03'),
        "Firefox Lite": s2d('2017-03-04'),
        "FirefoxForFireTV": s2d('2018-02-04'),
        "FirefoxConnect": s2d('2018-10-10'),
        "nondesktop_nofire_global": s2d('2017-01-30'),
        "nondesktop_nofire_tier1": s2d('2017-01-30'),
    }

    anomalyDates = {
        "desktop_global": [s2d('2019-05-16'), s2d('2019-06-07')],
        "Focus Android": [s2d('2018-09-01'), s2d('2019-03-01')],
        "Fennec iOS": [s2d('2017-11-08'), s2d('2017-12-31')],
    }
    temp = data.copy()
    if product in start_dates:
        start_date = start_dates[product]  # noqa: F841
        temp = temp.query("ds >= @start_date")
    if product in anomalyDates:
        anomalystart_date = anomalyDates[product][0]  # noqa: F841
        anomalyend_date = anomalyDates[product][1]  # noqa: F841
        temp = temp.query("(ds < @anomalystart_date) | (ds > @anomalyend_date)")
    return temp
