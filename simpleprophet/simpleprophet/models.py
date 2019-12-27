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
    release_cycles = pd.DataFrame({
        "ds": [
            s2d("2018-05-09"), s2d("2018-06-26"), s2d("2018-09-05"), s2d("2018-10-23"),
            s2d("2018-12-11"), s2d("2019-01-29 "), s2d("2019-03-19"), s2d("2019-05-21"),
            s2d("2019-07-09"), s2d("2019-09-03"), s2d("2019-10-22"), s2d("2019-12-03"),
            s2d("2020-01-07"), s2d("2020-02-11"), s2d("2020-03-10"), s2d("2020-04-07"),
            s2d("2020-05-05"), s2d("2020-06-02"), s2d("2020-06-30"), s2d("2020-07-28"),
            s2d("2020-08-25"), s2d("2020-09-22"), s2d("2020-10-20"), s2d("2020-11-17"),
            s2d("2020-12-15"),
        ],
        "holiday": "release",
        "lower_window": 0,
        "upper_window": 69
    })

    monitor_pushes = pd.DataFrame({
        "ds": [
            s2d("2019-11-25")
        ],
        "holiday": "monitor_push",
        "lower_window": 0,
        "upper_window": 30
    })
    models = {}
    models["desktop_global_mau"] = Prophet(
        yearly_seasonality=20,
        changepoint_range=0.7,
        seasonality_mode='multiplicative',
        changepoint_prior_scale=0.015,
        seasonality_prior_scale=0.25,
        holidays=get_holidays(years)
    )
    models["desktop_tier1_mau"] = Prophet()  # Not validated
    models["mobile_global_mau"] = Prophet(
        yearly_seasonality=20,
        changepoint_range=0.75,
        seasonality_mode='multiplicative',
        changepoint_prior_scale=0.008,
        seasonality_prior_scale=0.0002,
        holidays=get_holidays(years)
    )
    models["mobile_tier1_mau"] = Prophet(
        yearly_seasonality=20,
        changepoint_range=0.75,
        seasonality_mode='multiplicative',
        changepoint_prior_scale=0.008,
        seasonality_prior_scale=0.0002,
        holidays=get_holidays(years)
    )
    models["fxa_global_mau"] = Prophet(
        changepoint_range=0.9,
        changepoint_prior_scale=0.02,
        seasonality_prior_scale=0.00002,
        holidays=pd.concat([release_cycles, monitor_pushes], ignore_index=True),
        seasonality_mode='multiplicative',
        yearly_seasonality=10,
    )
    models["fxa_tier1_mau"] = Prophet(
        changepoint_range=0.9,
        changepoint_prior_scale=0.02,
        seasonality_prior_scale=0.00002,
        holidays=pd.concat([release_cycles, monitor_pushes], ignore_index=True),
        seasonality_mode='multiplicative',
        yearly_seasonality=10,
    )
    models["Fennec Android MAU"] = Prophet(
        changepoint_prior_scale=0.0005,
        seasonality_prior_scale=0.001,
        seasonality_mode='multiplicative'
    )
    models["Focus iOS MAU"] = Prophet(changepoint_prior_scale=0.0005)
    models["Focus Android MAU"] = Prophet(changepoint_prior_scale=0.005)
    models["Fennec iOS MAU"] = Prophet(
        changepoint_prior_scale=0.005,
        seasonality_prior_scale=0.001,
        seasonality_mode='multiplicative'
    )
    models["Fenix MAU"] = Prophet(changepoint_prior_scale=0.0005)
    models["Firefox Lite MAU"] = Prophet(changepoint_prior_scale=0.0005)
    models["FirefoxForFireTV MAU"] = Prophet(
        changepoint_prior_scale=0.0005,
        seasonality_prior_scale=0.005,
        seasonality_mode='multiplicative',
        yearly_seasonality=True
    )
    models["FirefoxConnect MAU"] = Prophet(changepoint_prior_scale=0.0005)
    models["Lockwise Android MAU"] = Prophet(  # Not validated
        changepoint_range=0.9,
        changepoint_prior_scale=0.007,
        seasonality_mode='multiplicative',
    )
    models["Fennec Android tier1 MAU"] = Prophet(  # Not validated
        changepoint_prior_scale=0.0005,
        seasonality_prior_scale=0.001,
        seasonality_mode='multiplicative'
    )
    models["Focus iOS tier1 MAU"] = Prophet(  # Not validated
        changepoint_prior_scale=0.0005
    )
    models["Focus Android tier1 MAU"] = Prophet(  # Not validated
        changepoint_prior_scale=0.005
    )
    models["Fennec iOS tier1 MAU"] = Prophet(  # Not validated
        changepoint_prior_scale=0.005,
        seasonality_prior_scale=0.001,
        seasonality_mode='multiplicative'
    )
    models["Fenix tier1 MAU"] = Prophet(  # Not validated
        changepoint_prior_scale=0.0005
    )
    models["Firefox Lite tier1 MAU"] = Prophet(  # Not validated
        changepoint_prior_scale=0.0005
    )
    models["FirefoxForFireTV tier1 MAU"] = Prophet(  # Not validated
        changepoint_prior_scale=0.0005,
        seasonality_prior_scale=0.005,
        seasonality_mode='multiplicative',
        yearly_seasonality=True
    )
    models["FirefoxConnect tier1 MAU"] = Prophet(  # Not validated
        changepoint_prior_scale=0.0005
    )
    models["Lockwise Android tier1 MAU"] = Prophet(
        changepoint_range=0.9,
        changepoint_prior_scale=0.007,
        seasonality_mode='multiplicative',
    )

    return models


def data_filter(data, product):
    start_dates = {
        "desktop_global_mau": s2d('2016-04-08'),
        "fxa_global_mau": s2d('2018-03-20'),
        "fxa_tier1_mau": s2d('2018-03-20'),
        "Fennec Android MAU": s2d('2017-03-04'),
        "Focus iOS MAU": s2d('2017-12-06'),
        "Focus Android MAU": s2d('2017-07-17'),
        "Fennec iOS MAU": s2d('2017-03-03'),
        "Fenix MAU": s2d('2019-07-03'),
        "Firefox Lite MAU": s2d('2017-03-04'),
        "FirefoxForFireTV MAU": s2d('2018-02-04'),
        "FirefoxConnect MAU": s2d('2018-10-10'),
        "mobile_global_mau": s2d('2017-01-30'),
        "mobile_tier1_mau": s2d('2017-01-30'),
    }

    anomalyDates = {
        "desktop_global_mau": [s2d('2019-05-16'), s2d('2019-06-07')],
        "Focus Android MAU": [s2d('2018-09-01'), s2d('2019-03-01')],
        "Fennec iOS MAU": [s2d('2017-11-08'), s2d('2017-12-31')],
        "mobile_global_mau": [s2d('2017-11-10'), s2d('2018-03-11')],
        "mobile_tier1_mau": [s2d('2017-11-10'), s2d('2018-03-11')],
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
