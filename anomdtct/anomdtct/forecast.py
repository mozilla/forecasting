# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import pandas as pd
import pickle
from fbprophet import Prophet
from anomdtct.utils import s2d


# The only holidays we have identified the need to explicitly model are Chinese
# New Year and Holi
chinese_new_year = pd.DataFrame({
    'ds': [
        s2d("2016-02-08"), s2d("2017-01-28"), s2d("2018-02-16"),
        s2d("2019-02-05"), s2d("2020-01-25")
    ],
    'holiday': "chinese_new_year",
    'lower_window': -20,
    'upper_window': 20,
})


holi = pd.DataFrame({
    'ds': [
        s2d("2016-03-06"), s2d("2017-03-13"), s2d("2018-03-02"),
        s2d("2019-03-21"), s2d("2020-03-10")
    ],
    'holiday': "holi",
    'lower_window': -1,
    'upper_window': 1,
})


easter_west = pd.DataFrame({
    'ds': [
        s2d("2016-03-27"), s2d("2017-04-16"), s2d("2018-04-01"),
        s2d("2019-04-21"), s2d("2020-04-12")
    ],
    'holiday': "easter_west",
    'lower_window': -6,
    'upper_window': 1,
})


easter_east = pd.DataFrame({
    'ds': [
        s2d("2016-05-01"), s2d("2017-04-16"), s2d("2018-04-08"),
        s2d("2019-04-28"), s2d("2020-04-19")
    ],
    'holiday': "easter_east",
    'lower_window': -6,
    'upper_window': 1,
})


nowruz = pd.DataFrame({
    'ds': [
        s2d("2016-03-20"), s2d("2017-03-21"), s2d("2018-03-20"),
        s2d("2019-03-20"), s2d("2020-03-19")
    ],
    'holiday': "nowruz",
    'lower_window': -11,
    'upper_window': 6,
})


islamic_republic_day = pd.DataFrame({
    'ds': [
        s2d("2016-03-31"), s2d("2017-04-01"), s2d("2018-04-01"),
        s2d("2019-04-01"), s2d("2020-03-31")
    ],
    'holiday': "islamic_republic_day",
    'lower_window': -6,
    'upper_window': 3,
})


def forecast(data, bq_client, model_cache_table, metric):
    forecast = {}
    for c in data.keys():
        model = get_cached_model(bq_client, metric, c, model_cache_table)

        forecast_period = model.make_future_dataframe(
            periods=1,
            include_history=False
        )

        forecast[c] = model.predict(forecast_period)
        forecast[c]['ds'] = pd.to_datetime(forecast[c]['ds']).dt.date
        forecast[c] = forecast[c][["ds", "yhat", "yhat_lower", "yhat_upper"]]
        # We join the forecast with our full data to allow calculation of deviations.
        forecast[c] = forecast[c].merge(data[c], on="ds", how="inner")
        forecast[c]["delta"] = (forecast[c].y - forecast[c].yhat) / forecast[c].y
        forecast[c]["ci_delta"] = 0
        forecast[c].loc[
            forecast[c].delta > 0, "ci_delta"
        ] = (
            forecast[c].y - forecast[c].yhat
        ) / (
            forecast[c].yhat_upper - forecast[c].yhat
        )
        forecast[c].loc[
            forecast[c].delta < 0, "ci_delta"
        ] = (
            forecast[c].y - forecast[c].yhat
        ) / (
            forecast[c].yhat - forecast[c].yhat_lower
        )
        print("Done with {}".format(c))
    return forecast


def fit_model(training_data, geo):
    print("Starting with {}".format(geo))
    # We use a mostly "default" model as we find it to be highly robust and
    # do not have resources to individually model each geo.  The only
    # adjustments made are the holidays sspecified and multiplicative
    # seasonality that is more appropriate, especially for regions in which
    # Firefox usage has grown from near-zero in our training period.
    model = Prophet(
        holidays=pd.concat([
            chinese_new_year, holi, easter_west, easter_east,
            nowruz, islamic_republic_day,
        ], ignore_index=True),
        seasonality_mode='multiplicative'
    )
    model.fit(training_data[geo])
    
    return pickle.dumps(model)


def get_cached_model(bq_client, metric, geo, model_cache_table):
    query = f'''
      SELECT
        model
      FROM {model_cache_table}
      WHERE
        metric IS LIKE {metric} AND
        geography IS LIKE {geo}
    '''

    result = list(bq_client.query(query).result())

    if len(result) >= 1:
      return pickle.loads(result[0]["model"])
    else:
      return None
