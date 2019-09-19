# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import timedelta

from forecasting.simpleprophet.models import setupModels, dataFilter


# Delete output table if necessary and create empty table with appropriate schema
def resetOuputTable(bigquery_client, project, dataset, table_name):
    dataset_ref = bigquery_client.dataset(dataset)
    table_ref = dataset_ref.table(table_name)
    try:
        bigquery_client.delete_table(table_ref)
    except NotFound:
        pass
    schema = [
        bigquery.SchemaField('asofdate', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('datasource', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('type', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('mau', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('low90', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('high90', 'FLOAT', mode='REQUIRED'),
    ] + [
        bigquery.SchemaField('p{}'.format(q), 'FLOAT', mode='REQUIRED')
        for q in range(10, 100, 10)
    ]
    table = bigquery.Table(table_ref, schema=schema)
    table = bigquery_client.create_table(table)
    return table


def writeForecasts(bigquery_client, table, modelDate, forecastEnd, data, product):
    minYear = data.ds.min().year
    maxYear = forecastEnd.year
    years = range(minYear, maxYear + 1)
    models = setupModels(years)
    forecastStart = modelDate + timedelta(days=1)
    forecastPeriod = pd.DataFrame({'ds': pd.date_range(forecastStart, forecastEnd)})
    data = dataFilter(data, product)
    models[product].fit(data.query("ds <= @modelDate"))
    forecastSamples = models[product].sample_posterior_predictive(
        models[product].setup_dataframe(forecastPeriod)
    )
    forecast = models[product].predict(forecastPeriod)
    outputData = {
        "asofdate": modelDate,
        "datasource": product,
        "date": forecast.ds,
        "type": "forecast",
        "mau": forecast.yhat,
        "low90": forecast.yhat_lower,
        "high90": forecast.yhat_upper,
    }
    outputData.update({
        "p{}".format(q): np.nanpercentile(forecastSamples['yhat'], q, axis=1)
        for q in range(10, 100, 10)
    })
    outputData = pd.DataFrame(outputData)[[
      "asofdate", "datasource", "date", "type", "mau", "low90", "high90",
      "p10", "p20", "p30", "p40", "p50", "p60", "p70", "p80", "p90"
    ]]
    outputData['date'] = pd.to_datetime(outputData['date']).dt.date
    errors = bigquery_client.insert_rows(
        table,
        list(outputData.itertuples(index=False, name=None))
    )
    assert errors == []
