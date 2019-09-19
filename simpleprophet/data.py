# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pandas as pd


_kpi_queries = {
    "desktop": '''
        SELECT
            submission_date as date,
            sum(mau) AS global_mau,
            SUM(IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'), mau, 0)) AS tier1_mau
        FROM
            `moz-fx-data-derived-datasets.telemetry.firefox_desktop_exact_mau28_by_dimensions_v1`
        GROUP BY
            date
        ORDER BY
            date
    ''',
    "nondesktop": '''
        SELECT
            submission_date as date,
            sum(mau) AS global_mau,
            SUM(IF(country IN ('US', 'FR', 'DE', 'GB', 'CA'), mau, 0)) AS tier1_mau
        FROM
            `moz-fx-data-derived-datasets.telemetry.firefox_nondesktop_exact_mau28_by_dimensions_v1`
        GROUP BY
            date
        ORDER BY
            date
    ''',
    "fxa": '''
        SELECT
            submission_date AS date,
            SUM(mau) AS global_mau,
            SUM(seen_in_tier1_country_mau) AS tier1_mau
        FROM
            `moz-fx-data-derived-datasets.telemetry.firefox_accounts_exact_mau28_by_dimensions_v1`
        GROUP BY
            submission_date
        ORDER BY
            submission_date
    '''
}


def getKPIData(bqClient, types=list(_kpi_queries.keys())):
    data = {}
    if not isinstance(types, list):
        types = [types]
    for q in types:
        rawData = bqClient.query(_kpi_queries[q]).to_dataframe()
        data['{}_global'.format(q)] = rawData[
            ["date", "global_mau"]
        ].rename(
            index=str, columns={"date": "ds", "global_mau": "y"}
        )
        data['{}_tier1'.format(q)] = rawData[
            ["date", "tier1_mau"]
        ].rename(
            index=str, columns={"date": "ds", "tier1_mau": "y"}
        )
    for k in data:
        data[k]['ds'] = pd.to_datetime(data[k]['ds']).dt.date
    return data


_nondesktop_query = '''
    SELECT
        submission_date as date,
        SUM(mau) AS global_mau,
        product
    FROM
        `moz-fx-data-derived-datasets.telemetry.firefox_nondesktop_exact_mau28_by_product_v1`
    GROUP BY
        submission_date,
        product
    ORDER BY
        date
    '''


def getNondesktopData(bqClient):
    data = {}
    rawData = bqClient.query(_nondesktop_query).to_dataframe()
    for p in [
        "Fennec Android", "Focus iOS", "Focus Android", "Fennec iOS", "Fenix",
        "Firefox Lite", "FirefoxForFireTV", "FirefoxConnect"
    ]:
        data['{}'.format(p)] = rawData.query("product == @p")[
            ["date", "global_mau"]
        ].rename(
            index=str, columns={"date": "ds", "global_mau": "y"}
        )
    for k in data:
        data[k]['ds'] = pd.to_datetime(data[k]['ds']).dt.date
    return data


_nondesktop_nofire_query = '''
    SELECT
        submission_date as date,
        SUM(mau) AS global_mau,
        SUM(tier1_mau) AS tier1_mau

    FROM
        `moz-fx-data-derived-datasets.telemetry.firefox_nondesktop_exact_mau28_by_product_v1`
    WHERE
        product != "FirefoxForFireTV"
    GROUP BY
        submission_date
    ORDER BY
        date
    '''


def getNondesktopNoFireData(bqClient):
    data = {}
    rawData = bqClient.query(_nondesktop_nofire_query).to_dataframe()
    data['nondesktop_nofire_global'] = rawData[
        ["date", "global_mau"]
    ].rename(
        index=str, columns={"date": "ds", "global_mau": "y"}
    )
    data['nondesktop_nofire_tier1'] = rawData[
        ["date", "tier1_mau"]
    ].rename(
        index=str, columns={"date": "ds", "tier1_mau": "y"}
    )
    for k in data:
        data[k]['ds'] = pd.to_datetime(data[k]['ds']).dt.date
    return data


_forecast_query = '''
    SELECT
        date,
        asofdate,
        mau,
        low90,
        high90
    FROM
        `{project}.{dataset}.{table}`
    WHERE
        datasource = '{product}'
        AND type = 'forecast'
        AND asofdate BETWEEN "{asofdateStart}" AND "{asofdateEnd}"
    ORDER BY
        date
    '''


def getForecastData(
    bqClient, project, dataset, table, product, asofdateStart, asofdateEnd
):
    rawData = bqClient.query(_forecast_query.format(
        project=project,
        dataset=dataset,
        table=table,
        product=product,
        asofdateStart=asofdateStart,
        asofdateEnd=asofdateEnd,
    )).to_dataframe().rename(columns={
        "date": "ds",
        "mau": "yhat",
        "high90": "yhat_upper",
        "low90": "yhat_lower",
    })
    rawData['ds'] = pd.to_datetime(rawData['ds']).dt.date
    return rawData
