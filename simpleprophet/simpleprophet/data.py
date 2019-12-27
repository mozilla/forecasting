# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tools for getting metric actual or forecast data from BigQuery.
"""
import pandas as pd


KPI_QUERIES = {
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
    "mobile": '''
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


def get_kpi_data(bq_client, types=list(KPI_QUERIES.keys())):
    data = {}
    if not isinstance(types, list):
        types = [types]
    for q in types:
        raw_data = bq_client.query(KPI_QUERIES[q]).to_dataframe()
        data['{}_global_mau'.format(q)] = raw_data[
            ["date", "global_mau"]
        ].rename(
            index=str, columns={"date": "ds", "global_mau": "y"}
        )
        data['{}_tier1_mau'.format(q)] = raw_data[
            ["date", "tier1_mau"]
        ].rename(
            index=str, columns={"date": "ds", "tier1_mau": "y"}
        )
    for k in data:
        data[k]['ds'] = pd.to_datetime(data[k]['ds']).dt.date
    return data


NONDESKTOP_QUERY = '''
    SELECT
        submission_date as date,
        SUM(mau) AS global_mau,
        SUM(tier1_mau) AS tier1_mau,
        product
    FROM
        `moz-fx-data-derived-datasets.telemetry.firefox_nondesktop_exact_mau28_by_product_v1`
    GROUP BY
        submission_date,
        product
    ORDER BY
        date
    '''


def get_nondesktop_data(bq_client):
    data = {}
    raw_data = bq_client.query(NONDESKTOP_QUERY).to_dataframe()
    for p in [
        "Fennec Android", "Focus iOS", "Focus Android", "Fennec iOS", "Fenix",
        "Firefox Lite", "FirefoxForFireTV", "FirefoxConnect", "Lockwise Android"
    ]:
        data['{} MAU'.format(p)] = raw_data.query("product == @p")[
            ["date", "global_mau"]
        ].rename(
            index=str, columns={"date": "ds", "global_mau": "y"}
        )
        data['{} tier1 MAU'.format(p)] = raw_data.query("product == @p")[
            ["date", "tier1_mau"]
        ].rename(
            index=str, columns={"date": "ds", "tier1_mau": "y"}
        )
    for k in data:
        data[k]['ds'] = pd.to_datetime(data[k]['ds']).dt.date
    return data


FORECAST_QUERY = '''
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
        AND asofdate BETWEEN "{asofdate_start}" AND "{asofdate_end}"
    ORDER BY
        date
    '''


def get_forecast_data(
    bq_client, project, dataset, table, product, asofdate_start, asofdate_end
):
    raw_data = bq_client.query(FORECAST_QUERY.format(
        project=project,
        dataset=dataset,
        table=table,
        product=product,
        asofdate_start=asofdate_start,
        asofdate_end=asofdate_end,
    )).to_dataframe().rename(columns={
        "date": "ds",
        "mau": "yhat",
        "high90": "yhat_upper",
        "low90": "yhat_lower",
    })
    raw_data['ds'] = pd.to_datetime(raw_data['ds']).dt.date
    return raw_data
