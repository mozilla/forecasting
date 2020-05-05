# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import pandas as pd
import pickle


# Note we obtain the most recent geo for each profile, as updates to the IPGeo
# database that we use create excessive noise.  Also note that we restrict to
# the top 1000 cities according to DAU on an arbitrary day.
QUERIES = {
    "light_funnel_dau_city": '''
        WITH top_cities_t AS (
          SELECT
            COUNT(client_id) AS dau,
            CONCAT(
              country, ":",
              geo_subdivision1, ":",
              geo_subdivision2, ":",
              city
            ) AS geo
          FROM `moz-fx-data-shared-prod.telemetry.clients_daily`
          WHERE
            submission_date = "2020-03-01"
            AND city != "??"
          GROUP BY
            country,
            geo_subdivision1,
            geo_subdivision2,
            city
          ORDER BY dau DESC
          LIMIT 1000
        ),
        geo_t AS (
          SELECT
            client_id,
            geo
          FROM (
            SELECT
              client_id,
              CONCAT(
                country, ":",
                geo_subdivision1, ":",
                geo_subdivision2, ":",
                cd_t.city
              ) AS geo
            FROM `moz-fx-data-shared-prod.telemetry.clients_first_seen` AS cd_t
            INNER JOIN (SELECT geo FROM top_cities_t) AS top_cities_t
            ON CONCAT(
                country, ":",
                geo_subdivision1, ":",
                geo_subdivision2, ":",
                city
              ) = top_cities_t.geo
          )
        )
        SELECT
          submission_date AS date,
          COUNT(client_id) AS value,
          COUNT(client_id) AS dau,
          geo_t.geo AS geo
        FROM (
            SELECT
              client_id,
              submission_date,
            FROM `moz-fx-data-shared-prod.telemetry.clients_daily`
            WHERE
              submission_date BETWEEN '{start_date}' AND '{end_date}'
              AND attribution.content IS NOT NULL
          )
        INNER JOIN geo_t
        USING(client_id)
        GROUP BY geo, submission_date
        ''',
    "light_funnel_dau_country": '''
        WITH geo_t AS (
          SELECT
            client_id,
            geo
          FROM (
            SELECT
              client_id,
              country AS geo,
            FROM `moz-fx-data-shared-prod.telemetry.clients_first_seen` AS cd_t
          )
        )
        SELECT
          submission_date AS date,
          COUNT(client_id) AS value,
          COUNT(client_id) AS dau,
          geo_t.geo AS geo
        FROM (
            SELECT
              client_id,
              submission_date,
            FROM `moz-fx-data-shared-prod.telemetry.clients_daily`
            WHERE
              submission_date BETWEEN '{start_date}' AND '{end_date}'
              AND attribution.content IS NOT NULL
          )
        INNER JOIN geo_t
        USING(client_id)
        GROUP BY geo, submission_date
        ''',
    "light_funnel_mean_active_hours_per_profile_city": '''
        WITH top_cities_t AS (
          SELECT
            COUNT(client_id) AS dau,
            CONCAT(
              country, ":",
              geo_subdivision1, ":",
              geo_subdivision2, ":",
              city
            ) AS geo
          FROM `moz-fx-data-shared-prod.telemetry.clients_daily`
          WHERE
            submission_date = "2020-03-01"
            AND city != "??"
          GROUP BY
            country,
            geo_subdivision1,
            geo_subdivision2,
            city
          ORDER BY dau DESC
          LIMIT 1000
        ),
        geo_t AS (
          SELECT
            client_id,
            geo
          FROM (
            SELECT
              client_id,
              CONCAT(
                country, ":",
                geo_subdivision1, ":",
                geo_subdivision2, ":",
                cd_t.city
              ) AS geo
            FROM `moz-fx-data-shared-prod.telemetry.clients_first_seen` AS cd_t
            INNER JOIN (SELECT geo FROM top_cities_t) AS top_cities_t
            ON CONCAT(
                country, ":",
                geo_subdivision1, ":",
                geo_subdivision2, ":",
                city
              ) = top_cities_t.geo
          )
        )
        SELECT
          submission_date AS date,
          AVG(active_hours_sum) AS value,
          COUNT(client_id) AS dau,
          geo_t.geo AS geo
        FROM (
            SELECT
              active_hours_sum,
              client_id,
              submission_date,
            FROM `moz-fx-data-shared-prod.telemetry.clients_daily`
            WHERE
              submission_date BETWEEN '{start_date}' AND '{end_date}'
              AND attribution.content IS NOT NULL
          )
        INNER JOIN geo_t
        USING(client_id)
        GROUP BY geo, submission_date
        ''',
    "light_funnel_mean_active_hours_per_profile_country": '''
        WITH geo_t AS (
          SELECT
            client_id,
            geo
          FROM (
            SELECT
              client_id,
              country AS geo,
            FROM `moz-fx-data-shared-prod.telemetry.clients_first_seen` AS cd_t
          )
        )
        SELECT
          submission_date AS date,
          AVG(active_hours_sum) AS value,
          COUNT(client_id) AS dau,
          geo_t.geo AS geo
        FROM (
            SELECT
              active_hours_sum,
              client_id,
              submission_date,
            FROM `moz-fx-data-shared-prod.telemetry.clients_daily`
            WHERE
              submission_date BETWEEN '{start_date}' AND '{end_date}'
              AND attribution.content IS NOT NULL
          )
        INNER JOIN geo_t
        USING(client_id)
        GROUP BY geo, submission_date
        '''
}


def get_raw_data(bq_client, bq_storage_client, metric, start_date, end_date):
    return bq_client.query(
        QUERIES[metric].format(start_date=start_date, end_date=end_date)
    ).result().to_dataframe(bqstorage_client=bq_storage_client)


def prepare_training_data(data, training_start, training_end):
    clean_data = {}
    clean_training_data = {}
    # Suppress any geoXdate with less than 5000 profiles as per minimum
    # aggregation standards for the policy this data will be released under.
    data = data[data.dau >= 5000].drop(columns=["dau"])
    for c in data.geo.unique():
        # We don't want to include a region unless we have at least about
        # two years of training data for the model
        if (len(data.query("geo==@c")) < 600):
            continue
        clean_data[c] = data.query("geo==@c").rename(
            columns={"date": "ds", "value": "y"}
        ).sort_values("ds")
        clean_data[c]['ds'] = pd.to_datetime(clean_data[c]['ds']).dt.date
        clean_data[c] = clean_data[c].set_index('ds')
        clean_training_data[c] = clean_data[c][
            training_start:training_end
        ].reset_index()
        clean_data[c] = clean_data[c].reset_index()
    return clean_training_data


def prepare_data(data):
    clean_data = {}
    for c in data.geo.unique():
        # We don't want to include a region unless we have at least about
        # two years of training data for the model
        clean_data[c] = data.query("geo==@c").rename(
            columns={"date": "ds", "value": "y"}
        ).sort_values("ds")
        clean_data[c]['ds'] = pd.to_datetime(clean_data[c]['ds']).dt.date
        clean_data[c] = clean_data[c].set_index('ds')
        clean_data[c] = clean_data[c].reset_index()
    return clean_data


def get_data_for_date(bq_client, bq_storage_client, metric, model_date):
    return bq_client.query(
        QUERIES[metric].format(start_date=model_date, end_date=model_date)
    ).result().to_dataframe(bqstorage_client=bq_storage_client)
