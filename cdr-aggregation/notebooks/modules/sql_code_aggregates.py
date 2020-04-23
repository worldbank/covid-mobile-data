# Databricks notebook source
def write_sql_code(calls = 'calls',
                   start_date = "\'2020-02-01\'",
                   end_date =  "\'2020-03-31\'",
                   start_date_weeks =  "\'2020-02-03\'",
                   end_date_weeks = "\'2020-03-29\'"):

  sql_code = {
    # Aggregate 1 (April 1 version)
    'count_unique_subscribers_per_region_per_day' :
    """
      SELECT * FROM (
          SELECT calls.call_date AS visit_date,
              cells.region AS region,
              count(DISTINCT msisdn) AS subscriber_count
          FROM calls
          INNER JOIN cells
              ON calls.location_id = cells.cell_id
          WHERE calls.call_date >= {}
              AND calls.call_date <= CURRENT_DATE
          GROUP BY 1, 2
      ) AS grouped
      WHERE grouped.subscriber_count >= 15
      """.format(start_date),

    # Intermediate Result - Home location
    'home_locations' :
    """
      SELECT msisdn, region FROM (
          SELECT
              msisdn,
              region,
              row_number() OVER (
                  PARTITION BY msisdn
                  ORDER BY total DESC, latest_date DESC
              ) AS daily_location_rank
          FROM (

              SELECT msisdn,
                  region,
                  count(*) AS total,
                  max(call_date) AS latest_date
              FROM (
                  SELECT calls.msisdn,
                      cells.region,
                      calls.call_date,
                      row_number() OVER (
                          PARTITION BY calls.msisdn, calls.call_date
                          ORDER BY calls.call_datetime DESC
                      ) AS event_rank
                  FROM calls
                  INNER JOIN cells
                      ON calls.location_id = cells.cell_id
                  WHERE calls.call_date >= {}
                      AND calls.call_date <= {}

              ) ranked_events

              WHERE event_rank = 1
              GROUP BY 1, 2

          ) times_visited
      ) ranked_locations
      WHERE daily_location_rank = 1
      """.format(start_date, end_date),

    # Aggregate 2 (April 1 version)
    'count_unique_active_residents_per_region_per_day' :
    """
      SELECT * FROM (
          SELECT calls.call_date AS visit_date,
              cells.region AS region,
              count(DISTINCT calls.msisdn) AS subscriber_count
          FROM calls
          INNER JOIN cells
              ON calls.location_id = cells.cell_id
          INNER JOIN home_locations homes     -- See intermediate_queries.sql for code to create the home_locations table
              ON calls.msisdn = homes.msisdn
              AND cells.region = homes.region
          GROUP BY 1, 2
      ) AS grouped
      WHERE grouped.subscriber_count >= 15""",

    'count_unique_visitors_per_region_per_day' :
    """
      SELECT * FROM (
          SELECT all_visits.visit_date,
              all_visits.region,
              all_visits.subscriber_count - coalesce(home_visits.subscriber_count, 0) AS subscriber_count
          FROM count_unique_subscribers_per_region_per_day all_visits
          LEFT JOIN count_unique_active_residents_per_region_per_day home_visits
              ON all_visits.visit_date = home_visits.visit_date
              AND all_visits.region = home_visits.region
      ) AS visitors
      WHERE visitors.subscriber_count >= 15""",

    # Aggregate 3 (April 1 version)
    'count_unique_subscribers_per_region_per_week' :
    """
      SELECT * FROM (
          SELECT extract(WEEK FROM calls.call_date) AS visit_week,
              cells.region AS region,
              count(DISTINCT calls.msisdn) AS subscriber_count
          FROM calls
          INNER JOIN cells
              ON calls.location_id = cells.cell_id
          WHERE calls.call_date >= {}
              AND calls.call_date <= {}
          GROUP BY 1, 2
      ) AS grouped
      WHERE grouped.subscriber_count >= 15
      """.format(start_date_weeks, end_date_weeks),

    # Aggregate 4 (April 1 version)
    'count_unique_active_residents_per_region_per_week' :
    """
    SELECT * FROM (
          SELECT extract(WEEK FROM calls.call_date) AS visit_week,
              cells.region AS region,
              count(DISTINCT calls.msisdn) AS subscriber_count
          FROM calls
          INNER JOIN cells
              ON calls.location_id = cells.cell_id
          INNER JOIN home_locations homes     -- See intermediate_queries.sql for code to create the home_locations table
              ON calls.msisdn = homes.msisdn
              AND cells.region = homes.region
          WHERE calls.call_date >= {}
              AND calls.call_date <= {}
          GROUP BY 1, 2
      ) AS grouped
      WHERE grouped.subscriber_count >= 15
      """.format(start_date_weeks, end_date_weeks),

    'count_unique_visitors_per_region_per_week' :
    """
    SELECT * FROM (
          SELECT all_visits.visit_week,
              all_visits.region,
              all_visits.subscriber_count - coalesce(home_visits.subscriber_count, 0) AS subscriber_count
          FROM count_unique_subscribers_per_region_per_week all_visits
          LEFT JOIN count_unique_active_residents_per_region_per_week home_visits
              ON all_visits.visit_week = home_visits.visit_week
              AND all_visits.region = home_visits.region
      ) AS visitors
      WHERE visitors.subscriber_count >= 15""",

    # Aggregate 5 (April 1 version)
    'regional_pair_connections_per_day' :
    """
    SELECT * FROM (
          SELECT connection_date,
              region1,
              region2,
              count(*) AS subscriber_count
          FROM (

              SELECT t1.call_date AS connection_date,
                  t1.msisdn AS msisdn,
                  t1.region AS region1,
                  t2.region AS region2
              FROM (
                  SELECT DISTINCT calls.msisdn,
                      calls.call_date,
                      cells.region
                  FROM calls
                  INNER JOIN cells
                      ON calls.location_id = cells.cell_id
                  WHERE calls.call_date >= {}
                      AND calls.call_date <= CURRENT_DATE
                  ) t1

                  FULL OUTER JOIN

                  (
                  SELECT DISTINCT calls.msisdn,
                      calls.call_date,
                      cells.region
                  FROM calls
                  INNER JOIN cells
                      ON calls.location_id = cells.cell_id
                  WHERE calls.call_date >= {}
                      AND calls.call_date <= CURRENT_DATE
                  ) t2

                  ON t1.msisdn = t2.msisdn
                  AND t1.call_date = t2.call_date
              WHERE t1.region < t2.region

          ) AS pair_connections
          GROUP BY 1, 2, 3
      ) AS grouped
      WHERE grouped.subscriber_count >= 15
      """.format(start_date, start_date),

    # Aggregate 6 (April 2 version)
    'directed_regional_pair_connections_per_day' :
    """
      WITH subscriber_locations AS (
          SELECT calls.msisdn,
              calls.call_date,
              cells.region,
              min(calls.call_datetime) AS earliest_visit,
              max(calls.call_datetime) AS latest_visit
          FROM calls
          INNER JOIN cells
              ON calls.location_id = cells.cell_id
          WHERE calls.call_date >= {}
              AND calls.call_date <= CURRENT_DATE
          GROUP BY msisdn, call_date, region
      )
      SELECT * FROM (
          SELECT connection_date,
              region_from,
              region_to,
              count(*) AS subscriber_count
          FROM (

              SELECT t1.call_date AS connection_date,
                  t1.msisdn AS msisdn,
                  t1.region AS region_from,
                  t2.region AS region_to
              FROM subscriber_locations t1
              FULL OUTER JOIN subscriber_locations t2
              ON t1.msisdn = t2.msisdn
                  AND t1.call_date = t2.call_date
              WHERE t1.region <> t2.region
                  AND t1.earliest_visit < t2.latest_visit

          ) AS pair_connections
          GROUP BY 1, 2, 3
      ) AS grouped
      WHERE grouped.subscriber_count >= 15
      """.format(start_date),

    # Aggregate 7 (April 3 version)
    'total_calls_per_region_per_day' :
    """
      SELECT
          call_date,
          region,
          total_calls
      FROM (
          SELECT calls.call_date AS call_date,
              cells.region AS region,
              count(DISTINCT msisdn) AS subscriber_count,
              count(*) AS total_calls
          FROM calls
          INNER JOIN cells
              ON calls.location_id = cells.cell_id
          WHERE calls.call_date >= {}
              AND calls.call_date <= CURRENT_DATE
          GROUP BY 1, 2
      ) AS grouped
      WHERE grouped.subscriber_count >= 15
      """.format(start_date),

    # Aggregate 8 (April 3 version)
    'home_location_counts_per_region' :
    """
      SELECT * FROM (
          SELECT region, count(msisdn) AS subscriber_count
          FROM home_locations     -- See intermediate_queries.sql for code to create the home_locations table
          GROUP BY region
      ) AS home_counts
      WHERE home_counts.subscriber_count >= 15"""}
  return sql_code
