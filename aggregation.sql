WITH date_ranges AS
  (
    SELECT
      MIN(date_trunc('day', actl_dep_lcl_tms)) AS start_date,
      MAX(date_trunc('day', actl_arr_lcl_tms)) + 1 AS end_date
    FROM "FPT Interview".main."FPT Interview"
  )
, time_intervals AS
  (
    SELECT
      dr.start_date + INTERVAL (n * 15) MINUTE AS interval_ts
    FROM date_ranges dr
    JOIN range(0, CAST(((dr.end_date) - (dr.start_date)) * 24 * 4 AS INTEGER) + 1) tbl(n)
    ON TRUE
  )
, airport_codes AS
  (
    SELECT DISTINCT orig AS port_code
    FROM "FPT Interview".main."FPT Interview"
    UNION
    SELECT DISTINCT dest
    FROM "FPT Interview".main."FPT Interview"
  )
, port_intervals AS
  (
    SELECT
      a.port_code,
      t.interval_ts
    FROM airport_codes a
    JOIN time_intervals t
    ON TRUE
  )
, rolling_aggregations AS
  (
    SELECT
      p.port_code,
      p.interval_ts,
      SUM
        (
          CASE
            WHEN p.port_code = f.orig
            AND f.actl_dep_lcl_tms BETWEEN p.interval_ts - INTERVAL '2 hours' AND p.interval_ts
            THEN 1
            ELSE 0
            END
        ) AS out_cnt,
      SUM
        (
          CASE
            WHEN p.port_code = f.dest
            AND f.actl_arr_lcl_tms BETWEEN p.interval_ts - INTERVAL '2 hours' AND p.interval_ts
            THEN 1
            ELSE 0
            END
        ) AS in_cnt
    FROM port_intervals p
    LEFT JOIN "FPT Interview".main."FPT Interview" f
    ON (p.port_code = f.orig AND f.actl_dep_lcl_tms BETWEEN p.interval_ts - INTERVAL '2 hours' AND p.interval_ts)
    OR (p.port_code = f.dest AND f.actl_arr_lcl_tms BETWEEN p.interval_ts - INTERVAL '2 hours' AND p.interval_ts)
    GROUP BY
      p.port_code,
      p.interval_ts
  )
SELECT * FROM rolling_aggregations
ORDER BY port_code, interval_ts
;
