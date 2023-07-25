# sw-cf-bq-gr-dash-gen
more complex processing to find the time window
the query is now working but we need to create the CTE temp table

WITH time_series AS (
  SELECT TIMESTAMP_TRUNC(t, HOUR) as ts_datetime
  FROM UNNEST(GENERATE_TIMESTAMP_ARRAY((SELECT MIN(TIMESTAMP(datetime)) FROM `acep-ext-eielson-2021.2022_11_11.vtndpp`), (SELECT MAX(TIMESTAMP(datetime)) FROM `acep-ext-eielson-2021.2022_11_11.vtndpp`), INTERVAL 1 HOUR)) t
),
data AS (
  SELECT
    TIMESTAMP_TRUNC(TIMESTAMP(datetime), HOUR) as data_datetime,
    COUNTIF(measurement_name = 'DT1-B_VAB') AS DT1_B_VAB_count,
    COUNTIF(measurement_name = 'DT1-B_VBC') AS DT1_B_VBC_count,
    COUNTIF(measurement_name = 'DT1-B_VCA') AS DT1_B_VCA_count
  FROM `acep-ext-eielson-2021.2022_11_11.vtndpp`
  GROUP BY 1
)
SELECT *
FROM time_series ts
LEFT JOIN data d
ON ts.ts_datetime = d.data_datetime
WHERE DT1_B_VAB_count >= 10
AND DT1_B_VBC_count >= 10
AND DT1_B_VCA_count >= 10
ORDER BY ts.ts_datetime ASC
LIMIT 1