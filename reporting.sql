CREATE SCHEMA IF NOT EXISTS reporting;
CREATE OR REPLACE VIEW reporting.flight AS
    SELECT *
    , CASE
        WHEN dep_delay_new > 0 THEN 1
         WHEN dep_delay_new is null THEN 0
        ELSE 0
    END AS is_delayed
FROM public.flight
WHERE cancelled = 0;
CREATE OR REPLACE VIEW reporting.top_reliability_roads AS
SELECT
    rf.origin_airport_id,
    al1.name AS origin_airport_name,
    rf.dest_airport_id,
    al2.name AS dest_airport_name,
    year,
    count(rf.id) AS cnt,
    sum(rf.is_delayed)/CAST(count(rf.is_delayed)AS FLOAT) AS reliability,
    DENSE_RANK() OVER (PARTITION BY sum(rf.is_delayed)/CAST(count(rf.is_delayed) AS FLOAT)) AS nb
FROM reporting.flight as rf
INNER JOIN airport_list AS al1  ON rf.origin_airport_id = al1.origin_airport_id
INNER JOIN airport_list AS al2 ON rf.dest_airport_id = al2.origin_airport_id
GROUP BY rf.origin_airport_id, al1.name , rf.dest_airport_id, dest_airport_name, year
HAVING count(rf.id)  > 1000
ORDER BY cnt DESC;
CREATE OR REPLACE VIEW reporting.year_to_year_comparision AS
SELECT
    year,
    month,
    count(id) AS flights_amount,
    sum(is_delayed)/CAST(count(is_delayed) AS FLOAT) AS reliability
FROM reporting.flight
GROUP BY year, month;
CREATE OR REPLACE VIEW reporting.day_to_day_comparision AS
SELECT
    year,
    day_of_week,
    count(id) AS flights_amount
FROM reporting.flight
GROUP BY year, day_of_week;
CREATE OR REPLACE VIEW reporting.day_by_day_reliability AS

SELECT
    CAST(year AS varchar) || '-' || LPAD(CAST(month AS varchar), 2, '0') || '-' || LPAD(CAST(day_of_month AS varchar), 2, '0') AS date,
    sum(is_delayed)/CAST(count(is_delayed) AS FLOAT) AS reliability
FROM reporting.flight
GROUP BY date