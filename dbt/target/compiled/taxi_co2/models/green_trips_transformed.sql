WITH emissions AS (
    SELECT *
    FROM read_csv_auto('/workspaces/ds3022-data-project-1/data/vehicle_emissions.csv', HEADER=TRUE)
    WHERE vehicle_type = 'green_taxi'
)

SELECT 
    t.*,
    t.trip_distance * e.co2_grams_per_mile / 1000 AS trip_co2_kgs,
    t.trip_distance / (EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600) AS avg_mph,
    EXTRACT(HOUR FROM lpep_pickup_datetime) AS hour_of_day,
    EXTRACT(DOW FROM lpep_pickup_datetime) AS day_of_week,
    EXTRACT(WEEK FROM lpep_pickup_datetime) AS week_of_year,
    EXTRACT(MONTH FROM lpep_pickup_datetime) AS month_of_year
FROM "emissions"."main"."green_table" AS t
CROSS JOIN emissions AS e