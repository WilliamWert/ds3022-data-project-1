
WITH emissions AS (
    -- Load vehicle emissions data from the CSV file
    SELECT *
    FROM read_csv_auto('/workspaces/ds3022-data-project-1/data/vehicle_emissions.csv', HEADER=TRUE)
    -- Only include emissions data for yellow taxis
    WHERE vehicle_type = 'yellow_taxi'
)

-- Select all columns from the yellow taxi trips table
SELECT 
    t.*,
    -- Calculate CO2 emissions for each trip in kilograms
    t.trip_distance * e.co2_grams_per_mile / 1000 AS trip_co2_kgs,
    -- Calculate average speed in miles per hour for each trip
    t.trip_distance / (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600) AS avg_mph,
    -- Extract hour of day from pickup datetime (0–23)
    EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour_of_day,
    -- Extract day of week from pickup datetime (0=Sunday, 6=Saturday)
    EXTRACT(DOW FROM tpep_pickup_datetime) AS day_of_week,
    -- Extract week of year from pickup datetime (1–52)
    EXTRACT(WEEK FROM tpep_pickup_datetime) AS week_of_year,
    -- Extract month of year from pickup datetime (1–12)
    EXTRACT(MONTH FROM tpep_pickup_datetime) AS month_of_year
-- Source table from dbt, yellow taxi trips
FROM {{ source('taxi_source', 'yellow_table') }} AS t
-- Join with emissions data to calculate CO2 per trip
CROSS JOIN emissions AS e

