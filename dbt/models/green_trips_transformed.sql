
WITH emissions AS (
    -- Load vehicle emissions data from the CSV file
    SELECT *
    FROM read_csv_auto('/workspaces/ds3022-data-project-1/data/vehicle_emissions.csv', HEADER=TRUE)
    -- Filter to include only emissions data for green taxis
    WHERE vehicle_type = 'green_taxi'
)

-- Main query: Select all columns from the green taxi trips table
SELECT 
    t.*,  -- Include all columns from the taxi trips table
    -- Calculate CO2 emissions for each trip in kilograms
    t.trip_distance * e.co2_grams_per_mile / 1000 AS trip_co2_kgs,
    -- Calculate average speed for each trip in miles per hour
    t.trip_distance / (EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600) AS avg_mph,
    -- Extract hour of day from pickup datetime (0 = midnight, 23 = 11 PM)
    EXTRACT(HOUR FROM lpep_pickup_datetime) AS hour_of_day,
    -- Extract day of week from pickup datetime (0 = Sunday, 6 = Saturday)
    EXTRACT(DOW FROM lpep_pickup_datetime) AS day_of_week,
    -- Extract week number from pickup datetime (1–52)
    EXTRACT(WEEK FROM lpep_pickup_datetime) AS week_of_year,
    -- Extract month from pickup datetime (1 = January, 12 = December)
    EXTRACT(MONTH FROM lpep_pickup_datetime) AS month_of_year
-- Source table from dbt project
FROM {{ source('taxi_source', 'green_table') }} AS t
-- Cross join with the emissions CTE to apply CO2 per mile to each trip
CROSS JOIN emissions AS e
