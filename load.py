import duckdb
import os
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

months = [f"{i:02d}" for i in range(1, 13)]  # Jan–Dec
yellow_urls = [f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{m}.parquet" for m in months]
green_urls = [f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-{m}.parquet" for m in months]


def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")


        con.execute(f"""
            -- Loading yellow trips
            CREATE OR REPLACE TABLE yellow_table AS
            SELECT *
            FROM read_parquet({yellow_urls});

            -- Loading green trips
            CREATE OR REPLACE TABLE green_table AS
            SELECT *
            FROM read_parquet({green_urls});

            -- Loading vehicle emissions
            CREATE OR REPLACE TABLE vehicle_emissions AS
            SELECT * FROM read_csv('data/vehicle_emissions.csv');
        """)

        logger.info("Tables loaded successfully")

        # --- Compute descriptive statistics ---
        queries = {
            "yellow_table": """
                SELECT 
                    COUNT(*) AS n_rows,
                    AVG(trip_distance) AS avg_trip_distance,
                    AVG(fare_amount) AS avg_fare,
                    AVG(tip_amount) AS avg_tip
                FROM yellow_table
            """,
            "green_table": """
                SELECT 
                    COUNT(*) AS n_rows,
                    AVG(trip_distance) AS avg_trip_distance,
                    AVG(fare_amount) AS avg_fare,
                    AVG(tip_amount) AS avg_tip
                FROM green_table
            """
        }

        for table, query in queries.items():
            df = con.execute(query).fetchdf()
            
            # Print to screen
            print(f"\nDescriptive statistics for {table}:")
            print(df)
            
            # Log to file
            logger.info(f"\nDescriptive statistics for {table}:\n{df.to_string(index=False)}")

        print("\n✅ Load complete. Stats written to screen and load.log")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()
