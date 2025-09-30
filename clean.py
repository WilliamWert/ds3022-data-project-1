import duckdb
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

def clean_trips():
    con = None
    try:
        con = duckdb.connect(database="emissions.duckdb", read_only=False) # duckdb connection

        # --- Clean yellow trips ---
        logger.info("Cleaning yellow trips...")
        con.execute("""
            CREATE TABLE yellow_table_clean AS
            SELECT DISTINCT
                   tpep_pickup_datetime,
                   tpep_dropoff_datetime,
                   passenger_count,
                   trip_distance,
                   total_amount,
                   EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) AS trip_seconds
            FROM yellow_table
            WHERE passenger_count > 0
              AND trip_distance > 0
              AND trip_distance <= 100
              AND EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) <= 86400;
        """)
        con.execute("DROP TABLE yellow_table;")
        con.execute("ALTER TABLE yellow_table_clean RENAME TO yellow_table;")

        # --- Clean green trips ---
        logger.info("Cleaning green trips...")
        con.execute("""
            CREATE TABLE green_table_clean AS
            SELECT DISTINCT
                   lpep_pickup_datetime,
                   lpep_dropoff_datetime,
                   passenger_count,
                   trip_distance,
                   total_amount,
                   EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) AS trip_seconds
            FROM green_table
            WHERE passenger_count > 0
              AND trip_distance > 0
              AND trip_distance <= 100
              AND EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) <= 86400;
        """)
        con.execute("DROP TABLE green_table;")
        con.execute("ALTER TABLE green_table_clean RENAME TO green_table;")

        # --- Verification ---
        for table, pickup_col, dropoff_col in [
            ("yellow_table", "tpep_pickup_datetime", "tpep_dropoff_datetime"),
            ("green_table", "lpep_pickup_datetime", "lpep_dropoff_datetime")
        ]:
            count = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            dupes = con.execute(f"""
                SELECT COUNT(*) - COUNT(DISTINCT ({pickup_col}, {dropoff_col}, passenger_count, trip_distance, total_amount))
                FROM {table};
            """).fetchone()[0]
            bad = con.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE passenger_count <= 0
                   OR trip_distance <= 0
                   OR trip_distance > 100
                   OR EXTRACT(EPOCH FROM ({dropoff_col} - {pickup_col})) > 86400;
            """).fetchone()[0]

            msg = f"{table}: {count} rows, duplicates={dupes}, bad={bad}"
            print(msg)
            logger.info(msg)

        print("\n✅ Cleaning complete. Results written to clean.log")

    except Exception as e:
        err_msg = f"An error occurred: {e}"
        print(err_msg)
        logger.error(err_msg)

if __name__ == "__main__":
    clean_trips()
