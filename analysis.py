import duckdb
import pandas as pd
import matplotlib.pyplot as plt

# Connect to DuckDB
con = duckdb.connect(database='emissions.duckdb', read_only=True)

cab_types = {
    'YELLOW': 'yellow_trips_transformed',
    'GREEN': 'green_trips_transformed'
}

results = {}

# Month names for X-axis
month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

for cab, table in cab_types.items():
    print(f"\n--- {cab} Taxi Analysis ---")

    # 1. Largest carbon producing trip
    query_max_trip = f"""
        SELECT *, trip_co2_kgs
        FROM {table}
        ORDER BY trip_co2_kgs DESC
        LIMIT 1;
    """
    max_trip = con.execute(query_max_trip).fetchdf()
    print(f"Largest carbon producing trip ({cab}):")
    print(max_trip[['trip_co2_kgs', 'trip_distance', 'hour_of_day', 'day_of_week']])
    
    # 2. Most and least carbon heavy hours
    query_hour = f"""
        SELECT hour_of_day, AVG(trip_co2_kgs) AS avg_co2
        FROM {table}
        GROUP BY hour_of_day
        ORDER BY avg_co2 DESC;
    """
    df_hour = con.execute(query_hour).fetchdf()
    print(f"Most carbon heavy hour ({cab}): {df_hour.iloc[0]['hour_of_day']} with {df_hour.iloc[0]['avg_co2']:.2f} kg CO2 on average")
    print(f"Least carbon heavy hour ({cab}): {df_hour.iloc[-1]['hour_of_day']} with {df_hour.iloc[-1]['avg_co2']:.2f} kg CO2 on average")

    # 3. Most and least carbon heavy days of the week
    query_day = f"""
        SELECT day_of_week, AVG(trip_co2_kgs) AS avg_co2
        FROM {table}
        GROUP BY day_of_week
        ORDER BY avg_co2 DESC;
    """
    df_day = con.execute(query_day).fetchdf()
    print(f"Most carbon heavy day ({cab}): {df_day.iloc[0]['day_of_week']} with {df_day.iloc[0]['avg_co2']:.2f} kg CO2 on average")
    print(f"Least carbon heavy day ({cab}): {df_day.iloc[-1]['day_of_week']} with {df_day.iloc[-1]['avg_co2']:.2f} kg CO2 on average")

    # 4. Most and least carbon heavy weeks
    query_week = f"""
        SELECT week_of_year, AVG(trip_co2_kgs) AS avg_co2
        FROM {table}
        GROUP BY week_of_year
        ORDER BY avg_co2 DESC;
    """
    df_week = con.execute(query_week).fetchdf()
    print(f"Most carbon heavy week ({cab}): {df_week.iloc[0]['week_of_year']} with {df_week.iloc[0]['avg_co2']:.2f} kg CO2 on average")
    print(f"Least carbon heavy week ({cab}): {df_week.iloc[-1]['week_of_year']} with {df_week.iloc[-1]['avg_co2']:.2f} kg CO2 on average")

    # 5. Most and least carbon heavy months
    query_month = f"""
        SELECT month_of_year, AVG(trip_co2_kgs) AS avg_co2
        FROM {table}
        GROUP BY month_of_year
        ORDER BY avg_co2 DESC;
    """
    df_month = con.execute(query_month).fetchdf()
    print(f"Most carbon heavy month ({cab}): {month_names[int(df_month.iloc[0]['month_of_year'])-1]} with {df_month.iloc[0]['avg_co2']:.2f} kg CO2 on average")
    print(f"Least carbon heavy month ({cab}): {month_names[int(df_month.iloc[-1]['month_of_year'])-1]} with {df_month.iloc[-1]['avg_co2']:.2f} kg CO2 on average")

    # Save month-level CO2 totals for plotting
    df_month_total = con.execute(f"""
        SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2
        FROM {table}
        GROUP BY month_of_year
        ORDER BY month_of_year
    """).fetchdf()
    # Add month names for plotting
    df_month_total['month_name'] = [month_names[int(m)-1] for m in df_month_total['month_of_year']]
    results[cab] = df_month_total

# --- Plot monthly CO2 totals ---
plt.figure(figsize=(10, 6))
for cab, df in results.items():
    plt.plot(df['month_name'], df['total_co2'], marker='o', label=cab)

plt.title("Monthly CO2 Totals by Taxi Type")
plt.xlabel("Month")
plt.ylabel("Total CO2 (kg)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("monthly_co2_totals.png")
plt.show()
print("\nPlot saved as 'monthly_co2_totals.png'")
