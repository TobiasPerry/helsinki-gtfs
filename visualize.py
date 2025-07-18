# Library imports
import duckdb
import pandas as pd
import plotly.express as px

# Path to the Parquet file
parquet_file = "hsl_vehicle_positions.parquet"

# Function that extracts trajectory of one vehicle from route 1
def query_tram1_trajectory(parquet_file):
    # Connect to DuckDB in-memory database
    con = duckdb.connect()
    query = f"""
    SELECT *
    FROM parquet_scan('{parquet_file}')
    WHERE route_id = '1'
      AND vehicle_id IS NOT NULL
      AND latitude IS NOT NULL AND longitude IS NOT NULL
    ORDER BY vehicle_id, timestamp
    """
    df = con.execute(query).fetchdf()
    con.close()
    return df

# Function to visualize the trajectory using Plotly
def visualize_tram1_trajectory(df):
    if df.empty:
        print("No data available for Tram 1.")
        return
    # Optionally filter a single vehicle to visualize
    vehicle_id = df['vehicle_id'].unique()[0]
    df = df[df['vehicle_id'] == vehicle_id]

    fig = px.scatter_mapbox(
        df,
        lat="latitude",
        lon="longitude",
        hover_name="vehicle_id",
        hover_data=["route_id", "start_time", "direction_id", "speed"],
        color="speed",
        color_continuous_scale="Hot",
        title=f"Trajectory of Tram Route 1 – Vehicle {vehicle_id}",
        zoom=12
    )
    fig.update_layout(
        mapbox_style="open-street-map",
        mapbox_center={"lat": 60.1695, "lon": 24.9354},  # Centered on Helsinki
        margin={"r": 0, "t": 0, "l": 0, "b": 0}
    )
    fig.show()

# Optional: Inspect schema
def inspect_parquet_schema(parquet_file):
    con = duckdb.connect()
    query = f"DESCRIBE SELECT * FROM parquet_scan('{parquet_file}')"
    schema_df = con.execute(query).fetchdf()
    con.close()
    print(schema_df)

# Optional: Preview data
def inspect_parquet_head(parquet_file, n=5):
    df = pd.read_parquet(parquet_file)
    print("Columns:", df.columns.tolist())
    print(df.head(n).to_string(index=False))

# Main script
if __name__ == "__main__":

    inspect_parquet_head(parquet_file, n=5)
    # tram1_df = query_tram1_trajectory(parquet_file)
    # if not tram1_df.empty:
    #     visualize_tram1_trajectory(tram1_df)
    # else:
    #     print("No data found for Tram 1.")
