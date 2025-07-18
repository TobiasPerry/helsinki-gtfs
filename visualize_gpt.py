import pandas as pd
import plotly.graph_objects as go
import webbrowser
import os

# Path to your .parquet file
PARQUET_FILE = "hsl_vehicle_positions_2_hours.parquet"  # Replace with your actual file name

def load_and_clean_data(parquet_file):
    df = pd.read_parquet(parquet_file)

    # Keep and sanitize relevant columns
    df = df[['latitude', 'longitude', 'speed', 'vehicle_id', 'timestamp']]
    df = df.dropna(subset=['latitude', 'longitude', 'speed', 'vehicle_id', 'timestamp'])

    # Remove rows with speed < 0 and invalid coordinates (0.0 is not a real lat/lon in this context)
    df = df[(df['speed'] >= 0) & (df['latitude'] != 0) & (df['longitude'] != 0)]

    # Sort for trajectory plotting
    df = df.sort_values(by=['vehicle_id', 'timestamp'])

    return df


def create_trajectory_map(df):
    fig = go.Figure()

    for vehicle_id, group in df.groupby('vehicle_id'):
        fig.add_trace(go.Scattermapbox(
            lat=group['latitude'],
            lon=group['longitude'],
            mode='lines+markers',
            line=dict(width=2),
            marker=dict(
                size=6,
                color=group['speed'],
                colorscale='hot',
                colorbar=dict(title='Speed (km/h)'),
                showscale=False  # Turn on for one trace only if needed
            ),
            name=str(vehicle_id)
        ))

    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=10,
        mapbox_center={"lat": 60.1699, "lon": 24.9384},  # Centered on Helsinki
        title="Vehicle Trajectories in Helsinki (Grouped by Vehicle, Colored by Speed)",
        margin=dict(l=0, r=0, t=40, b=0)
    )

    return fig

def main():
    df = load_and_clean_data(PARQUET_FILE)
    fig = create_trajectory_map(df)

    output_file = "vehicle_trajectories_by_vehicle.html"
    fig.write_html(output_file)
    webbrowser.open('file://' + os.path.realpath(output_file))

if __name__ == "__main__":
    main()
