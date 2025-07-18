# Library imports
import time

import pandas as pd
import requests
from google.transit import gtfs_realtime_pb2

GTFS_REALTIME_URL = "https://realtime.hsl.fi/realtime/vehicle-positions/v2/hsl"
# Function that gets a parsed GTFS Realtime protobuf feed from a URL
def fetch_gtfs_realtime_data(url):
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        response = requests.get(url, verify=False)
        # Check for HTTP errors
        response.raise_for_status()
        # Parse the protobuf data
        feed.ParseFromString(response.content)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching GTFS Realtime data: {e}")
        return None
    return feed


def extract_vehicle_positions(feed):
    vehicle_positions = []
    for entity in feed.entity:
        if entity.HasField('vehicle'):
            vehicle = entity.vehicle
            vehicle_positions.append({
                "id": entity.id,
                "route_id": vehicle.trip.route_id,
                "direction_id": vehicle.trip.direction_id,
                "start_time": vehicle.trip.start_time,
                "start_date": vehicle.trip.start_date,
                "schedule_relationship": vehicle.trip.schedule_relationship,
                "latitude": vehicle.position.latitude,
                "longitude": vehicle.position.longitude,
                "bearing": vehicle.position.bearing,
                "odometer": vehicle.position.odometer,
                "speed": vehicle.position.speed * 3.6,  # Convert m/s to km/h
                "current_status": vehicle.current_status,
                "occupancy_status": vehicle.occupancy_status,
                "timestamp": vehicle.timestamp,
                "stop_id": vehicle.stop_id,
                "vehicle_id": vehicle.vehicle.id,
                "vehicle_label": vehicle.vehicle.label if vehicle.vehicle.HasField("label") else None,
                "vehicle_license_plate": vehicle.vehicle.license_plate if vehicle.vehicle.HasField("license_plate") else None,
            })
    return vehicle_positions

# Function that collects a sequence of timestamped positions from a real-time feed
def collect_vehicle_positions(duration_minutes, interval_seconds):
    collected_data = []
    end_time = time.time() + duration_minutes * 60 # Convert minutes to seconds
    while time.time() < end_time:
        # Fetch the GTFS Realtime data
        feed = fetch_gtfs_realtime_data(GTFS_REALTIME_URL)
        # If feed is fetched, extract vehicle positions
        if feed:
            vehicle_positions = extract_vehicle_positions(feed)
            if vehicle_positions:
                collected_data.extend(vehicle_positions) # Add the new data to the list
        else:
            print("Failed to fetch the GTFS Realtime feed.")
        # Wait for the specifed interval before making the next request
        time.sleep(interval_seconds)
    # Convert the collected data into a DataFrame
    df = pd.DataFrame(collected_data)
    return df


# Library imports
import pyarrow.parquet as pq
import pyarrow as pa

# Function that stores a DataFrame containing vehicle positions in a Parquet fle
def save_to_parquet(df, fle_name):
    if not df.empty:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, fle_name)
        print(f"Data successfully saved to {fle_name}")
    else:
        print("No data to save.")


# Main script
if __name__ == "__main__":
    # Parameters
    DURATION_MINUTES = 2 # Collect data during one hour
    INTERVAL_SECONDS = 1# Call the endpoint every ten seconds
    # Collects vehicle positions over the specifed duration
    print(f"Starting data collection for {DURATION_MINUTES} minutes, querying every {INTERVAL_SECONDS} seconds...")
    vehicle_positions_df = collect_vehicle_positions(DURATION_MINUTES,INTERVAL_SECONDS)
    # Save the data to a Parquet fle
    output_fle = "hsl_vehicle_positions.parquet"
    save_to_parquet(vehicle_positions_df, output_fle)
