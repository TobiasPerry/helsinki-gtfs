# Library imports
import psycopg as pg
import pandas as pd # Added missing import
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import dash_bootstrap_components as dbc
import json # Used for composite key in dropdown

# Function that connects to the PostgreSQL database
def connect_to_postgres():
    try:
        # Replace ... with your actual connection string
        conn = pg.connect("dbname=... user=... password=... host=... port=...")
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None, None


# Function that fetches available trips from table vehicle_positions
def fetch_unique_trips():
    conn, cur = connect_to_postgres()
    if conn is None or cur is None:
        return []
    # Fetch distinct combinations of identifiers
    trip_query = """
        SELECT DISTINCT route_id, start_date, start_time, direction_id
        FROM vehicle_positions
        ORDER BY route_id, start_date, start_time;
    """
    cur.execute(trip_query)
    # Fetch all rows
    trips = cur.fetchall()
    cur.close()
    conn.close()
    return trips


# Function that fetches vehicle positions for a specific trip
def fetch_vehicle_positions(route_id, start_date, start_time, direction_id):
    conn, cur = connect_to_postgres()
    if conn is None or cur is None:
        return pd.DataFrame()
    query = """
            SELECT latitude, longitude, vehicle_id, speed, timestamp
            FROM vehicle_positions
            WHERE route_id = %s
              AND start_date = %s
              AND start_time = %s
              AND direction_id = %s
            ORDER BY timestamp;
            """
    # Execute the query with the composite key
    cur.execute(query, (route_id, start_date, start_time, direction_id))
    vehicle_positions = pd.DataFrame(cur.fetchall(), columns=
    ['latitude', 'longitude', 'vehicle_id', 'speed', 'timestamp'])
    cur.close()
    conn.close()
    return vehicle_positions


# Dash visualization layout
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Get initial trips to populate the dropdown
unique_trips = fetch_unique_trips()

# Create options for the dropdown
trip_options = [
    {
        'label': f"Route: {trip[0]} | Date: {trip[1]} | Time: {trip[2]} | Dir: {trip[3]}",
        # Use a JSON string to store the composite key
        'value': json.dumps({
            'route_id': trip[0],
            'start_date': str(trip[1]),
            'start_time': str(trip[2]),
            'direction_id': trip[3]
        })
    }
    for trip in unique_trips
]

# Set default trip (first one in the list)
initial_trip_value = trip_options[0]['value'] if trip_options else None

app.layout = dbc.Container([
    html.H1("Visualization of the Vehicle Positions"),
    # Dropdown for selecting a trip
    dcc.Dropdown(
        id="trip-dropdown",
        options=trip_options,
        value=initial_trip_value,
        clearable=False,
        style={"width": "80%"}
    ),
    # Graph to display vehicle positions
    dcc.Graph(id="vehicle-map")
])


# Callback function to update map based on selected trip
@app.callback(
    Output("vehicle-map", "figure"),
    Input("trip-dropdown", "value")
)
def update_map(selected_trip_json):
    if selected_trip_json is None:
        return px.scatter_mapbox() # Empty map if no trip is selected

    # Parse the JSON string from the dropdown value
    trip_data = json.loads(selected_trip_json)
    route_id = trip_data['route_id']
    start_date = trip_data['start_date']
    start_time = trip_data['start_time']
    direction_id = trip_data['direction_id']

    # Fetch vehicle positions for the selected trip
    vehicle_positions_df = fetch_vehicle_positions(route_id, start_date, start_time, direction_id)

    if vehicle_positions_df.empty:
        return px.scatter_mapbox() # Return empty map if no data

    # Create a map visualization using Plotly
    title = f"Route: {route_id} | Date: {start_date} | Time: {start_time}"
    fig = px.scatter_mapbox(vehicle_positions_df,
                            lat="latitude", lon="longitude", hover_name="vehicle_id",
                            hover_data=["speed", "timestamp"],
                            title=title, zoom=12)

    fig.update_traces(marker=dict(size=10, color='red'))

    fig.update_layout(mapbox_style="open-street-map",
                      mapbox_center={"lat": vehicle_positions_df["latitude"].mean(),
                                     "lon": vehicle_positions_df["longitude"].mean()},
                      margin={"r": 0, "t": 0, "l": 0, "b": 0})
    return fig


# Main function to run the Dash visualization
if __name__ == "__main__":
    app.run_server(debug=True, port=8050)