import folium as fl
import geopandas as gpd
import branca.colormap as cm
from folium import GeoJsonTooltip
from sqlalchemy import create_engine
from IPython.display import display

# --- Database Connection and Data Loading ---
# Replace with your actual database connection details
DATABASE_URL = 'postgresql://postgres:mobility@localhost:5434/urbanmobility'
engine = create_engine(DATABASE_URL)

# SQL query to load segment data from PostGIS
# Replace 'your_table_name' with the actual name of your table
sql_query = "SELECT routes, from_stop_name, to_stop_name, notrips, geometry FROM aggregate_trips_per_segment"

# Load data from PostGIS into a GeoDataFrame
segment_gdf = gpd.read_postgis(sql_query, engine, geom_col='geometry')


def visualize_segments(segment_data, cutoff=10000):
    transport_map = fl.Map(location=[60.20247, 24.93252], tiles='CartoDB positron', zoom_start=12)

    colormap = cm.LinearColormap(['white', 'red'], vmin=0, vmax=cutoff, caption='Trip Count')
    colormap.add_to(transport_map)

    for _, segment in segment_data.iterrows():
        trip_count = min(segment['notrips'], cutoff)
        color = colormap(trip_count)

        geo_json = fl.GeoJson(
            data={
                'type': 'Feature',
                'geometry': segment['geometry'].__geo_interface__,
                'properties': {
                    'routes': segment['routes'],
                    'from_stop_name': segment['from_stop_name'],
                    'to_stop_name': segment['to_stop_name'],
                    'notrips': segment['notrips'],
                }
            },
            style_function=lambda x, colour=color: {
                'color': colour,
                'weight': 3,
                'opacity': 0.7},
            tooltip=GeoJsonTooltip(
                fields=['routes', 'from_stop_name', 'to_stop_name', 'notrips'],
                aliases=['Routes:', 'From Stop:', 'To Stop:', 'Trip Count:'],
                localize=True,
            )
        )

        geo_json.add_to(transport_map)

    return transport_map


transport_map = visualize_segments(segment_gdf)
display(transport_map)
