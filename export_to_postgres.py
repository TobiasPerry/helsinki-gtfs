import duckdb

PARQUET_FILE = "hsl_vehicle_positions_2_hours.parquet"

con = duckdb.connect()
query = f"""
COPY (SELECT * FROM read_parquet("{PARQUET_FILE}"))
TO '/home/jr/PycharmProjects/helsinki-gtfs/hsl_vehicle_positions.csv' (HEADER, DELIMITER ',');
"""

con.execute(query)
con.close()