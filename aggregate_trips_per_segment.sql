CREATE MATERIALIZED VIEW aggregate_trips_per_segment AS
SELECT c.from_stop_id,
       c.from_stop_name,
       c.to_stop_id,
       c.to_stop_name,
       ST_Transform(s.geometry, 3879) AS geometry,
       COUNT(trip_id)                               AS notrips,
       string_agg(DISTINCT c.route_short_name, '.') AS routes
FROM segments s,
     connections c
WHERE s.route_id = c.route_id
  AND s.direction_id = c.direction_id
  AND s.start_stop_id = c.from_stop_id
  AND s.end_stop_id = c.to_stop_id
  AND date BETWEEN '2025-07-09' AND '2025-09-06'
GROUP BY c.from_stop_id, c.from_stop_name, c.to_stop_id, c.to_stop_name, s.geometry;

CREATE INDEX ON aggregate_trips_per_segment (from_stop_id, to_stop_id);
CREATE INDEX ON aggregate_trips_per_segment (notrips);

CREATE MATERIALIZED VIEW aggregate_routes_per_segment AS
SELECT c.from_stop_id,
       c.from_stop_name,
       c.to_stop_id,
       c.to_stop_name,
       ST_Transform(s.geometry, 3879) AS geometry,
       COUNT(DISTINCT c.route_short_name)                               AS notrips,
       string_agg(DISTINCT c.route_short_name, '.') AS routes
FROM segments s,
     connections c
WHERE s.route_id = c.route_id
  AND s.direction_id = c.direction_id
  AND s.start_stop_id = c.from_stop_id
  AND s.end_stop_id = c.to_stop_id
  AND date BETWEEN '2025-07-09' AND '2025-09-06'
GROUP BY c.from_stop_id, c.from_stop_name, c.to_stop_id, c.to_stop_name, s.geometry;

CREATE INDEX ON aggregate_routes_per_segment (from_stop_id, to_stop_id);
CREATE INDEX ON aggregate_routes_per_segment (notrips);

SELECT *
FROM aggregate_trips_per_segment
ORDER BY notrips DESC
LIMIT 20;

SELECT ST_srid(geometry) FROM aggregate_trips_per_segment;

SELECT * FROM geometry_columns WHERE f_table_name = 'aggregate_trips_per_segment';

SELECT DISTINCT GeometryType(geometry) FROM aggregate_trips_per_segment;
SELECT * FROM aggregate_trips_per_segment WHERE geometry IS NULL;
SELECT geometry::geometry(LINESTRING, 3879) AS geometry, * FROM aggregate_trips_per_segment;
