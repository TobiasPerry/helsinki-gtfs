CREATE DATABASE urbanmobility;

CREATE EXTENSION IF NOT EXISTS mobilitydb;

CREATE OR REPLACE VIEW "public".shapes_aggregated AS
SELECT shape_id,
       array_agg(shape_dist_traveled)                                             AS distances_travelled,
       ST_Transform(ST_SetSrid(ST_MakeLine(array_agg(shape_pt_loc)), 4326), 3879) AS shape
FROM (SELECT shape_id,
             shape_dist_traveled,
             ST_AsText(shape_pt_loc)::geometry AS shape_pt_loc
      FROM "public".shapes
      ORDER BY shape_id, shape_pt_sequence) shapes
GROUP BY shape_id;

SELECT DISTINCT ST_Srid(shape)
FROM shapes_aggregated;

SELECT trip_id, stop_sequence, stop_id
FROM stop_times
ORDER BY trip_id, stop_sequence;

WITH HelsinkiCenter AS (SELECT ST_MakeEnvelope(-24.95, 60.17, 60.3553, 25.0467, 4326) AS shape)
SELECT sh.*
FROM shapes_aggregated sh,
     HelsinkiCenter hc
WHERE ST_Intersects(ST_SetSrid(sh.shape, 4326), hc.shape);

-- Narinkka
SELECT ST_Buffer(
               ST_SetSRID(ST_MakePoint(25496354.75, 6673017.97), 3879),
               300
       );

-- Itakeskus
SELECT ST_Buffer(
               ST_SetSRID(ST_MakePoint(25504437.1, 6677557.5), 3879),
               300
       );

-- Vuosaari
SELECT ST_Buffer(
               ST_SetSRID(ST_MakePoint(25508086.81, 6676420.28), 3879),
               300
       );

WITH Narinkka AS (SELECT ST_Buffer(
                                 ST_SetSRID(ST_MakePoint(25496354.75, 6673017.97), 3879),
                                 300
                         ) AS shape)
SELECT sa.shape_id, sa.shape
FROM shapes_aggregated sa,
     Narinkka n
WHERE ST_Intersects(sa.shape, n.shape);

WITH Itakeskus AS (SELECT ST_Buffer(
                                  ST_SetSRID(ST_MakePoint(25504437.1, 6677557.5), 3879),
                                  500
                          ) AS shape)
SELECT sa.shape_id, sa.shape
FROM shapes_aggregated sa,
     Itakeskus i
WHERE ST_Intersects(sa.shape, i.shape);

WITH Vuosaari AS (SELECT ST_Buffer(
                                 ST_SetSRID(ST_MakePoint(25508086.81, 6676420.28), 3879),
                                 300
                         ) AS shape)
SELECT sa.shape_id, sa.shape
FROM shapes_aggregated sa,
     Vuosaari v
WHERE ST_Intersects(sa.shape, v.shape);


WITH Narinkka AS (SELECT ST_Buffer(
                                 ST_SetSRID(ST_MakePoint(25496354.75, 6673017.97), 3879),
                                 300
                         ) AS shape)
SELECT Count(sa.shape_id)
FROM shapes_aggregated sa
         CROSS JOIN Narinkka n
WHERE ST_IsValid(sa.shape)
  AND ST_Disjoint(sa.shape, n.shape);

WITH Narinkka AS (SELECT ST_Buffer(
                                 ST_SetSRID(ST_MakePoint(25496354.75, 6673017.97), 3879),
                                 300
                         ) AS shape),
     Itakeskus AS (SELECT ST_Buffer(
                                  ST_SetSRID(ST_MakePoint(25504437.1, 6677557.5), 3879),
                                  500
                          ) AS shape),
     Vuosaari AS (SELECT ST_Buffer(
                                 ST_SetSRID(ST_MakePoint(25508086.81, 6676420.28), 3879),
                                 300
                         ) AS shape)
SELECT sa.shape_id, sa.shape
FROM shapes_aggregated sa
         CROSS JOIN Narinkka n
         CROSS JOIN Itakeskus i
         CROSS JOIN Vuosaari v
WHERE ST_Disjoint(sa.shape, n.shape)
  AND ST_Disjoint(sa.shape, i.shape)
  AND ST_Disjoint(sa.shape, v.shape);
