SELECT AVG(s.distance_m /
           EXTRACT(EPOCH FROM (t.end_time_actual - t.start_time_actual)) * 3.6) AS
           speed_kmh,
       s.geometry,
       t.start_stop_id,
       t.end_stop_id
FROM trip_segments t,
     segments s
WHERE t.start_stop_id = s.start_stop_id
  AND t.end_stop_id = s.end_stop_id
  AND t.start_time_actual IS NOT NULL
  AND t.shape_id = s.shape_id
  AND EXTRACT(EPOCH FROM (t.end_time_actual - t.start_time_actual)) > 0
GROUP BY s.geometry, t.start_stop_id, t.end_stop_id;

CREATE TABLE trips_join_segments AS
SELECT t.actual_trip_id,
       t.schedule_trip_id,
       t.shape_id,
       s.distance_m,
       t.end_time_actual,
       t.start_time_actual,
       EXTRACT(EPOCH FROM (t.end_time_actual - t.start_time_actual))     AS
                                                                            elapsed_time_actual,
       EXTRACT(EPOCH FROM (t.end_time_schedule - t.start_time_schedule)) AS
                                                                            elapsed_time_schedule,
       t.end_time_schedule,
       t.start_time_schedule,
       s.geometry                                                        AS seg_geo,
       t.start_stop_id,
       t.end_stop_id,
       s.stop_sequence
FROM trip_segments t,
     segments s
WHERE t.start_stop_id = s.start_stop_id
  AND t.end_stop_id = s.end_stop_id
  AND t.start_time_actual IS NOT NULL
  AND t.shape_id = s.shape_id
  AND EXTRACT(EPOCH FROM (t.end_time_actual - t.start_time_actual)) > 0;

SELECT start_stop_id, end_stop_id, COUNT(*) AS no_trips_seg
FROM trips_join_segments
GROUP BY start_stop_id, end_stop_id
ORDER BY no_trips_seg DESC;

ALTER TABLE trip_stops
    ADD COLUMN delay numeric;
UPDATE trip_stops
SET delay = EXTRACT(EPOCH FROM actual_time - schedule_time) / 60;

WITH agg_by_segm AS (SELECT start_stop_id,
                            end_stop_id,
                            seg_geo,
                            COUNT(*)                   AS no_trips_seg,
                            AVG(elapsed_time_actual)   AS avg_span_actual,
                            AVG(elapsed_time_schedule) AS avg_span_schedule
                     FROM trips_join_segments
                     GROUP BY start_stop_id, end_stop_id, seg_geo)
SELECT seg_geo,
       no_trips_seg,
       CASE WHEN avg_span_actual - avg_span_schedule < 0 THEN 1 ELSE 0 END
           AS has_delay
FROM agg_by_segm;

-- Trip delay total
DROP TABLE IF EXISTS trip_delay_total;
CREATE TABLE trip_delay_total AS
WITH numbered_trips AS (SELECT actual_trip_id,
                               schedule_trip_id,
                               stop_loc,
                               schedule_time,
                               trip_geom,
                               actual_time,
                               ctid,
                               ROW_NUMBER()
                               OVER (PARTITION BY actual_trip_id, schedule_trip_id ORDER BY schedule_time, ctid) -
                               1 AS row_offset
                        FROM trip_stops)
SELECT actual_trip_id,
       schedule_trip_id,
       transform(tgeompointSeq(array_agg(tgeompoint(stop_loc, schedule_time + row_offset * interval '1 microsecond')
                                         ORDER BY schedule_time + row_offset * interval '1 microsecond', ctid)),
                 3879) AS schedule_trip,
       transform(tgeompointSeq(array_agg(tgeompoint(trip_geom, actual_time + row_offset * interval '1 microsecond')
                                         ORDER BY actual_time + row_offset * interval '1 microsecond', ctid)),
                 3879) AS actual_trip
FROM numbered_trips
GROUP BY actual_trip_id, schedule_trip_id;

SELECT actual_trip_id,
       duration(timespan(actual_trip))                                                                AS duration_actual,
       duration(timespan(schedule_trip))                                                              AS duration_schedule,
       EXTRACT(EPOCH FROM (duration(timespan(actual_trip)) - duration(timespan(schedule_trip)))) / 60 AS delay,
       -- Explicitly set the SRID and cast the geometry type
       ST_SetSRID(ST_MakeLine(trajectory(actual_trip)), 3879)::geometry(LineString, 3879)             AS trajectory
FROM trip_delay_total
group by actual_trip_id, actual_trip, schedule_trip;

SELECT srid(actual_trip)
FROM trip_delay_total;

-- Stop delay
SELECT schedule_trip_id, schedule_time, delay
FROM trip_stops
WHERE schedule_trip_id in ('2582_20250709_Pe_2_2202') ORDER BY schedule_time;