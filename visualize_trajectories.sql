ALTER TABLE vehicle_positions
    ADD COLUMN geom geometry(Point, 4326);

UPDATE vehicle_positions
SET geom = ST_Point(longitude, latitude, 4326);

DROP TABLE actual_trips;

CREATE TABLE actual_trips(route_id, start_date, start_time, direction_id, trip) AS
WITH positions(route_id, start_date, start_time, direction_id, geom, t) AS (
    -- We use DISTINCT since we observed duplicate tuples
    SELECT DISTINCT ON (route_id, start_date, start_time, direction_id, to_timestamp(timestamp)) route_id,
                                                                                                 start_date,
                                                                                                 start_time,
                                                                                                 direction_id,
                                                                                                 ST_Transform(geom, 3879),
                                                                                                 to_timestamp(timestamp)
    FROM vehicle_positions)
SELECT route_id,
       start_date,
       start_time,
       direction_id,
       tgeompointSeq(array_agg(tgeompoint(geom, t) ORDER BY t) FILTER (WHERE geom IS NOT NULL))
FROM positions
GROUP BY route_id, start_date, start_time, direction_id;

ALTER TABLE actual_trips
    ADD COLUMN trajectory geometry;
UPDATE actual_trips
SET trajectory = trajectory(trip);

DROP TABLE IF EXISTS trip_stops;

CREATE TABLE trip_stops AS
WITH
-- Step 1: Create a lookup for scheduled trips based on their natural key.
-- This CTE augments the GTFS `trips` table with the specific date and scheduled start time
-- for each journey, creating the key we need to join with `actual_trips`.
scheduled_trip_details AS (SELECT t.trip_id,
                                  t.route_id,
                                  t.direction_id,
                                  t.shape_id,
                                  cd.date         AS schedule_date,
                                  st.arrival_time AS schedule_start_time
                           FROM trips AS t
                                    JOIN
                                stop_times AS st ON t.trip_id = st.trip_id
                                    JOIN
                                -- This uses calendar_dates. If you use calendar.txt, the join needs to be adapted.
                                    calendar_dates AS cd ON t.service_id = cd.service_id
                           WHERE
                               -- Assumption: The first stop in the sequence defines the trip's start time.
                               -- This is a common and efficient way to find the trip start.
                               st.stop_sequence = 1),

-- Step 2: Find the nearest approach of each actual trip to its scheduled stops.
-- This CTE joins the actual trip data with the scheduled data via our new lookup CTE.
-- It then uses MobilityDB functions to calculate the exact time and location of nearest approach.
Temp AS (SELECT
             -- Identifiers for the actual trip from the vehicle positions data.
             a.route_id,
             a.start_date,
             a.start_time,
             a.direction_id,
             -- Matched identifiers from the scheduled GTFS data.
             sched.trip_id AS schedule_trip_id,
             sched.shape_id,
             -- Stop details from GTFS.
             s.stop_id,
             s.stop_name,
             ad.stop_sequence,
             s.stop_loc::geometry,
             -- Scheduled arrival time at this specific stop.
             ad.t_arrival  AS schedule_time,
             -- Core MobilityDB function: finds the instant of closest approach between the
             -- vehicle's path (a temporal geometry) and the stop's location (a static geometry).
             nearestApproachInstant(
                     a.trip,
                     ST_Transform(s.stop_loc::geometry, 3879) -- Transform stop to trip's SRID (3879).
             )             AS stop_instant
         FROM actual_trips AS a
                  -- The main join: link actual trips to scheduled trips using the composite key.
                  JOIN
              scheduled_trip_details AS sched ON
                  a.route_id = sched.route_id AND
                  EXTRACT(ISODOW FROM a.start_date) = EXTRACT(ISODOW FROM sched.schedule_date) AND
                  a.start_time = sched.schedule_start_time AND
                  a.direction_id = sched.direction_id
                  -- Join to get the scheduled arrival times for the stops of the matched trip.
                  JOIN
              arrivals_departures AS ad ON
                  sched.trip_id = ad.trip_id AND
                  a.start_date = ad.date -- Ensures we only consider stops for the correct day.
              -- Join to get stop locations and names.
                  JOIN
              stops AS s ON ad.stop_id = s.stop_id
         WHERE
             -- This proximity filter is crucial for performance and accuracy. It ensures we only
             -- calculate nearest approach for stops that the vehicle actually passed.
             -- The distance (in meters) may need tuning based on your data's precision.
             nearestApproachDistance(
                     a.trip,
                     ST_Transform(s.stop_loc::geometry, 3879)
             ) < 100)

-- Step 3: Final projection to shape the output table.
-- This selects the final columns, extracts the data from the 'instant' object returned
-- by MobilityDB, and creates a synthetic `actual_trip_id` for clarity.
SELECT
    -- We create a synthetic ID for the actual trip, as one does not exist.
    a.route_id || '-' || a.start_date || '-' || a.start_time || '-' || a.direction_id AS actual_trip_id,
    schedule_trip_id,
    shape_id,
    stop_id,
    stop_name,
    stop_sequence,
    stop_loc,
    schedule_time,
    -- Extract the timestamp from the `tgeompoint` instant.
    getTimestamp(stop_instant)                                                        AS actual_time,
    -- Extract the geometry value and transform it back to WGS84 for general use (e.g., mapping).
    ST_Transform(getValue(stop_instant), 3879)                                        AS trip_geom
FROM Temp AS a
WHERE
    -- Ensure we only include rows where a valid nearest approach could be found.
    stop_instant IS NOT NULL;

CREATE TABLE trip_segments AS
SELECT actual_trip_id,
       schedule_trip_id,
       shape_id,
       stop_id            AS end_stop_id,
       schedule_time      AS end_time_schedule,
       actual_time        AS end_time_actual,
-- Use LAG to get the previous stop's information
       LAG(stop_id) OVER (PARTITION BY actual_trip_id ORDER BY stop_sequence)
                          AS start_stop_id,
       LAG(schedule_time) OVER (PARTITION BY actual_trip_id ORDER BY
           stop_sequence) AS start_time_schedule,
       LAG(actual_time) OVER (PARTITION BY actual_trip_id ORDER BY stop_sequence)
                          AS start_time_actual
FROM trip_stops;