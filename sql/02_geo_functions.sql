/*============================================================================
  File:    02_geo_functions.sql
  Project: Find Data Nearby
  Purpose: Geospatial UDTFs and stored procedures for location search
  Usage:   Run after 01_setup_database.sql
  Depends: U_S__ZIP_CODE_METADATA.ZIP_DEMOGRAPHICS.ZIP_CODE_METADATA
           WORLDWIDE_ADDRESS_DATA.ADDRESS.OPENADDRESS
============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE INGEST;

/*--------------------------------------------------------------------------
  1. search_nearby_zip_codes
     Finds zip codes within a given radius of a lat/lon point.
     ZIP_CODE_METADATA stores lat/lon as VARCHAR, so we cast to FLOAT.
--------------------------------------------------------------------------*/
CREATE OR REPLACE FUNCTION ANALYTICS_DEV_DB.STAGING.search_nearby_zip_codes(
    p_lat FLOAT,
    p_lon FLOAT,
    p_radius_meters FLOAT
)
RETURNS TABLE (
    zip           VARCHAR,
    city          VARCHAR,
    state         VARCHAR,
    latitude      FLOAT,
    longitude     FLOAT,
    distance_meters FLOAT
)
LANGUAGE SQL
AS
$$
    SELECT
        z.ZIP,
        z.CITY,
        z.STATE,
        TRY_CAST(z.LATITUDE AS FLOAT)  AS latitude,
        TRY_CAST(z.LONGITUDE AS FLOAT) AS longitude,
        ST_DISTANCE(
            ST_MAKEPOINT(TRY_CAST(z.LONGITUDE AS FLOAT), TRY_CAST(z.LATITUDE AS FLOAT)),
            ST_MAKEPOINT(p_lon, p_lat)
        ) AS distance_meters
    FROM U_S__ZIP_CODE_METADATA.ZIP_DEMOGRAPHICS.ZIP_CODE_METADATA AS z
    WHERE TRY_CAST(z.LATITUDE AS FLOAT) IS NOT NULL
      AND TRY_CAST(z.LONGITUDE AS FLOAT) IS NOT NULL
      AND ST_DISTANCE(
              ST_MAKEPOINT(TRY_CAST(z.LONGITUDE AS FLOAT), TRY_CAST(z.LATITUDE AS FLOAT)),
              ST_MAKEPOINT(p_lon, p_lat)
          ) <= p_radius_meters
    ORDER BY distance_meters ASC
$$;

/*--------------------------------------------------------------------------
  2. search_nearby_addresses
     Finds addresses within a radius from the OPENADDRESS dataset.
     LON and LAT are already FLOAT in that table.
--------------------------------------------------------------------------*/
CREATE OR REPLACE FUNCTION ANALYTICS_DEV_DB.STAGING.search_nearby_addresses(
    p_lat FLOAT,
    p_lon FLOAT,
    p_radius_meters FLOAT,
    p_limit INT
)
RETURNS TABLE (
    street          VARCHAR,
    city            VARCHAR,
    region          VARCHAR,
    postcode        VARCHAR,
    country         VARCHAR,
    latitude        FLOAT,
    longitude       FLOAT,
    distance_meters FLOAT
)
LANGUAGE SQL
AS
$$
    SELECT
        a.STREET,
        a.CITY,
        a.REGION,
        a.POSTCODE,
        a.COUNTRY,
        a.LAT   AS latitude,
        a.LON   AS longitude,
        ST_DISTANCE(
            ST_MAKEPOINT(a.LON, a.LAT),
            ST_MAKEPOINT(p_lon, p_lat)
        ) AS distance_meters
    FROM WORLDWIDE_ADDRESS_DATA.ADDRESS.OPENADDRESS AS a
    WHERE a.LAT IS NOT NULL
      AND a.LON IS NOT NULL
      AND a.LAT BETWEEN -90 AND 90
      AND a.LON BETWEEN -180 AND 180
      AND a.LAT BETWEEN (p_lat - 1) AND (p_lat + 1)
      AND a.LON BETWEEN (p_lon - 1) AND (p_lon + 1)
      AND ST_DISTANCE(
              ST_MAKEPOINT(a.LON, a.LAT),
              ST_MAKEPOINT(p_lon, p_lat)
          ) <= p_radius_meters
    ORDER BY distance_meters ASC
    LIMIT 100
$$;

/*--------------------------------------------------------------------------
  3. search_nearby_demo_data
     Searches geo-enabled tables in DEMO.DEMO, GHOST_DETECTION.APP, and
     NYC_TRANSIT for data within a radius. Returns a unified result set
     with source label, name, description, coordinates, and distance.
--------------------------------------------------------------------------*/
CREATE OR REPLACE FUNCTION ANALYTICS_DEV_DB.STAGING.search_nearby_demo_data(
    p_lat FLOAT,
    p_lon FLOAT,
    p_radius_meters FLOAT
)
RETURNS TABLE (
    source          VARCHAR,
    name            VARCHAR,
    description     VARCHAR,
    latitude        FLOAT,
    longitude       FLOAT,
    distance_meters FLOAT
)
LANGUAGE SQL
AS
$$
    WITH weather AS (
        SELECT
            'weather_station' AS source,
            w.LOCATION AS name,
            'Temp: ' || COALESCE(w.TEMP_F::VARCHAR, '?') || 'F, Humidity: '
                || COALESCE(w.RELATIVE_HUMIDITY::VARCHAR, '?') || '%' AS description,
            w.LATITUDE, w.LONGITUDE,
            ST_DISTANCE(ST_MAKEPOINT(w.LONGITUDE, w.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) AS distance_meters
        FROM DEMO.DEMO.WEATHER_STATIONS_GEO w
        WHERE w.LATITUDE IS NOT NULL AND w.LONGITUDE IS NOT NULL
          AND w.LATITUDE BETWEEN (p_lat - 1) AND (p_lat + 1)
          AND w.LONGITUDE BETWEEN (p_lon - 1) AND (p_lon + 1)
          AND ST_DISTANCE(ST_MAKEPOINT(w.LONGITUDE, w.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) <= p_radius_meters
        ORDER BY distance_meters ASC LIMIT 50
    ),
    air_quality AS (
        SELECT
            'air_quality' AS source,
            COALESCE(a.REPORTINGAREA, '') || ' (' || COALESCE(a.POLLUTANT, '') || ')' AS name,
            'AQI: ' || COALESCE(a.AQI::VARCHAR, '?') || ' - ' || COALESCE(a.CATEGORY, '') AS description,
            a.LATITUDE::FLOAT AS latitude, a.LONGITUDE::FLOAT AS longitude,
            ST_DISTANCE(ST_MAKEPOINT(a.LONGITUDE::FLOAT, a.LATITUDE::FLOAT), ST_MAKEPOINT(p_lon, p_lat)) AS distance_meters
        FROM DEMO.DEMO.AIR_QUALITY_MONITORS_GEO a
        WHERE a.LATITUDE IS NOT NULL AND a.LONGITUDE IS NOT NULL
          AND a.LATITUDE BETWEEN (p_lat - 1) AND (p_lat + 1)
          AND a.LONGITUDE BETWEEN (p_lon - 1) AND (p_lon + 1)
          AND ST_DISTANCE(ST_MAKEPOINT(a.LONGITUDE::FLOAT, a.LATITUDE::FLOAT), ST_MAKEPOINT(p_lon, p_lat)) <= p_radius_meters
        ORDER BY distance_meters ASC LIMIT 50
    ),
    traffic AS (
        SELECT
            'traffic_event' AS source,
            COALESCE(t.EVENTTYPE, 'Event') || ': ' || COALESCE(t.ROADWAYNAME, '') AS name,
            LEFT(COALESCE(t.DESCRIPTION, ''), 200) AS description,
            t.LATITUDE, t.LONGITUDE,
            ST_DISTANCE(ST_MAKEPOINT(t.LONGITUDE, t.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) AS distance_meters
        FROM DEMO.DEMO.NYCTRAFFICEVENTS t
        WHERE t.LATITUDE IS NOT NULL AND t.LONGITUDE IS NOT NULL
          AND t.LATITUDE BETWEEN (p_lat - 1) AND (p_lat + 1)
          AND t.LONGITUDE BETWEEN (p_lon - 1) AND (p_lon + 1)
          AND ST_DISTANCE(ST_MAKEPOINT(t.LONGITUDE, t.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) <= p_radius_meters
        ORDER BY distance_meters ASC LIMIT 50
    ),
    cameras_cte AS (
        SELECT
            'camera' AS source,
            c.CAMERA_NAME AS name,
            COALESCE(c.BOROUGH, '') || ' - ' || COALESCE(c.ROADWAY_NAME, '') || ' ' || COALESCE(c.DIRECTION, '') AS description,
            c.LATITUDE, c.LONGITUDE,
            ST_DISTANCE(ST_MAKEPOINT(c.LONGITUDE, c.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) AS distance_meters
        FROM DEMO.DEMO.CAMERAS c
        WHERE c.LATITUDE IS NOT NULL AND c.LONGITUDE IS NOT NULL
          AND ST_DISTANCE(ST_MAKEPOINT(c.LONGITUDE, c.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) <= p_radius_meters
        ORDER BY distance_meters ASC LIMIT 50
    ),
    aircraft AS (
        SELECT
            'aircraft' AS source,
            COALESCE(ac.FLIGHT, ac.ICAO_HEX) AS name,
            'Alt: ' || COALESCE(ac.ALTITUDE_FT::VARCHAR, '?') || 'ft, Speed: '
                || COALESCE(ac.SPEED_KTS::VARCHAR, '?') || 'kts' AS description,
            ac.LATITUDE, ac.LONGITUDE,
            ST_DISTANCE(ST_MAKEPOINT(ac.LONGITUDE, ac.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) AS distance_meters
        FROM DEMO.DEMO.ADSB_CURRENT_AIRCRAFT ac
        WHERE ac.LATITUDE IS NOT NULL AND ac.LONGITUDE IS NOT NULL
          AND ST_DISTANCE(ST_MAKEPOINT(ac.LONGITUDE, ac.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) <= p_radius_meters
        ORDER BY distance_meters ASC LIMIT 50
    ),
    iot_nodes AS (
        SELECT
            'iot_node' AS source,
            'Node ' || m.FROM_NUM::VARCHAR AS name,
            'Packets: ' || COALESCE(m.PACKET_COUNT::VARCHAR, '0')
                || ', SNR: ' || COALESCE(m.AVG_SNR::VARCHAR, '?') AS description,
            m.LAST_LATITUDE AS latitude, m.LAST_LONGITUDE AS longitude,
            ST_DISTANCE(ST_MAKEPOINT(m.LAST_LONGITUDE, m.LAST_LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) AS distance_meters
        FROM DEMO.DEMO.MESHTASTIC_ACTIVE_NODES m
        WHERE m.LAST_LATITUDE IS NOT NULL AND m.LAST_LONGITUDE IS NOT NULL
          AND ST_DISTANCE(ST_MAKEPOINT(m.LAST_LONGITUDE, m.LAST_LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) <= p_radius_meters
        ORDER BY distance_meters ASC LIMIT 50
    ),
    -- GHOST_DETECTION.APP tables
    ghost_sightings AS (
        SELECT
            'ghost_sighting' AS source,
            COALESCE(g.LOCATION_NAME, 'Unknown') AS name,
            'Activity: ' || COALESCE(g.PARANORMAL_ACTIVITY_LEVEL::VARCHAR, '?')
                || ' | ' || COALESCE(g.EVIDENCE_TYPE, '')
                || ' | EMF: ' || COALESCE(g.EMF_READING::VARCHAR, '?') AS description,
            g.LATITUDE, g.LONGITUDE,
            ST_DISTANCE(ST_MAKEPOINT(g.LONGITUDE, g.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) AS distance_meters
        FROM GHOST_DETECTION.APP.GHOST_SIGHTINGS g
        WHERE g.LATITUDE IS NOT NULL AND g.LONGITUDE IS NOT NULL
          AND g.LATITUDE BETWEEN (p_lat - 2) AND (p_lat + 2)
          AND g.LONGITUDE BETWEEN (p_lon - 2) AND (p_lon + 2)
          AND ST_DISTANCE(ST_MAKEPOINT(g.LONGITUDE, g.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) <= p_radius_meters
        ORDER BY distance_meters ASC LIMIT 50
    ),
    ghost_sensors AS (
        SELECT
            'ghost_sensor' AS source,
            COALESCE(sf.LOCATION_NAME, 'Sensor') AS name,
            LEFT(COALESCE(sf.LOCATION_NAME, ''), 200) AS description,
            sf.LATITUDE, sf.LONGITUDE,
            ST_DISTANCE(ST_MAKEPOINT(sf.LONGITUDE, sf.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) AS distance_meters
        FROM GHOST_DETECTION.APP.SENSOR_FUSION_DATA sf
        WHERE sf.LATITUDE IS NOT NULL AND sf.LONGITUDE IS NOT NULL
          AND ST_DISTANCE(ST_MAKEPOINT(sf.LONGITUDE, sf.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) <= p_radius_meters
        ORDER BY distance_meters ASC LIMIT 50
    ),
    ghost_offices AS (
        SELECT
            'ghost_office' AS source,
            o.OFFICE_NAME AS name,
            COALESCE(o.CITY, '') || ', ' || COALESCE(o.COUNTRY, '')
                || ' (' || COALESCE(o.OFFICE_TYPE, '') || ')' AS description,
            o.LATITUDE, o.LONGITUDE,
            ST_DISTANCE(ST_MAKEPOINT(o.LONGITUDE, o.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) AS distance_meters
        FROM GHOST_DETECTION.APP.OFFICES o
        WHERE o.LATITUDE IS NOT NULL AND o.LONGITUDE IS NOT NULL
          AND ST_DISTANCE(ST_MAKEPOINT(o.LONGITUDE, o.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) <= p_radius_meters
        ORDER BY distance_meters ASC LIMIT 50
    ),
    ghost_threats AS (
        SELECT
            'ghost_threat' AS source,
            COALESCE(tf.LOCATION_NAME, 'Threat') AS name,
            LEFT(COALESCE(tf.LOCATION_NAME, ''), 200) AS description,
            tf.LATITUDE, tf.LONGITUDE,
            ST_DISTANCE(ST_MAKEPOINT(tf.LONGITUDE, tf.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) AS distance_meters
        FROM GHOST_DETECTION.APP.OSINT_THREAT_FEED tf
        WHERE tf.LATITUDE IS NOT NULL AND tf.LONGITUDE IS NOT NULL
          AND ST_DISTANCE(ST_MAKEPOINT(tf.LONGITUDE, tf.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) <= p_radius_meters
        ORDER BY distance_meters ASC LIMIT 50
    ),
    ghost_missions AS (
        SELECT
            'ghost_mission' AS source,
            COALESCE(ml.LOCATION_NAME, 'Mission') AS name,
            COALESCE(ml.LOG_TYPE, '') || ' [' || COALESCE(ml.PRIORITY, '') || '] '
                || LEFT(COALESCE(ml.MESSAGE, ''), 150) AS description,
            ml.LATITUDE, ml.LONGITUDE,
            ST_DISTANCE(ST_MAKEPOINT(ml.LONGITUDE, ml.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) AS distance_meters
        FROM GHOST_DETECTION.APP.MISSION_CONTROL_LOG ml
        WHERE ml.LATITUDE IS NOT NULL AND ml.LONGITUDE IS NOT NULL
          AND ST_DISTANCE(ST_MAKEPOINT(ml.LONGITUDE, ml.LATITUDE), ST_MAKEPOINT(p_lon, p_lat)) <= p_radius_meters
        ORDER BY distance_meters ASC LIMIT 50
    ),
    -- NYC_TRANSIT tables (may be empty but ready for data)
    subway_stations AS (
        SELECT
            'subway_station' AS source,
            COALESCE(ss.STOP_NAME, ss.GTFS_STOP_ID, 'Station') AS name,
            'Lines: ' || COALESCE(ss.DAYTIME_ROUTES, '?')
                || ' | ' || COALESCE(ss.BOROUGH, '') AS description,
            ss.LATITUDE::FLOAT AS latitude, ss.LONGITUDE::FLOAT AS longitude,
            ST_DISTANCE(ST_MAKEPOINT(ss.LONGITUDE::FLOAT, ss.LATITUDE::FLOAT), ST_MAKEPOINT(p_lon, p_lat)) AS distance_meters
        FROM NYC_TRANSIT.RAW_DATA.SUBWAY_STATIONS ss
        WHERE ss.LATITUDE IS NOT NULL AND ss.LONGITUDE IS NOT NULL
          AND ss.LATITUDE BETWEEN (p_lat - 1) AND (p_lat + 1)
          AND ss.LONGITUDE BETWEEN (p_lon - 1) AND (p_lon + 1)
          AND ST_DISTANCE(ST_MAKEPOINT(ss.LONGITUDE::FLOAT, ss.LATITUDE::FLOAT), ST_MAKEPOINT(p_lon, p_lat)) <= p_radius_meters
        ORDER BY distance_meters ASC LIMIT 50
    ),
    bus_positions AS (
        SELECT
            'bus_position' AS source,
            COALESCE(bp.PUBLISHED_LINE_NAME, bp.LINE_REF, 'Bus') AS name,
            COALESCE(bp.DESTINATION_NAME, '')
                || ' | ' || COALESCE(bp.ARRIVAL_PROXIMITY_TEXT, '') AS description,
            bp.LATITUDE::FLOAT AS latitude, bp.LONGITUDE::FLOAT AS longitude,
            ST_DISTANCE(ST_MAKEPOINT(bp.LONGITUDE::FLOAT, bp.LATITUDE::FLOAT), ST_MAKEPOINT(p_lon, p_lat)) AS distance_meters
        FROM NYC_TRANSIT.CURATED.BUS_POSITIONS_LATEST bp
        WHERE bp.LATITUDE IS NOT NULL AND bp.LONGITUDE IS NOT NULL
          AND bp.LATITUDE BETWEEN (p_lat - 1) AND (p_lat + 1)
          AND bp.LONGITUDE BETWEEN (p_lon - 1) AND (p_lon + 1)
          AND ST_DISTANCE(ST_MAKEPOINT(bp.LONGITUDE::FLOAT, bp.LATITUDE::FLOAT), ST_MAKEPOINT(p_lon, p_lat)) <= p_radius_meters
        ORDER BY distance_meters ASC LIMIT 50
    )
    SELECT * FROM weather
    UNION ALL SELECT * FROM air_quality
    UNION ALL SELECT * FROM traffic
    UNION ALL SELECT * FROM cameras_cte
    UNION ALL SELECT * FROM aircraft
    UNION ALL SELECT * FROM iot_nodes
    UNION ALL SELECT * FROM ghost_sightings
    UNION ALL SELECT * FROM ghost_sensors
    UNION ALL SELECT * FROM ghost_offices
    UNION ALL SELECT * FROM ghost_threats
    UNION ALL SELECT * FROM ghost_missions
    UNION ALL SELECT * FROM subway_stations
    UNION ALL SELECT * FROM bus_positions
    ORDER BY distance_meters ASC
$$;

/*--------------------------------------------------------------------------
  4. geocode_to_nearby_summary
     Finds nearby zip codes and nearest city, then uses Cortex LLM to
     generate a natural language summary of the area.
--------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE ANALYTICS_DEV_DB.STAGING.geocode_to_nearby_summary(
    p_lat FLOAT,
    p_lon FLOAT,
    p_radius_meters FLOAT
)
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    LET nearby_zip_count INT := 0;
    LET nearest_city VARCHAR := 'Unknown';
    LET nearest_state VARCHAR := 'Unknown';
    LET ai_summary VARCHAR := '';

    -- Count nearby zip codes and find the nearest city
    SELECT
        COUNT(*)                                    AS zip_count,
        COALESCE(MIN_BY(z.CITY, dist.d), 'Unknown')  AS closest_city,
        COALESCE(MIN_BY(z.STATE, dist.d), 'Unknown')  AS closest_state
    INTO :nearby_zip_count, :nearest_city, :nearest_state
    FROM U_S__ZIP_CODE_METADATA.ZIP_DEMOGRAPHICS.ZIP_CODE_METADATA AS z,
         LATERAL (
             SELECT ST_DISTANCE(
                 ST_MAKEPOINT(TRY_CAST(z.LONGITUDE AS FLOAT), TRY_CAST(z.LATITUDE AS FLOAT)),
                 ST_MAKEPOINT(:p_lon, :p_lat)
             ) AS d
         ) AS dist
    WHERE TRY_CAST(z.LATITUDE AS FLOAT) IS NOT NULL
      AND TRY_CAST(z.LONGITUDE AS FLOAT) IS NOT NULL
      AND dist.d <= :p_radius_meters;

    -- Build a prompt and call Cortex LLM for a natural language summary
    LET prompt VARCHAR := 'You are a geographic information assistant. '
        || 'The user searched at latitude ' || :p_lat::VARCHAR
        || ', longitude ' || :p_lon::VARCHAR
        || ' with a radius of ' || :p_radius_meters::VARCHAR || ' meters. '
        || 'The nearest city is ' || :nearest_city || ', ' || :nearest_state || '. '
        || 'There are ' || :nearby_zip_count::VARCHAR || ' zip codes within that radius. '
        || 'Provide a brief, helpful summary of what is nearby this location.';

    SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', :prompt) INTO :ai_summary;

    -- Log the search to history
    INSERT INTO ANALYTICS_DEV_DB.STAGING.search_history
        (search_lat, search_lon, search_radius_meters, result_count)
    VALUES (:p_lat, :p_lon, :p_radius_meters, :nearby_zip_count);

    RETURN :ai_summary;
END;
