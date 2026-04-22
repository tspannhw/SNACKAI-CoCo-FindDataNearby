/*============================================================================
  File:    01_setup_database.sql
  Project: Find Data Nearby
  Purpose: Create database, schema, and application tables
  Usage:   Run with ACCOUNTADMIN role and INGEST warehouse
============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE INGEST;

-- Database and schema
CREATE DATABASE IF NOT EXISTS ANALYTICS_DEV_DB;
CREATE SCHEMA IF NOT EXISTS ANALYTICS_DEV_DB.STAGING;

-- Search history: tracks every geo search performed by users
CREATE OR REPLACE TABLE ANALYTICS_DEV_DB.STAGING.search_history (
    id              NUMBER AUTOINCREMENT PRIMARY KEY,
    search_lat      FLOAT          NOT NULL,
    search_lon      FLOAT          NOT NULL,
    search_address  VARCHAR,
    search_radius_meters FLOAT     NOT NULL,
    result_count    INT,
    searched_at     TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- Cached locations: stores resolved locations from external sources
CREATE OR REPLACE TABLE ANALYTICS_DEV_DB.STAGING.cached_locations (
    id          NUMBER AUTOINCREMENT PRIMARY KEY,
    name        VARCHAR,
    address     VARCHAR,
    city        VARCHAR,
    state       VARCHAR,
    zip         VARCHAR,
    latitude    FLOAT,
    longitude   FLOAT,
    geo_point   GEOGRAPHY,
    source      VARCHAR,
    cached_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
