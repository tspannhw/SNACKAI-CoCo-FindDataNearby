/*============================================================================
  File:    03_cortex_agent.sql
  Project: Find Data Nearby
  Purpose: Cortex-powered natural language agent for geo queries
  Usage:   Run after 02_geo_functions.sql
  Depends: ANALYTICS_DEV_DB.STAGING.search_nearby_zip_codes
           ANALYTICS_DEV_DB.STAGING.search_nearby_addresses
           ANALYTICS_DEV_DB.STAGING.geocode_to_nearby_summary
============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE INGEST;

/*--------------------------------------------------------------------------
  find_nearby_agent
  Natural-language interface for geo search. Takes a user question such as
  "What zip codes are within 10 miles of downtown Denver?" and:
    1. Uses Cortex LLM to extract lat, lon, radius, and intent
    2. Calls the appropriate geo function(s)
    3. Returns a natural-language answer via Cortex LLM
--------------------------------------------------------------------------*/
CREATE OR REPLACE PROCEDURE ANALYTICS_DEV_DB.STAGING.find_nearby_agent(
    p_question VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    LET extracted_json VARCHAR := '';
    LET lat FLOAT := 0;
    LET lon FLOAT := 0;
    LET radius_meters FLOAT := 1609.34;  -- default 1 mile
    LET intent VARCHAR := 'zip_codes';
    LET result_text VARCHAR := '';
    LET final_answer VARCHAR := '';

    ---------------------------------------------------------------------------
    -- Step 1: Extract structured location intent from the natural language
    ---------------------------------------------------------------------------
    LET extraction_prompt VARCHAR :=
        'Extract location data from this question as JSON only. No other text. '
        || 'Keys: latitude (float), longitude (float), radius_meters (float), intent (one of: zip_codes, addresses, summary). '
        || 'If city mentioned, use approximate coordinates. Convert miles to meters (1mi=1609.34m). '
        || 'Default radius: 1609.34. Default intent: zip_codes. '
        || 'Question: ' || :p_question;

    SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', :extraction_prompt)
    INTO :extracted_json;

    -- Extract JSON substring from LLM response and parse it
    LET clean_json VARCHAR := '';
    BEGIN
        clean_json := REGEXP_SUBSTR(:extracted_json, '\\{[^}]+\\}');
        lat := PARSE_JSON(:clean_json):latitude::FLOAT;
        lon := PARSE_JSON(:clean_json):longitude::FLOAT;
        radius_meters := COALESCE(PARSE_JSON(:clean_json):radius_meters::FLOAT, 1609.34);
        intent := COALESCE(PARSE_JSON(:clean_json):intent::VARCHAR, 'zip_codes');
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'I could not parse location from your question. Raw LLM output: ' || :extracted_json
                || '. Try: "What is near latitude 40.7, longitude -74.0 within 5 miles?"';
    END;

    ---------------------------------------------------------------------------
    -- Step 2: Call the appropriate geo function based on intent
    ---------------------------------------------------------------------------
    CASE :intent
        WHEN 'zip_codes' THEN
            -- Get nearby zip codes and format as text
            WITH nearby AS (
                SELECT *
                FROM TABLE(ANALYTICS_DEV_DB.STAGING.search_nearby_zip_codes(
                    :lat, :lon, :radius_meters
                ))
                LIMIT 20
            )
            SELECT COALESCE(
                LISTAGG(
                    zip || ' - ' || city || ', ' || state
                    || ' (' || ROUND(distance_meters, 0)::VARCHAR || 'm)',
                    '\n'
                ) WITHIN GROUP (ORDER BY distance_meters),
                'No zip codes found within the specified radius.'
            )
            INTO :result_text
            FROM nearby;

        WHEN 'addresses' THEN
            -- Get nearby addresses and format as text
            WITH nearby AS (
                SELECT *
                FROM TABLE(ANALYTICS_DEV_DB.STAGING.search_nearby_addresses(
                    :lat, :lon, :radius_meters, 20
                ))
            )
            SELECT COALESCE(
                LISTAGG(
                    COALESCE(street, '') || ', ' || COALESCE(city, '')
                    || ', ' || COALESCE(region, '') || ' ' || COALESCE(postcode, '')
                    || ' (' || ROUND(distance_meters, 0)::VARCHAR || 'm)',
                    '\n'
                ) WITHIN GROUP (ORDER BY distance_meters),
                'No addresses found within the specified radius.'
            )
            INTO :result_text
            FROM nearby;

        WHEN 'summary' THEN
            -- Generate an AI-powered area summary
            CALL ANALYTICS_DEV_DB.STAGING.geocode_to_nearby_summary(
                :lat, :lon, :radius_meters
            ) INTO :result_text;

        ELSE
            result_text := 'No results found for the specified intent.';
    END CASE;

    ---------------------------------------------------------------------------
    -- Step 3: Use Cortex LLM to compose a friendly answer
    ---------------------------------------------------------------------------
    LET answer_prompt VARCHAR :=
        'You are a helpful geographic assistant. The user asked: "'
        || :p_question || '". '
        || 'Here are the search results for latitude ' || :lat::VARCHAR
        || ', longitude ' || :lon::VARCHAR
        || ' within ' || :radius_meters::VARCHAR || ' meters:\n'
        || :result_text || '\n\n'
        || 'Provide a clear, conversational answer to the user''s question '
        || 'based on these results. Keep it concise.';

    SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-8b', :answer_prompt)
    INTO :final_answer;

    -- Log the search
    INSERT INTO ANALYTICS_DEV_DB.STAGING.search_history
        (search_lat, search_lon, search_radius_meters, result_count)
    VALUES (:lat, :lon, :radius_meters, NULL);

    RETURN :final_answer;
END;
