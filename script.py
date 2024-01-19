import requests
import psycopg2
import time
from config import (db_params, API_KEY)


def list_time_zone():
    """
    Get all timezones from the timezonedb API /list-time-zone endpoint

    The API returns a list of time zones in this format:
    [{
        "countryCode":"AD",
        "countryName":"Andorra",
        "zoneName":"Europe/Andorra",
        "gmtOffset":7200,
        "dst": False
    },...]

    Return data, error message
    """
    json_result = []
    error = ''
    try:
        response = requests.get(
            'http://api.timezonedb.com/v2.1/list-time-zone',
            params={
                'key': API_KEY,
                'format': 'json',
                'fields': (
                    'countryCode,'
                    'countryName,'
                    'zoneName,'
                    'gmtOffset,'
                    'dst'
                )
            }
        )

        data, error = extract_json(response)

        json_result = data.get('zones', [])

    except Exception as e:
        error = "Python Script Error: " + repr(e)

    return json_result, error


def get_time_zone_by_name(zone_name):
    """
    Look up info for a time zone by name using the timezonedb
    API's /get-time-zone endpoint

    The API returns the requested time zone in this format:
    {
        "countryCode": "US",
        "countryName": "United States",
        "zoneName": "America/New_York",
        "gmtOffset": -18000,
        "dst": False,
        "zoneStart": 1699164000,
        "zoneEnd": 1710054000
    }
    """
    json_result = {}
    error = ''
    try:
        response = requests.get(
            'http://api.timezonedb.com/v2.1/get-time-zone',
            params={
                'key': API_KEY,
                'format': 'json',
                'by': 'zone',
                'zone': zone_name,
                'fields': (
                    'zoneName,'
                    'zoneStart,'
                    'zoneEnd,'
                    'countryCode,'
                    'countryName,'
                    'gmtOffset,'
                    'dst'
                )
            }
        )

        json_result, error = extract_json(response)

    except Exception as e:
        error = "Python Script Error: " + repr(e)

    # Fill in non-nullable fields
    max_bigint_value = 9223372036854775807
    if json_result and not json_result.get('zoneStart'):
        json_result['zoneStart'] = -max_bigint_value
    if json_result and not json_result.get('zoneEnd'):
        json_result['zoneEnd'] = max_bigint_value

    return json_result, error


def get_time_zone_details(time_zones, rate_limit=1, buffer=1):
    """
    Get the time zone details for each time zone in time_zones

    rate_limit: the requests/second cap
    buffer: a small factor to add to the request time, to account
        for variations in the target server's rate tracking
    """
    time_per_request = 1 / rate_limit + buffer  # in seconds

    # Wait in case a different request just finished
    time.sleep(time_per_request)

    start_time = time.time()

    zone_details = []
    errors = []

    # Show progress
    total_zones = len(time_zones)
    i = 1

    # Get data for each timezone
    for tz in time_zones:
        zone_name = tz['zoneName']
        zone_detail, error = get_time_zone_by_name(zone_name)

        if error:
            errors.append(error)
        else:
            zone_details.append(zone_detail)

        # Show progress
        print(f"{i}/{total_zones} Time Zones Loaded")
        i += 1

        # Calculate the time elapsed since the request was made
        elapsed_time = time.time() - start_time

        # If too little time has passed, sleep for the remaining time
        if elapsed_time < time_per_request:
            time.sleep(time_per_request - elapsed_time)

        # Reset the start time for the next iteration
        start_time = time.time()

    return zone_details, errors


def extract_json(response):
    '''
    Get the json from the response, or any error messages
    '''
    json_result = {}

    # Errors like rate limits, bad API key, etc.
    # Some errors return 200 but status = "FAILED"
    if not response.ok or response.json().get('status') != 'OK':
        # response.json() will error if this is True
        if 'application/json' not in response.headers.get('Content-Type', {}):
            return json_result, response.reason

        error = response.json().get('message', 'No error message in response')
        return json_result, error

    json_result = response.json()
    return json_result, ''


def log_error(cursor, err):
    """
    Log any errors to TZDB_ERROR_LOG
    """
    # the ERROR_DATE column is automatically filled
    error_query = """
    INSERT INTO "TZDB_ERROR_LOG" ("ERROR_MESSAGE") VALUES (%s)
    """
    cursor.execute(error_query, (err,))


def populate_data(connection):
    """
    Import data from the TimeZoneDB API into the connected database
    """
    cursor = connection.cursor()

    # Get timezones
    time_zones, time_zones_error = list_time_zone()
    TZDB_TIMEZONES_entries = time_zones

    # Recoverable error
    if time_zones_error:
        log_error(cursor, time_zones_error)

    # Don't do anything if the list query failed
    if not TZDB_TIMEZONES_entries:
        log_error(
            cursor, err="No data received from API. List query failed."
        )
        cursor.close()
        connection.commit()
        return

    # Make a temp table with the current time so all entries loaded
    # this run have the same time
    cursor.execute("""
    CREATE TEMPORARY TABLE temp_time_table (
        import_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    cursor.execute("INSERT INTO temp_time_table DEFAULT VALUES;")

    # Delete old TZDB_TIMEZONES
    cursor.execute('TRUNCATE TABLE "TZDB_TIMEZONES";')

    # Populate tables
    tzdb_timezones_query = """
        INSERT INTO "TZDB_TIMEZONES"(
            "COUNTRYCODE",
            "COUNTRYNAME",
            "ZONENAME",
            "GMTOFFSET",
            "IMPORT_DATE"
        ) VALUES (
            %(countryCode)s,
            %(countryName)s,
            %(zoneName)s,
            %(gmtOffset)s,
            (SELECT import_time FROM temp_time_table)
        );
    """
    for row in TZDB_TIMEZONES_entries:
        cursor.execute(tzdb_timezones_query, row)

    # Get timezone details
    TZDB_ZONE_DETAILS_entries, details_errors = get_time_zone_details(
        TZDB_TIMEZONES_entries
    )

    for d_error in details_errors:
        log_error(cursor, d_error)

    # Staging table
    # Use TZDB_ZONE_DETAILS structure
    cursor.execute("""
    CREATE TEMPORARY TABLE temp_zone_details AS
    SELECT * FROM "TZDB_ZONE_DETAILS" WHERE 1=0;
    """)

    temp_table_insert_query = """
        INSERT INTO temp_zone_details(
            "ZONENAME",
            "ZONESTART",
            "ZONEEND",
            "COUNTRYCODE",
            "COUNTRYNAME",
            "GMTOFFSET",
            "DST",
            "IMPORT_DATE"
        ) VALUES (
            %(zoneName)s,
            %(zoneStart)s,
            %(zoneEnd)s,
            %(countryCode)s,
            %(countryName)s,
            %(gmtOffset)s,
            %(dst)s,
            (SELECT import_time FROM temp_time_table)
        );
    """
    for row in TZDB_ZONE_DETAILS_entries:
        cursor.execute(temp_table_insert_query, row)

    # Add new rows from the API, ignoring those already present
    filtered_insert_query = """
    INSERT INTO "TZDB_ZONE_DETAILS" (
        "ZONENAME",
        "ZONESTART",
        "ZONEEND",
        "COUNTRYCODE",
        "COUNTRYNAME",
        "GMTOFFSET",
        "DST",
        "IMPORT_DATE"
    )
    SELECT temp_zone_details."ZONENAME",
        temp_zone_details."ZONESTART",
        temp_zone_details."ZONEEND",
        temp_zone_details."COUNTRYCODE",
        temp_zone_details."COUNTRYNAME",
        temp_zone_details."GMTOFFSET",
        temp_zone_details."DST",
        temp_zone_details."IMPORT_DATE"
    FROM temp_zone_details LEFT JOIN "TZDB_ZONE_DETAILS" ON
        temp_zone_details."ZONENAME" = "TZDB_ZONE_DETAILS"."ZONENAME"
        AND temp_zone_details."ZONESTART" = "TZDB_ZONE_DETAILS"."ZONESTART"
        AND temp_zone_details."ZONEEND" = "TZDB_ZONE_DETAILS"."ZONEEND"
    WHERE "TZDB_ZONE_DETAILS"."ZONENAME" IS NULL;
    """
    cursor.execute(filtered_insert_query)

    cursor.close()
    connection.commit()


if __name__ == '__main__':
    connection = psycopg2.connect(**db_params)
    populate_data(connection)
    connection.close()
