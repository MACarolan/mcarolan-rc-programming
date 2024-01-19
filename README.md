# TimeZoneDB Data Extractor Script

A script to import data from the [TimeZoneDB](https://timezonedb.com/references/get-time-zone) API into PostgreSQL with error logging.

## Description

Run `script.py` to query the TimeZoneDB API and populate the tables TZDB_TIMEZONES and TZDB_ZONE_DETAILS.

Errors when retrieving the API are logged to the table TZDB_ERROR_LOG.

The TZDB_TIMEZONES table is deleted every time the script runs, and then it is populated with the latest data from the API.

TZDB_ZONE_DETAILS is populated incrementally, if a new row is loaded, it will be added to the table, otherwise, the existing row will be left alone. This is accomplished using a staging table and a left join using the composite primary key to check for uniqueness.

## Getting Started

### Dependencies

* Python (3.12.0)
* Pip (23.2.1)
* PostgreSQL (16.1)

### Setup

1. Clone this repo
2. Make a copy of `config_sample.py` named `config.py` in the same directory as `script.py`.
   * This file will contain secrets, so it should never be committed. `config.py` is already in the .gitignore file.
3. To get access to the API provided by TimezoneDB go to: https://timezonedb.com and create a free
account.
4. Fill in your API key and the connection parameters for your PostgreSQL database in `config.py`.
5. Run `pip install -r requirements.txt`

### Database Creation

1. Open a `psql` terminal with the same credentials you used in `config.py`.
2. Create a new database:
   * `CREATE DATABASE "ROYAL_CARIBBEAN";`
3. Connect to your database:
   * `\c "ROYAL_CARIBBEAN"`
4. Create the **TZDB_TIMEZONES** table:
```
CREATE TABLE "TZDB_TIMEZONES" (
    "COUNTRYCODE" VARCHAR(2) NOT NULL,
    "COUNTRYNAME" VARCHAR(100) NOT NULL,
    "ZONENAME" VARCHAR(100) PRIMARY KEY NOT NULL,
    "GMTOFFSET" INTEGER,
    "IMPORT_DATE" TIMESTAMP
);
```
5. Create the **TZDB_ZONE_DETAILS** table:
```
CREATE TABLE "TZDB_ZONE_DETAILS" (
    "COUNTRYCODE" VARCHAR(2) NOT NULL,
    "COUNTRYNAME" VARCHAR(100) NOT NULL,
    "ZONENAME" VARCHAR(100) NOT NULL,
    "GMTOFFSET" INTEGER NOT NULL,
    "DST" SMALLINT NOT NULL,
    "ZONESTART" BIGINT NOT NULL,
    "ZONEEND" BIGINT NOT NULL,
    "IMPORT_DATE" TIMESTAMP,
    PRIMARY KEY ("ZONENAME", "ZONESTART", "ZONEEND")
);
```
6. Create the **TZDB_ERROR_LOG** table:
```
CREATE TABLE "TZDB_ERROR_LOG" (
    "ERROR_DATE" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "ERROR_MESSAGE" VARCHAR(1000) NOT NULL
);
```

### Executing program

Run `script.py` to connect to the API and load the data into your database. The script takes about 15 minutes to run. This could be sped up with multiple API accounts parallelizing requests.

### Troubleshooting

Errors that occur during data processing will be logged to the **TZDB_ERROR_LOG** table. Common errors include:
* Rate limiting (the rate limit for a free account is 1 request/second)
* Invalid API key

An error prefixed with "Python Script Error:" signals that something has gone wrong with the import script, and it may need to be updated.