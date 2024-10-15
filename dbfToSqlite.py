#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Modified dbf2sqlite - convert dbf files into an SQLite database with an additional "mandant" column
and avoids inserting duplicate entries.
"""

import sys
import sqlite3
import traceback
import os
import logging
import configparser
from dbfread import DBF
from datetime import date, datetime

def setup_logging(config):
    log_level_str = config['Logging'].get('level', 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.WARNING)
    log_targets = [target.strip().lower() for target in config['Logging'].get('targets', 'console').split(',')]
    log_file_path = config['Logging'].get('file_path', 'dbf2sqlite.log')

    # Clear existing handlers (if any)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    handlers = []
    if 'console' in log_targets:
        handlers.append(logging.StreamHandler(sys.stdout))  # Ensure output to stdout
    if 'file' in log_targets:
        handlers.append(logging.FileHandler(log_file_path))

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

def load_config(config_path='dbfToSqlite.ini'):
    config = configparser.ConfigParser()
    try:
        config.read(config_path)
        logging.info(f"Configuration loaded from {config_path}")
        return config
    except configparser.Error as e:
        logging.error(f"Error reading configuration file: {e}")
        sys.exit(1)

# Mapping of DBF field types to SQLite types
typemap = {
    'F': 'FLOAT',
    'L': 'BOOLEAN',
    'I': 'INTEGER',
    'C': 'TEXT',
    'N': 'REAL',  # because it can be integer or float
    'M': 'TEXT',
    'D': 'DATE',
    'T': 'DATETIME',
    '0': 'INTEGER',
}

def adapt_date(value):
    """Adapt date objects to string format for SQLite."""
    return value.isoformat()

def adapt_datetime(value):
    """Adapt datetime objects to string format for SQLite."""
    return value.isoformat()

# Register adapters for date and datetime types
sqlite3.register_adapter(date, adapt_date)
sqlite3.register_adapter(datetime, adapt_datetime)

def create_table_if_not_exists(cursor, table):
    """Create a SQLite table if it does not already exist, with an 'id' primary key and additional 'mandant' column."""

    # Map DBF field types to SQLite field types
    field_types = {field.name: typemap.get(field.type, 'TEXT') for field in table.fields}

    # Define table schema with an 'id' primary key, existing fields, and an additional "mandant" column
    defs = ['"id" INTEGER PRIMARY KEY AUTOINCREMENT']
    defs += [f'"{f}" {field_types[f]}' for f in table.field_names]
    defs.append('"mandant" TEXT')  # Additional column to store the mandant

    # Create the table in SQLite if it does not already exist
    sql = f'CREATE TABLE IF NOT EXISTS "{table.name}" ({", ".join(defs)})'
    cursor.execute(sql)

def load_existing_records(cursor, table_name, mandant):
    """Load existing records for a specific mandant from the SQLite table."""
    query = f'SELECT wz__pat, wz__dat, wz_time FROM "{table_name}" WHERE mandant = ?'
    cursor.execute(query, (mandant,))
    return {(str(row[0]), str(row[1]), str(row[2])) for row in cursor.fetchall()}

def insert_records(cursor, table, mandant):
    """Insert records from the DBF table into the SQLite database with an additional 'mandant' column."""

    # Load all existing records for the current mandant into a set
    existing_records = load_existing_records(cursor, table.name, mandant)

    # Create placeholders for the number of fields
    placeholders = ', '.join(['?' for _ in table.field_names])
    placeholders += ', ?'  # Placeholder for "mandant"

    # Prepare the SQL insert statement
    sql = f'INSERT INTO "{table.name}" ({", ".join(["id"] + table.field_names + ["mandant"])}) VALUES (NULL, {placeholders})'

    logging.info(f'Processing mandant {mandant}...')

    total_records = len(table)  # Total number of records in the DBF file
    inserted_count = 0

    # Insert each record from the DBF file into the SQLite table
    for i, rec in enumerate(table, start=1):
        rec_dict = dict(rec)

        # Create a tuple for checking, ensuring all elements are strings
        rec_tuple = (str(rec_dict['wz__pat']), 
                     rec_dict['wz__dat'].isoformat() if isinstance(rec_dict['wz__dat'], date) else str(rec_dict['wz__dat']), 
                     str(rec_dict['wz_time']))

        # Check if the record already exists in the existing records set
        if rec_tuple not in existing_records:
            cursor.execute(sql, list(rec.values()) + [mandant])
            inserted_count += 1
            existing_records.add(rec_tuple)  # Add the new record to the set

        # Log progress every 1000 records
        if i % 1000 == 0 or i == total_records:
            logging.info(f'Processed {i}/{total_records} records. Inserted: {inserted_count}')

    logging.info(f'Completed processing {total_records} records for mandant {mandant}. Total inserted: {inserted_count}')

def main():
    config = load_config()
    setup_logging(config)

    logging.info("Starting DBF to SQLite conversion process")

    directories = dict(config['Directories'])
    settings = config['Settings']
    output_file = settings.get('output_file') or None
    encoding = settings.get('encoding') or None
    char_decode_errors = settings.get('char_decode_errors') or 'strict'


    # Connect to the SQLite database, or create it in memory if no output file is specified
    conn = sqlite3.connect(output_file or ':memory:')
    cursor = conn.cursor()

    # Process each directory and corresponding DBF file
    for mandant, directory in directories.items():
        dbf_file_path = os.path.join(directory, 'el_pwz.dbf')

        if not os.path.exists(dbf_file_path):
            logging.warning(f"DBF file '{dbf_file_path}' not found in directory '{directory}'. Skipping.")
            continue

        try:
            logging.info(f"Processing DBF file: {dbf_file_path}")
            # Read the DBF file
            dbf_table = DBF(dbf_file_path,
                            load=True,
                            lowernames=True,
                            encoding=encoding,
                            char_decode_errors=char_decode_errors)

            # Create the SQLite table if it doesn't exist
            create_table_if_not_exists(cursor, dbf_table)

            # Insert records from the DBF file into the SQLite table
            insert_records(cursor, dbf_table, mandant)

        except UnicodeDecodeError:
            logging.error(f"UnicodeDecodeError encountered while processing {dbf_file_path}")
            traceback.print_exc()
            sys.exit('Please use encoding or char-decode-errors.')

    # Commit the changes to the database
    conn.commit()

    # If no output file is specified, dump the SQL schema and data to stdout
    if not output_file:
        for line in conn.iterdump():
            print(line)
    else:
        logging.info(f"Data successfully written to {output_file}")

    # Close the database connection
    conn.close()
    logging.info("DBF to SQLite conversion process completed")

if __name__ == '__main__':
    main()