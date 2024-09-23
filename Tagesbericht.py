#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import sqlite3
import pandas as pd
import openpyxl
from openpyxl.styles import PatternFill
from datetime import datetime

# List of mandants
mandants = {
    'A': 'Allgemeinmedizin',
    'B': 'Kinderheilkunde',
    'D': 'HNO',
    'E': 'Augenheilkunde',
}

column_names = {
    'wz__pat': 'EL Nr.',
    'wz_name': 'Name',
    'wz__dat': 'Datum',
    'wz_time': 'Zeit',
    'wz__geb': 'Geburtstag',
    'wz__sys': 'System',
    'wz__gnr': 'GNR',
    'wz_term': 'Termin',
    'wz___bg': 'BG Fall',
    'wz__bem': 'Bemerkung',
    'wz_ziel': 'Ziel',
    'wz__vpk': 'Unbekannt',
    'wz_kknr': 'Krankenkassennummer',
    'wz_ktgr': 'Kostenträgergruppe',
    'wz__hvm': 'Unbekannt',
    'wz_prxg': 'Praxisgebühr',
    'wz_gone': 'Verlassen',
    'mandant': 'Mandant'
}

# columns = ['wz__pat', 'wz_name', 'wz__dat', 'wz_time', 'wz__geb']

def format_date(date_string):
    if pd.isna(date_string) or date_string == '':
        return ''
    try:
        return datetime.strptime(date_string, '%Y-%m-%d').strftime('%d.%m.%Y')
    except ValueError:
        return date_string

def get_data_from_sqlite(db_path, mandant, columns, date_from=None, date_to=None):
    conn = sqlite3.connect(db_path)
    query = f"SELECT {', '.join(columns)} FROM el_pwz WHERE mandant = ?"
    params = [mandant]

    if date_from:
        query += " AND wz__dat >= ?"
        params.append(date_from)
    if date_to:
        query += " AND wz__dat <= ?"
        params.append(date_to)

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def create_excel_sheet(workbook, sheet_name, df):
    sheet = workbook.create_sheet(title=sheet_name)
    
    fill = PatternFill(start_color="808080", end_color="808080", fill_type="solid")

    # Write the column headers
    for col, header in enumerate(df.columns, start=1):
        cell = sheet.cell(row=1, column=col, value=column_names.get(header, header))
        cell.fill = fill
        sheet.column_dimensions[cell.column_letter].width = max(len(column_names[header]), 10)


    # Write the data
    for row, data in enumerate(df.values, start=2):
        for col, value in enumerate(data, start=1):
            #sheet.cell(row=row, column=col, value=value)

            cell = sheet.cell(row=row, column=col, value=value)
            
            # Adjust column width based on the length of the content in each cell
            column_letter = cell.column_letter
            current_width = sheet.column_dimensions[column_letter].width
            value_length = len(str(value)) if value else 0
            
            # Set the new width if the value is longer than the current width
            if value_length > current_width:
                sheet.column_dimensions[column_letter].width = value_length


def sqlite_to_xlsx(db_path, xlsx_path, columns, date_from=None, date_to=None):
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)  # Remove the default sheet

    for mandant, name in mandants.items():
        df = get_data_from_sqlite(db_path, mandant, columns, date_from, date_to)
        
        if not df.empty:
            # Format date columns
            for col in ['wz__dat', 'wz__geb']:
                if col in df.columns:
                    df[col] = df[col].apply(format_date)

            create_excel_sheet(workbook, name, df)

    workbook.save(xlsx_path)
    print(f"Excel file created successfully at {xlsx_path}")

def parse_args():
    parser = argparse.ArgumentParser(description='Convert SQLite data to XLSX with flexible data selection.')
    parser.add_argument('-o', '--output-file', default='Tagesbericht.xlsx', help='XLSX file to write to (default: Tagesbericht.xlsx)')
    parser.add_argument('-c', '--columns', nargs='+', default=['wz__pat', 'wz_name', 'wz__dat', 'wz_time', 'wz__geb'], help='Columns to include in the report')
    parser.add_argument('-df', '--date-from', help='Start date for the report (YYYY-MM-DD)')
    parser.add_argument('-dt', '--date-to', help='End date for the report (YYYY-MM-DD)')
    parser.add_argument('-d', '--date', help='Date for the report (YYYY-MM-DD)')
    parser.add_argument('-db', '--database', default='out.sqlite', help='SQLite database file (default: out.sqlite)')
    return parser.parse_args()

def main():
    args = parse_args()
    if args.date:
        output_file = f'Tagesbericht_{args.date}.xlsx'
        sqlite_to_xlsx(args.database, output_file, args.columns, args.date, args.date)
    else:
        sqlite_to_xlsx(args.database, args.output_file, args.columns, args.date_from, args.date_to)

if __name__ == '__main__':
    main()