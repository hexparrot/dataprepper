#!/usr/bin/env python3
import os
import csv
import sys
from format_a_parser import FormatAParser
from format_b_parser import FormatBParser
from format_c_parser import FormatCParser
from format_d_parser import FormatDParser
from datetime import datetime


def extract_date_from_filename(filename):
    """
    Extract the date from the filename in 'YYYY-MM-DD' format.
    :param filename: The name of the file.
    :return: Extracted date as a string or None if not found.
    """
    try:
        # Example: '2005-08-13 [Saturday].htm' -> '2005-08-13'
        date_part = filename.split(" ")[0]
        datetime.strptime(date_part, "%Y-%m-%d")  # Validate the format
        return date_part
    except (ValueError, IndexError):
        return None


def validate_records(records):
    """
    Validate records for completeness of ISO 8601 timestamps.
    :param records: List of parsed records.
    :return: Tuple (valid_count, is_complete).
    """
    valid_count = 0
    is_complete = True

    for record in records:
        if "timestamp" not in record or not record["timestamp"]:
            is_complete = False
        else:
            try:
                # Validate ISO 8601 format
                datetime.fromisoformat(record["timestamp"])
                valid_count += 1
            except ValueError:
                is_complete = False

    return valid_count, is_complete


def main():
    # Parsers to evaluate
    parsers = {
        "FormatA": FormatAParser(),
        "FormatB": FormatBParser(),
        "FormatC": FormatCParser(),
        "FormatD": FormatDParser(),
    }

    # HTML files to test
    test_dir = "../testdata/html"
    html_files = [
        f for f in os.listdir(test_dir) if f.endswith(".htm") or f.endswith(".html")
    ]

    # Performance results
    results = {}

    for file in html_files:
        file_path = os.path.join(test_dir, file)
        results[file] = {}

        # Extract date from filename
        file_date = extract_date_from_filename(file)

        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            html_content = f.read()

        for parser_name, parser in parsers.items():
            # Inject date into parser if needed
            if hasattr(parser, "date_str"):
                parser.date_str = file_date

            # Parse the file
            records = parser.parse(html_content)

            # Validate records
            valid_count, is_complete = validate_records(records)
            results[file][parser_name] = {
                "records_parsed": len(records),
                "valid_records": valid_count,
                "complete_timestamps": is_complete,
            }

    # Write the performance table to CSV
    csv_headers = ["File", *parsers.keys()]
    csv_rows = []

    for file, parser_results in results.items():
        row = [file]
        for parser_name in parsers.keys():
            result = parser_results.get(parser_name, {})
            row.append(
                result.get("valid_records", 0)
            )  # Use valid_records as the metric
        csv_rows.append(row)

    # Output CSV to stdout
    writer = csv.writer(sys.stdout)
    writer.writerow(csv_headers)  # Write headers
    writer.writerows(csv_rows)  # Write data rows


if __name__ == "__main__":
    main()
