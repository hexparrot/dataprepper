#!/usr/bin/env python3
import os
import json
from format_a_parser import FormatAParser  # Add more parsers here
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
        "FormatA": FormatAParser(),  # Add other parsers here
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

    # Output the performance table
    print("\nPerformance Table:\n")
    headers = ["File", *parsers.keys()]
    table = [headers]

    for file, parser_results in results.items():
        row = [file]
        for parser_name in parsers.keys():
            result = parser_results.get(parser_name, {})
            row.append(
                result.get("valid_records", 0)
            )  # Use valid_records as the primary metric
        table.append(row)

    # Print the table
    max_lengths = [max(len(str(row[i])) for row in table) for i in range(len(headers))]
    for row in table:
        print(
            " | ".join(str(row[i]).ljust(max_lengths[i]) for i in range(len(headers)))
        )


if __name__ == "__main__":
    main()
