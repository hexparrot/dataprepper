#!/usr/bin/env python3
import sys
import json
from xform.format_a_parser import FormatAParser
from xform.format_b_parser import FormatBParser
from xform.format_c_parser import FormatCParser
from xform.format_d_parser import FormatDParser


def main():
    """
    Main function to parse HTML input using multiple parsers, select the best output, and report results.
    """
    # Extract optional command-line argument for date
    if len(sys.argv) > 2:
        print("Usage: ./parse_html.py [YYYY-MM-DD]", file=sys.stderr)
        sys.exit(1)

    date_str = sys.argv[1] if len(sys.argv) == 2 else "1970-01-01"

    # Validate the date format if provided
    if len(date_str) != 10 or not (
        date_str[4] == "-"
        and date_str[7] == "-"
        and date_str[:4].isdigit()
        and date_str[5:7].isdigit()
        and date_str[8:].isdigit()
    ):
        print("Invalid date format. Use YYYY-MM-DD.", file=sys.stderr)
        sys.exit(1)

    # Read HTML content from stdin
    html_content = sys.stdin.read()

    # Initialize all available parsers with the provided or default date
    parsers = {
        "FormatA": FormatAParser(date_str=date_str),
        "FormatB": FormatBParser(),
        "FormatC": FormatCParser(date_str=date_str),
        "FormatD": FormatDParser(date_str=date_str),
    }

    best_parser = None
    best_records = []
    parser_performance = {}

    # Iterate through all parsers and evaluate performance
    for parser_name, parser in parsers.items():
        try:
            # Parse the HTML content with the current parser
            records = parser.parse(html_content)

            # Log the number of records parsed
            parser_performance[parser_name] = len(records)

            # Update the best parser if this one has more records
            if len(records) > len(best_records):
                best_parser = parser_name
                best_records = records

        except Exception as e:
            # Log parser errors to stderr
            print(f"Error with {parser_name}: {e}", file=sys.stderr)
            parser_performance[parser_name] = 0  # No records parsed due to error

    # Output the parsed records using the best parser to stdout
    print(json.dumps(best_records, indent=4))

    # Report parser performance and chosen parser to stderr
    print("\nParser Performance:", file=sys.stderr)
    for parser_name, record_count in parser_performance.items():
        print(f"{parser_name}: {record_count} records parsed", file=sys.stderr)

    print(
        f"\nBest Parser: {best_parser} ({len(best_records)} records)", file=sys.stderr
    )


if __name__ == "__main__":
    main()
