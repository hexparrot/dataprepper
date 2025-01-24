#!/usr/bin/env python3
import sys
import json
from format_d_parser import FormatDParser


def main():
    """
    Main function to parse HTML input and output JSON to stdout.
    """
    # Read HTML content from stdin
    html_content = sys.stdin.read()

    # Initialize the parser
    parser = FormatDParser(date_str="1970-01-01")  # Use the date from the filename

    try:
        # Parse the HTML content
        records = parser.parse(html_content)

        # Debugging: Print parsed records
        print(json.dumps(records, indent=4))

    except Exception as e:
        print(f"Error occurred: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
