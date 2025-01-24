#!/usr/bin/env python3
import sys
import json
from format_a_parser import FormatAParser


def main():
    """
    Main function to parse HTML input and output JSON to stdout.
    """
    # Read HTML content from stdin
    html_content = sys.stdin.read()

    # Initialize the parser
    parser = FormatAParser(date_str="2005-08-13")  # Customize date if needed

    try:
        # Parse the HTML content
        records = parser.parse(html_content)

        # Output the parsed JSON to stdout
        print(json.dumps(records, indent=4))

    except Exception as e:
        print(f"Error occurred: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
