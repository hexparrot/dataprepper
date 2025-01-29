#!/usr/bin/env python3
import sys
import os
import pandas as pd
from importlib import import_module


def load_parser(letter):
    """
    Dynamically load the parser class based on the given letter.
    :param letter: Single letter representing the parser (e.g., "a" for FormatAParser).
    :return: The parser class.
    """
    try:
        module = import_module(f"xform.format_{letter.lower()}_parser")
        class_name = f"Format{letter.upper()}Parser"
        return getattr(module, class_name)
    except (ModuleNotFoundError, AttributeError) as e:
        print(
            f"Error: Could not load parser for letter '{letter}'. {e}", file=sys.stderr
        )
        sys.exit(1)


def parse_html_with_parser(file_path, parser, date_str="1970-01-01"):
    """
    Parse an HTML file using the specified parser.
    :param file_path: Path to the HTML file.
    :param parser: An instance of the parser to use.
    :param date_str: Date string in YYYY-MM-DD format.
    :return: List of records parsed.
    """
    try:
        with open(file_path, "r") as f:
            html_content = f.read()
        parser_instance = parser(date_str=date_str)
        return parser_instance.parse(html_content)
    except Exception as e:
        print(f"Error parsing {file_path} with {parser.__name__}: {e}", file=sys.stderr)
        return []


def compare_parsers(directory, parser1_letter, parser2_letter, date_str="1970-01-01"):
    """
    Compare two parsers across all HTML files in a directory.
    :param directory: Directory containing HTML files.
    :param parser1_letter: Letter of the first parser (e.g., "a").
    :param parser2_letter: Letter of the second parser (e.g., "d").
    :param date_str: Date string in YYYY-MM-DD format.
    """
    parser1 = load_parser(parser1_letter)
    parser2 = load_parser(parser2_letter)

    results = []
    for root, _, files in os.walk(directory):
        for file in files:
            print(file)
            if file.lower().endswith((".html", ".htm")):
                file_path = os.path.join(root, file)
                records1 = parse_html_with_parser(file_path, parser1, date_str)
                records2 = parse_html_with_parser(file_path, parser2, date_str)

                set1 = {str(record) for record in records1}
                set2 = {str(record) for record in records2}

                results.append(
                    {
                        "File": file,
                        f"Records_{parser1_letter.upper()}": len(records1),
                        f"Records_{parser2_letter.upper()}": len(records2),
                        "Common_Records": len(set1 & set2),
                        f"Unique_to_{parser1_letter.upper()}": len(set1 - set2),
                        f"Unique_to_{parser2_letter.upper()}": len(set2 - set1),
                    }
                )

    df = pd.DataFrame(results)

    # Print comparison results to stdout
    print("\nComparison Results:")
    print(df.to_string(index=False))


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: ./compare_parser.py <directory> <a|b|c|d> <a|b|c|d>")
        sys.exit(1)

    directory = sys.argv[1]
    parser1_letter = sys.argv[2].lower()
    parser2_letter = sys.argv[3].lower()

    compare_parsers(directory, parser1_letter, parser2_letter)
