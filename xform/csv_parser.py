import sys
import csv
import json
from xform.base_parser import BaseParser


class CSVParser(BaseParser):
    """
    Reads CSV or TSV data from stdin and outputs JSON.
    Automatically detects delimiter (comma or tab).
    """

    def _detect_delimiter(self, csv_content: str) -> str:
        """
        Detects whether the input is CSV (comma-separated) or TSV (tab-separated).
        """
        sample_lines = csv_content.splitlines()[:5]  # Take first few lines as a sample
        comma_count = sum(line.count(",") for line in sample_lines)
        tab_count = sum(line.count("\t") for line in sample_lines)

        return (
            "\t" if tab_count > comma_count else ","
        )  # Assume tab if more tabs than commas

    def _extract_records(self, csv_content: str) -> list[dict]:
        """
        Reads CSV/TSV content and returns a list of records as JSON.
        Automatically detects delimiter.
        """
        delimiter = self._detect_delimiter(csv_content)
        reader = csv.DictReader(csv_content.splitlines(), delimiter=delimiter)
        return [row for row in reader]  # No extra processing, just direct conversion


def main():
    """
    Reads CSV or TSV from stdin and outputs JSON.
    Usage examples:
        cat file.csv | python3 csv_parser.py
        cat file.tsv | python3 csv_parser.py
    """
    csv_content = sys.stdin.read()
    parser = CSVParser()
    parsed_data = parser.parse(csv_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
