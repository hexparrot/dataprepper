import sys
import csv
import json
from xform.base_parser import BaseParser


class CSVParser(BaseParser):
    """
    Reads CSV data from stdin and outputs JSON.
    """

    def _extract_records(self, csv_content: str) -> list[dict]:
        """
        Reads CSV content from stdin and returns a list of records as JSON.
        """
        reader = csv.DictReader(csv_content.splitlines())
        return [row for row in reader]  # No extra processing, just direct conversion


def main():
    """
    Reads CSV from stdin and outputs JSON.
    Usage example:
        cat file.csv | python3 csv_parser.py
    """
    csv_content = sys.stdin.read()
    parser = CSVParser()
    parsed_data = parser.parse(csv_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
