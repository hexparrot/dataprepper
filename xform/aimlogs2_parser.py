import sys
import json
import re
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser as date_parser
from xform.base_parser import BaseParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class AimLogs2Parser(BaseParser):
    """
    Parser for AIM chat logs with timestamps inside <span> tags.
    Reads from stdin and outputs structured JSON to stdout.
    """

    def __init__(self, date_str: str):
        """
        Initializes the parser with a provided date (YYYY-MM-DD).
        :param date_str: Date string in format YYYY-MM-DD to prepend to parsed timestamps.
        """
        self.date_str = date_str

    def _extract_records(self, html_content: str) -> list[dict]:
        """
        Extract chat messages from an HTML log where each message is stored in a <span> tag.
        Extracts author, timestamp, and message from correctly formatted spans.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        raw_records = []

        for span in soup.find_all("span"):
            text = span.get_text(strip=True)

            # Match pattern: AUTHOR (TIMESTAMP): MESSAGE
            match = re.match(r"^(.*)\((\d{1,2}:\d{2}:\d{2} (?:AM|PM))\):\s?(.*)$", text)

            if match:
                author, raw_timestamp, message = match.groups()
                timestamp = self._format_timestamp(raw_timestamp)

                if timestamp and message.strip():
                    raw_records.append(
                        {
                            "author": author.strip(),
                            "timestamp": timestamp,
                            "message": message.strip(),
                        }
                    )
            else:
                logging.debug(f"Skipping unrecognized span: {text}")

        return raw_records

    def _format_timestamp(self, raw_timestamp: str) -> str:
        """
        Format the raw timestamp into ISO 8601 using the provided date.
        """
        try:
            full_timestamp = f"{self.date_str} {raw_timestamp}"
            timestamp_obj = date_parser.parse(full_timestamp)
            return timestamp_obj.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            logging.warning(f"Failed to parse timestamp: {raw_timestamp}")
            return None


def main():
    """
    Reads AIM log HTML from stdin and outputs JSON to stdout.
    """
    if len(sys.argv) < 2:
        print("Usage: python aimlogs2_parser.py <YYYY-MM-DD>", file=sys.stderr)
        sys.exit(1)

    date_str = sys.argv[1]
    parser = AimLogs2Parser(date_str)
    html_content = sys.stdin.read()
    parsed_data = parser.parse(html_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
