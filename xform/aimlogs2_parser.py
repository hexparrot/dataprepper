from bs4 import BeautifulSoup
from datetime import datetime
import re
from dateutil import parser as date_parser
from xform.base_parser import BaseParser


class AimLogs2Parser(BaseParser):
    """
    Parser for chat logs with timestamps inside <span> tags.
    Accepts a provided date (YYYY-MM-DD) since timestamps lack a date.
    """

    def __init__(self, date_str: str):
        """
        Initializes the parser with a provided date (YYYY-MM-DD).
        :param date_str: Date string in format YYYY-MM-DD to prepend to parsed timestamps.
        """
        super().__init__()
        self.date_str = date_str

    def _extract_records(self, html_content: str) -> list[dict]:
        """
        Extract chat messages from an HTML file where each message is stored in a <span> tag.
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

                # Normalize timestamp with provided date
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
                print(f"[DEBUG] Skipping unrecognized span: {text}")

        return raw_records

    def _format_timestamp(self, raw_timestamp: str) -> str:
        """
        Format the raw timestamp into ISO 8601 using the provided date.
        :param raw_timestamp: Time string (e.g., "12:48:44 PM").
        :return: Full ISO 8601 timestamp (YYYY-MM-DDTHH:MM:SS).
        """
        try:
            full_timestamp = f"{self.date_str} {raw_timestamp}"
            timestamp_obj = date_parser.parse(full_timestamp)
            return timestamp_obj.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            print(f"[DEBUG] Failed to parse timestamp: {raw_timestamp}")
            return None
