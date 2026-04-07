#!/usr/bin/env python3
import sys
import os
import json
import re
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser as date_parser

# Ensure Python finds the project modules no matter where the script is run
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
from xform.base_parser import BaseParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

TS_PATTERN = r"\d{1,2}:\d{2}:\d{2} (?:AM|PM)"


class AimLogs3Parser(BaseParser):
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

    def _split_blob(self, text: str) -> list[tuple]:
        """
        If a span's text contains multiple concatenated messages, split them apart.
        Returns a list of (author, raw_timestamp, message) tuples.
        """
        pattern = re.compile(
            r"(.*?)\((" + TS_PATTERN + r")\):\s?(.*?)(?=.+?\(" + TS_PATTERN + r"\):|$)"
        )
        matches = pattern.findall(text)
        return matches

    def _extract_records(self, html_content: str) -> list[dict]:
        soup = BeautifulSoup(html_content, "html.parser")
        raw_records = []

        for span in soup.find_all("span"):
            style = span.get("style", "")
            if "xx-small" in style:
                continue

            # Find the timestamp span
            ts_span = span.find("span", style=lambda s: s and "xx-small" in s)
            if not ts_span:
                continue

            raw_timestamp = ts_span.get_text(strip=True).strip("() ")
            timestamp = self._format_timestamp(raw_timestamp)
            if not timestamp:
                continue

            # Use get_text() on the whole span, then parse with non-greedy regex
            text = span.get_text(strip=True)

            match = re.match(r"^(.*?)\(" + TS_PATTERN + r"\):\s?(.*)$", text)
            if not match:
                logging.debug(f"Skipping unrecognized span: {text[:80]!r}")
                continue

            author = match.group(1).strip()
            message = match.group(2).strip()

            # Check for blob: invalid characters in author field indicate consolidation
            if re.search(r"[():]", author):
                logging.warning(f"Blob detected, attempting split: {text[:120]!r}")
                sub_records = self._split_blob(text)
                for sub_author, sub_raw_ts, sub_message in sub_records:
                    sub_author = sub_author.strip()
                    sub_message = sub_message.strip()
                    sub_timestamp = self._format_timestamp(sub_raw_ts)
                    if not sub_author or not sub_message or not sub_timestamp:
                        continue
                    if re.search(r"[():]", sub_author):
                        logging.warning(
                            f"Sub-record author still invalid, skipping: {sub_author!r}"
                        )
                        continue
                    raw_records.append(
                        {
                            "author": sub_author,
                            "timestamp": sub_timestamp,
                            "message": sub_message,
                        }
                    )
                continue  # skip normal append

            if author and message:
                raw_records.append(
                    {
                        "author": author,
                        "timestamp": timestamp,
                        "message": message,
                    }
                )

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
        print("Usage: python aimlogs3_parser.py <YYYY-MM-DD>", file=sys.stderr)
        sys.exit(1)

    date_str = sys.argv[1]
    parser = AimLogs3Parser(date_str)
    html_content = sys.stdin.read()
    parsed_data = parser.parse(html_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
