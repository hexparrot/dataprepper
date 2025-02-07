#!/usr/bin/env python3
import sys
import os
import json
import logging
import html
import re
from bs4 import BeautifulSoup, NavigableString, Tag
from datetime import datetime

# Ensure Python finds the project modules no matter where the script is run
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
from xform.base_parser import BaseParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class MsnParser(BaseParser):
    """
    Parser for MSN chat logs in HTML format.
    Reads from stdin and outputs structured JSON to stdout.
    """

    def __init__(self, date_str=""):
        """
        Initialize the parser with an optional date string in 'YYYY-MM-DD' format.
        If not provided, the date must be extracted from the <title> tag.
        """
        self.date_str = date_str

    def _extract_records(self, html_content: str) -> list[dict]:
        """
        Extract raw chat records from MSN chat logs.
        :param html_content: Raw HTML content as a string.
        :return: List of dictionaries with 'author', 'message', and 'timestamp'.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        raw_records = []

        # Determine the date to use
        self.date_str = (
            self._extract_date_from_title(soup) if not self.date_str else self.date_str
        )

        # Find all <font> tags with a color attribute (used to indicate message author)
        font_tags = soup.find_all("font", color=re.compile(r"^#"))

        for font in font_tags:
            try:
                timestamp = self._extract_timestamp(font)
                author = self._extract_author(font)
                message = self._extract_message(font)

                if timestamp and author and message:
                    raw_records.append(
                        {
                            "author": author,
                            "message": message,
                            "timestamp": timestamp,
                        }
                    )
                else:
                    logging.warning("Skipping incomplete chat entry.")
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                continue

        return raw_records

    def _extract_date_from_title(self, soup):
        """
        Extract the date from the <title> tag in 'MM/DD/YYYY' format.
        :param soup: BeautifulSoup object of the HTML content.
        :return: Date string in 'YYYY-MM-DD' format or None if not found.
        """
        title_tag = soup.find("title")
        if not title_tag:
            return None

        title_text = title_tag.get_text(strip=True)
        try:
            date_part = title_text.split(" at ")[1].split(" ")[0]
            date_obj = datetime.strptime(date_part, "%m/%d/%Y")
            return date_obj.strftime("%Y-%m-%d")
        except (IndexError, ValueError):
            return None

    def _extract_timestamp(self, font):
        """
        Extract and format the timestamp from the nested <font> tag.
        :param font: The parent <font> tag containing the nested timestamp.
        :return: ISO 8601 formatted timestamp or None if invalid.
        """
        nested_font = font.find("font", size="2")
        if not nested_font:
            return None

        timestamp_text = nested_font.get_text(strip=True).strip("()")
        try:
            time_obj = datetime.strptime(timestamp_text, "%I:%M:%S %p")
            date_part = self.date_str if self.date_str else "1970-01-01"
            return f"{date_part}T{time_obj.strftime('%H:%M:%S')}"
        except ValueError:
            logging.warning(f"Invalid timestamp format: {timestamp_text}")
            return None

    def _extract_author(self, font):
        """
        Extract the author from the <b> tag inside the <font> tag.
        :param font: The <font> tag containing the <b> tag with the author name.
        :return: Unescaped author text or None if not found.
        """
        b_tag = font.find("b")
        if not b_tag:
            return None

        author_text = b_tag.get_text(strip=True).rstrip(":")
        return html.unescape(author_text)

    def _extract_message(self, font):
        """
        Extract the message content, which is the next sibling of the <font> tag.
        :param font: The <font> tag whose sibling contains the message content.
        :return: Extracted message as a string or None if not found.
        """
        message_element = font.next_sibling
        if not message_element:
            return None

        while (
            isinstance(message_element, NavigableString) and not message_element.strip()
        ):
            message_element = message_element.next_sibling

        if isinstance(message_element, NavigableString):
            return message_element.strip()
        elif isinstance(message_element, Tag):
            return message_element.get_text(strip=True)
        return None


def main():
    """
    Reads MSN chat logs in HTML from stdin and outputs JSON to stdout.
    """
    html_content = sys.stdin.read()
    parser = MsnParser()
    parsed_data = parser.parse(html_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
