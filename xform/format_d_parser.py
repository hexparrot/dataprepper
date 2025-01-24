from bs4 import BeautifulSoup
from datetime import datetime
import re
from base_parser import BaseParser


class FormatDParser(BaseParser):
    """
    Parser for chat logs in Format D.
    """

    def __init__(self, date_str=""):
        """
        Initialize the parser with an optional date string in 'YYYY-MM-DD' format.
        If not provided, the date must be inferred from context or filename.
        """
        self.date_str = date_str

    def _extract_records(self, html_content: str) -> list[dict]:
        """
        Extract raw records from the HTML content for Format D.
        :param html_content: Sanitized HTML content as a string.
        :return: List of dictionaries with 'author', 'message', and 'timestamp'.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        raw_records = []

        # Find all <span> tags with specific inline styles
        span_tags = soup.find_all(
            "span",
            style=lambda value: value and "background-color: #ffffff" in value.lower(),
        )

        for span in span_tags:
            try:
                # Extract the author
                author = self._extract_author(span)
                if not author:
                    continue

                # Extract the timestamp
                timestamp = self._extract_timestamp(span)
                if not timestamp:
                    continue

                # Extract the message
                message = self._extract_message(span)
                if not message:
                    continue

                # Append the raw record
                raw_records.append(
                    {
                        "author": author,
                        "message": message,
                        "timestamp": timestamp,
                    }
                )
            except Exception:
                # Skip spans that fail processing
                continue

        return raw_records

    def _extract_author(self, span):
        """
        Extract the author from the <b> tag inside the <font> tag within the <span>.
        Removes any parenthesized text from the author's name.
        :param span: The <span> element containing the author.
        :return: Extracted author as a string or None if not found.
        """
        font_tag = span.find("font", color="#ff0000")
        if not font_tag:
            return None

        bold_tag = font_tag.find("b")
        if bold_tag:
            # Strip parentheses and their contents from the author name
            author_text = bold_tag.get_text(strip=True)
            return re.sub(r"\s*\(.*?\)", "", author_text).strip()

        # Fallback if <b> is not found
        author_text = font_tag.get_text(strip=True)
        return re.sub(r"\s*\(.*?\)", "", author_text).strip()

    def _extract_timestamp(self, span):
        """
        Extract and format the timestamp from the nested <span> tag.
        :param span: The <span> element containing the timestamp.
        :return: ISO 8601 formatted timestamp or None if invalid.
        """
        time_span = span.find(
            "span", style=lambda value: value and "font-size: xx-small" in value.lower()
        )
        if not time_span:
            return None

        timestamp_text = time_span.get_text(strip=True).strip("()")
        try:
            time_obj = datetime.strptime(timestamp_text, "%I:%M:%S %p")
            date_part = self.date_str if self.date_str else "1970-01-01"
            return f"{date_part}T{time_obj.strftime('%H:%M:%S')}"
        except ValueError:
            return None

    def _extract_message(self, span):
        """
        Extract the message content, which is a sibling of the <font> tag.
        :param span: The <span> element containing the message.
        :return: Extracted message as a string or None if not found.
        """
        font_tags = span.find_all("font")
        for font in font_tags:
            if font.attrs.get("face") == "Tahoma" and font.attrs.get("size") == "1":
                return font.get_text(strip=True)
        return None
