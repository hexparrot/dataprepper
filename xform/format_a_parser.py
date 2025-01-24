from bs4 import BeautifulSoup
from datetime import datetime
from base_parser import BaseParser


class FormatAParser(BaseParser):
    """
    Parser for chat logs in Format A.
    """

    def __init__(self, date_str="2005-08-13"):
        """
        Initialize the parser with the date string in 'YYYY-MM-DD' format.
        """
        self.date_str = date_str

    def _extract_records(self, html_content: str) -> list[dict]:
        """
        Extract raw records for Format A.
        :param html_content: Raw HTML content as a string.
        :return: List of dictionaries with 'author', 'message', and 'timestamp'.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        raw_records = []

        # Find all <span> elements with background-color #ffffff
        span_elements = soup.find_all(
            "span",
            style=lambda value: value and "background-color: #ffffff" in value.lower(),
        )

        for span in span_elements:
            try:
                author, timestamp = self._extract_author_and_timestamp(span)
                if not author or not timestamp:
                    continue

                message = self._extract_message(span)
                if not message:
                    continue

                raw_records.append(
                    {
                        "author": author,
                        "message": message,
                        "timestamp": timestamp,
                    }
                )
            except Exception:
                continue

        return raw_records

    def _extract_author_and_timestamp(self, span):
        """
        Extract author and timestamp from a span element.
        :param span: The span element to parse.
        :return: Tuple (author, timestamp) or (None, None) if not found.
        """
        first_font = span.find(
            "font",
            color=lambda value: value and value.lower() in ["#ff0000", "#0000ff"],
        )
        if not first_font:
            return None, None

        author = first_font.get_text(strip=True).split("(")[0].strip()

        time_span = span.find(
            "span", style=lambda value: value and "font-size: xx-small" in value.lower()
        )
        if not time_span:
            return None, None

        time_text = time_span.get_text(strip=True).strip("()")
        try:
            time_obj = datetime.strptime(time_text, "%I:%M:%S %p")
            timestamp = f"{self.date_str}T{time_obj.strftime('%H:%M:%S')}"
            return author, timestamp
        except ValueError:
            return None, None

    def _extract_message(self, span):
        """
        Extract the message content from the span.
        :param span: The span element to parse.
        :return: Message text or None if not found.
        """
        font_tags = span.find_all("font")
        if not font_tags:
            return None

        message_font = font_tags[-1]
        message = message_font.get_text(strip=True)
        return message if message and message != ":" else None
