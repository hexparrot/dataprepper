from bs4 import BeautifulSoup
from datetime import datetime
from xform.base_parser import BaseParser


class AimLogsParser(BaseParser):
    """
    Combined parser for multiple AIM client formats
    """

    def __init__(self, date_str=None):
        """
        Initialize the parser with the provided date string in 'YYYY-MM-DD' format.
        If no date is provided, default to '1970-01-01'.
        """
        self.date_str = date_str if date_str else "1970-01-01"  # ✅ Use provided date

    def _extract_records(self, html_content: str) -> list[dict]:
        """
        Extract raw records using combined logic from Format A and Format D.
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
                # Try parsing with Format A logic
                author, timestamp = self._extract_author_and_timestamp_a(span)
                if author and timestamp:
                    message = self._extract_message_a(span)
                    if message:
                        raw_records.append(
                            {
                                "author": author,
                                "message": message,
                                "timestamp": timestamp,
                            }
                        )
                        continue

                # If Format A fails, fallback to Format D logic
                author, timestamp = self._extract_author_and_timestamp_d(span)
                if author and timestamp:
                    message = self._extract_message_d(span)
                    if message:
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

    # Format A methods
    def _extract_author_and_timestamp_a(self, span):
        """
        Extract author and timestamp using Format A logic.
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
            timestamp = f"{self.date_str}T{time_obj.strftime('%H:%M:%S')}"  # ✅ Uses extracted date
            return author, timestamp
        except ValueError:
            return None, None

    def _extract_message_a(self, span):
        """
        Extract the message using Format A logic.
        """
        font_tags = span.find_all("font")
        if not font_tags:
            return None

        message_font = font_tags[-1]
        message = message_font.get_text(strip=True)
        return message if message and message != ":" else None

    # Format D methods
    def _extract_author_and_timestamp_d(self, span):
        """
        Extract author and timestamp using Format D logic, with handling for malformed timestamps.
        """
        first_font = span.find(
            "font",
            color=lambda value: value and value.lower() in ["#ff0000", "#0000ff"],
        )
        if not first_font:
            return None, None

        author = first_font.get_text(strip=True).split("(")[0].strip()

        time_match = span.find(text=lambda text: text and "(" in text and ")" in text)
        if not time_match:
            return None, None

        time_text = time_match.strip().lstrip("(").rstrip(")")
        try:
            time_obj = datetime.strptime(time_text, "%I:%M:%S %p")
            timestamp = f"{self.date_str}T{time_obj.strftime('%H:%M:%S')}"  # ✅ Uses extracted date
            return author, timestamp
        except ValueError:
            time_text_normalized = time_text.replace("(", "").replace(")", "").strip()
            try:
                time_obj = datetime.strptime(time_text_normalized, "%I:%M:%S %p")
                timestamp = f"{self.date_str}T{time_obj.strftime('%H:%M:%S')}"  # ✅ Uses extracted date
                return author, timestamp
            except ValueError:
                return None, None

    def _extract_message_d(self, span):
        """
        Extract the message using Format D logic.
        """
        font_tags = span.find_all("font")
        if not font_tags:
            return None

        message_font = font_tags[-1]
        message = message_font.get_text(strip=True)
        return message if message and message != ":" else None
