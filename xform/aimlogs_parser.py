import sys
import json
from bs4 import BeautifulSoup
from datetime import datetime
from xform.base_parser import BaseParser


class AimLogsParser(BaseParser):
    """
    Parses AIM logs from HTML format.
    Reads from stdin and outputs structured JSON to stdout.
    """

    def __init__(self, date_str=None):
        """
        Initialize the parser with an optional date string (YYYY-MM-DD format).
        Defaults to '1970-01-01' if not provided.
        """
        self.date_str = date_str if date_str else "1970-01-01"

    def _extract_records(self, html_content: str) -> list[dict]:
        """
        Extracts chat records from AIM HTML logs.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        raw_records = []
        span_elements = soup.find_all(
            "span", style=lambda v: v and "background-color: #ffffff" in v.lower()
        )

        for span in span_elements:
            try:
                author, timestamp = self._extract_author_and_timestamp(span)
                if author and timestamp:
                    message = self._extract_message(span)
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

    def _extract_author_and_timestamp(self, span):
        """
        Extracts author and timestamp from a given chat entry span.
        """
        first_font = span.find(
            "font", color=lambda v: v and v.lower() in ["#ff0000", "#0000ff"]
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
            return author, f"{self.date_str}T{time_obj.strftime('%H:%M:%S')}"
        except ValueError:
            return None, None

    def _extract_message(self, span):
        """
        Extracts the message from a chat entry.
        """
        font_tags = span.find_all("font")
        if not font_tags:
            return None

        message_font = font_tags[-1]
        message = message_font.get_text(strip=True)
        return message if message and message != ":" else None


def main():
    """
    Reads AIM log HTML from stdin and outputs JSON to stdout.
    """
    date_str = sys.argv[1] if len(sys.argv) > 1 else None
    parser = AimLogsParser(date_str)
    html_content = sys.stdin.read()
    parsed_data = parser.parse(html_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
