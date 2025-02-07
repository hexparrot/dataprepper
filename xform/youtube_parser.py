import sys
import json
import re
import logging
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from xform.base_parser import BaseParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class YouTubeParser(BaseParser):
    """
    Parser for YouTube Watch History HTML.
    Extracts video title, URL, channel name, timestamp, product, and message.
    """

    def _extract_records(self, html_content: str) -> list[dict]:
        """
        Extracts watch history records from YouTube HTML.
        :param html_content: Raw HTML content.
        :return: List of dictionaries with structured data.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        records = []
        content_blocks = soup.find_all(
            "div", class_="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"
        )

        if not content_blocks:
            logging.warning("No content blocks found in YouTube watch history.")
            return records

        for block in content_blocks:
            try:
                # Extract video URL and title
                video_link = block.find(
                    "a", href=re.compile(r"^https://www\.youtube\.com/watch\?v=")
                )
                video_url = video_link["href"] if video_link else "Unknown"
                video_title = video_link.text.strip() if video_link else "Unknown Title"

                # Extract channel name
                channel_link = (
                    block.find_all("a")[1] if len(block.find_all("a")) > 1 else None
                )
                channel_name = (
                    channel_link.text.strip() if channel_link else "Unknown Channel"
                )

                # Extract timestamp
                text_content = block.get_text(separator=" ", strip=True)
                timestamp_match = re.search(
                    r"([A-Za-z]{3} \d{1,2}, \d{4}, \d{1,2}:\d{2}:\d{2} ?(?:AM|PM)?)",
                    text_content,
                )
                raw_timestamp = (
                    timestamp_match.group(1) if timestamp_match else "Unknown"
                )

                # Convert timestamp to ISO 8601
                iso_timestamp = "1970-01-01T00:00:00"
                if raw_timestamp != "Unknown":
                    try:
                        dt = date_parser.parse(raw_timestamp)
                        iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S")
                    except ValueError:
                        logging.warning(f"Failed to parse timestamp: {raw_timestamp}")

                # Construct record
                record = {
                    "title": video_title,
                    "url": video_url,
                    "channel": channel_name,
                    "timestamp": iso_timestamp,
                    "product": "YouTube",
                    "author": "unspecified",
                    "message": f"Playing {video_title} on YouTube",
                }
                records.append(record)

            except Exception as e:
                logging.error(f"Error parsing entry: {e}")
                continue

        return records


def main():
    """
    Reads HTML from stdin and outputs structured JSON.
    """
    html_content = sys.stdin.read().strip()
    if not html_content:
        logging.warning("No input provided. Exiting gracefully.")
        sys.exit(0)

    parser = YouTubeParser()
    parsed_data = parser.parse(html_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
