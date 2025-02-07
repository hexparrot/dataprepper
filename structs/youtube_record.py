import sys
import json
import logging
from bs4 import BeautifulSoup
from structs.base_record import BaseRecord

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class YouTubeRecord(BaseRecord):
    """
    Represents an entry from a YouTube watch history.
    Reads from stdin and outputs structured JSON to stdout.
    """

    def __init__(self):
        """
        Initializes the YouTubeRecord.
        """
        super().__init__()

    @classmethod
    def parse(cls, html_content: str):
        """
        Parses a YouTube watch history HTML string and returns structured records.
        :param html_content: Raw HTML as a string.
        :return: List of structured YouTube watch records.
        """
        try:
            soup = BeautifulSoup(html_content, "html.parser")
        except Exception as e:
            logging.error(f"Failed to parse HTML content: {e}")
            return []

        youtube_records = cls._extract_entries(soup)
        if not youtube_records:
            logging.warning("No valid YouTube watch records found.")
        return youtube_records

    @staticmethod
    def _extract_entries(soup):
        """
        Extracts watch history entries from the parsed HTML.
        :param soup: BeautifulSoup object of the HTML.
        :return: List of parsed watch history entries.
        """
        records = []

        # Find all watch history entries
        for entry in soup.find_all(
            "div", class_="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp"
        ):
            try:
                title_tag = entry.find("a")
                title = title_tag.text.strip() if title_tag else "Unknown Title"
                url = (
                    title_tag["href"]
                    if title_tag and "href" in title_tag.attrs
                    else "Unknown"
                )

                timestamp_tag = entry.find("div", class_="metadata-cell")
                timestamp = (
                    timestamp_tag.text.strip()
                    if timestamp_tag
                    else "1970-01-01T00:00:00"
                )

                channel_tag = entry.find("a", class_="yt-simple-endpoint")
                channel = channel_tag.text.strip() if channel_tag else "Unknown Channel"

                record = {
                    "title": title,
                    "url": url,
                    "channel": channel,
                    "timestamp": timestamp,
                    "product": "YouTube",
                    "author": "unspecified",
                    "detail": f"Playing {title} on YouTube",
                }
                records.append(record)
            except Exception as e:
                logging.warning(f"Skipping malformed entry: {e}")

        return records


def main():
    """
    Reads YouTube Watch History HTML from stdin and outputs structured JSON to stdout.
    """
    html_content = sys.stdin.read().strip()
    if not html_content:
        logging.warning("No input provided. Exiting gracefully.")
        sys.exit(0)

    records = YouTubeRecord.parse(html_content)
    print(json.dumps(records, indent=4))


if __name__ == "__main__":
    main()
