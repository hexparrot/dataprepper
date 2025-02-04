import os
import logging
import json
from bs4 import BeautifulSoup
from structs.base_record import BaseRecord


class YouTubeRecord(BaseRecord):
    """
    Represents an entry from a YouTube watch history.
    """

    def __init__(self):
        """
        Initializes the YouTubeRecord.
        """
        super().__init__()

    @classmethod
    def parse_file(cls, file_path):
        """
        Parses a YouTube watch history HTML file and returns structured records.
        :param file_path: Path to the HTML file.
        :return: List of structured YouTube watch records.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                soup = BeautifulSoup(f, "html.parser")

            youtube_records = []
            for entry in cls._extract_entries(soup):
                youtube_records.append(entry)

            return youtube_records
        except Exception as e:
            logging.error(f"Error parsing YouTube watch history HTML {file_path}: {e}")
            return []

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
                title = title_tag.text.strip() if title_tag else "unknown"
                url = (
                    title_tag["href"]
                    if title_tag and "href" in title_tag.attrs
                    else "unknown"
                )

                timestamp_tag = entry.find("div", class_="metadata-cell")
                timestamp = (
                    timestamp_tag.text.strip()
                    if timestamp_tag
                    else "1970-01-01T00:00:00"
                )

                channel_tag = entry.find("a", class_="yt-simple-endpoint")
                channel = channel_tag.text.strip() if channel_tag else "unknown"

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
