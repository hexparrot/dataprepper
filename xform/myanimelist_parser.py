#!/usr/bin/env python3
import sys
import os
import json
import logging
import xml.etree.ElementTree as ET

# Ensure Python finds the project modules no matter where the script is run
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))

from structs.myanimelist_record import MyAnimeListRecord
from xform.base_parser import BaseParser  # Now inherits from BaseParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class MyAnimeListParser(BaseParser):
    """
    Parses MyAnimeList XML exports and extracts structured data.
    Reads from stdin and outputs JSON to stdout.
    """

    def _extract_records(self, xml_content: str) -> list:
        """
        Extracts structured records from MyAnimeList XML.
        :param xml_content: Raw XML content as a string.
        :return: List of structured anime records as dictionaries.
        """
        try:
            root = (
                ET.fromstring(xml_content)
                if isinstance(xml_content, str)
                else xml_content
            )
        except ET.ParseError as e:
            logging.error(f"Error parsing XML input: {e}")
            return []

        # Extract the author (username)
        user_info = root.find("myinfo")
        author = (
            user_info.findtext("user_name", "Unknown").strip()
            if user_info is not None
            else "Unknown"
        )

        # Extract all anime entries
        anime_entries = root.findall("anime")
        records = []

        if not anime_entries:
            logging.warning("No anime entries found in the XML.")

        for anime in anime_entries:
            parsed_entry = MyAnimeListRecord.parse_anime_entry(anime, author)

            if parsed_entry:
                records.append(parsed_entry)

        return records


def main():
    """
    Reads MyAnimeList XML from stdin and outputs structured JSON to stdout.
    """
    xml_content = sys.stdin.read().strip()  # Remove unnecessary whitespace
    if not xml_content:
        logging.error("No XML content provided. Exiting.")
        sys.exit(1)

    parser = MyAnimeListParser()
    parsed_data = parser.parse(xml_content)  # Calls BaseParser's `parse()`
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
