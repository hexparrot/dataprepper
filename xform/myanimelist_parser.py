import sys
import json
import logging
import xml.etree.ElementTree as ET
from structs.myanimelist_record import MyAnimeListRecord

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class MyAnimeListParser:
    """
    Parses MyAnimeList XML exports and extracts structured data.
    Reads from stdin and outputs JSON to stdout.
    """

    def parse(self, xml_content: str) -> list[dict]:
        """
        Parses the MyAnimeList XML content.
        :param xml_content: Raw XML content as a string.
        :return: List of structured anime records.
        """
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            logging.error(f"Error parsing XML input: {e}")
            return []

        # Extract the author (username)
        user_info = root.find("myinfo")
        author = (
            user_info.find("user_name").text if user_info is not None else "Unknown"
        )

        # Extract all anime entries
        anime_entries = root.findall("anime")
        records = []

        if not anime_entries:
            logging.warning("No anime entries found in the XML.")

        for anime in anime_entries:
            record = MyAnimeListRecord(author=author)
            record.parse(anime)
            if record.is_valid:
                records.append(record._fields)

        return records


def main():
    """
    Reads MyAnimeList XML from stdin and outputs structured JSON to stdout.
    """
    xml_content = sys.stdin.read()
    parser = MyAnimeListParser()
    parsed_data = parser.parse(xml_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
