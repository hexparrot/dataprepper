import logging
import xml.etree.ElementTree as ET
from structs.base_record import BaseRecord


class MyAnimeListRecord(BaseRecord):
    """
    Represents a single anime record from a MyAnimeList XML export.
    Parses anime data and transforms it into structured JSON.
    """

    def __init__(self, author: str):
        """
        Initialize an anime record with a given author (username).
        :param author: The username associated with this anime list.
        """
        super().__init__()  # No required fields enforced
        self.set_field("author", author)

    def parse(self, anime_element: ET.Element):
        """
        Parses a single <anime> XML element and extracts relevant fields.
        :param anime_element: XML Element representing an anime.
        """

        def get_text(element, default="Unknown"):
            """Helper function to safely extract text from XML elements."""
            return (
                element.text.strip()
                if element is not None and element.text
                else default
            )

        self.set_field(
            "title", get_text(anime_element.find("series_title"), "Unknown Title")
        )
        self.set_field("score", get_text(anime_element.find("my_score"), "0"))
        self.set_field("status", get_text(anime_element.find("my_status"), "unknown"))
        self.set_field(
            "episodes_watched", get_text(anime_element.find("my_watched_episodes"), "0")
        )

        # Extract `timestamp` from `my_finish_date`, fallback to `my_start_date`
        finish_date = get_text(anime_element.find("my_finish_date"), "")
        start_date = get_text(anime_element.find("my_start_date"), "")

        # Ensure a valid timestamp in full ISO 8601 format
        if finish_date and finish_date != "0000-00-00":
            self.set_field("timestamp", f"{finish_date}T00:00:00")
        elif start_date and start_date != "0000-00-00":
            self.set_field("timestamp", f"{start_date}T00:00:00")
        else:
            self.set_field(
                "timestamp", "1970-01-01T00:00:00"
            )  # Default to valid timestamp

    @classmethod
    def parse_anime_entry(cls, anime_element: ET.Element, author: str):
        """
        Parses an anime entry and returns a structured record.
        :param anime_element: XML Element representing an anime.
        :param author: The username associated with this anime list.
        :return: A dictionary of the anime record.
        """
        record = cls(author)
        record.parse(anime_element)
        return record._fields  # Always return parsed data

    @classmethod
    def parse_from_xml(cls, xml_content: str):
        """
        Parses MyAnimeList XML content and returns a list of records.
        :param xml_content: Raw XML as a string.
        :return: List of parsed anime records as dictionaries.
        """
        try:
            root = (
                ET.fromstring(xml_content)
                if isinstance(xml_content, str)
                else xml_content
            )
        except ET.ParseError as e:
            logging.error(f"Error parsing MyAnimeList XML: {e}")
            return []

        # Extract username
        user_info = root.find("myinfo")
        author = (
            user_info.findtext("user_name", "Unknown").strip()
            if user_info is not None
            else "Unknown"
        )

        # Extract all anime entries
        anime_entries = root.findall("anime")
        if not anime_entries:
            logging.warning("No anime entries found in XML.")

        # Parse each anime entry
        return [
            cls.parse_anime_entry(anime, author)
            for anime in anime_entries
            if anime is not None
        ]
