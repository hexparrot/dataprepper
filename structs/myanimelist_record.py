import xml.etree.ElementTree as ET
import logging
from structs.base_record import BaseRecord


class MyAnimeListRecord(BaseRecord):
    """
    Represents an entire MyAnimeList export.
    Parses and returns structured records from an XML file.
    """

    def __init__(self, author):
        """
        Initializes an instance for MyAnimeList parsing.
        :param author: MyAnimeList username.
        """
        super().__init__()
        self.set_field("author", author)

    @classmethod
    def parse_file(cls, file_path):
        """
        Parses a MyAnimeList XML file and returns a list of records.
        :param file_path: Path to the XML file.
        :return: List of parsed anime records.
        """
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()

            # Extract username from <myinfo> block
            user_name_element = root.find(".//user_name")
            author = (
                user_name_element.text.strip()
                if user_name_element is not None
                else "Unknown"
            )

            anime_records = []
            for anime_element in root.findall(".//anime"):
                record = cls.parse_anime_entry(anime_element, author)
                if record:
                    anime_records.append(record)

            return anime_records
        except Exception as e:
            logging.error(f"Error parsing MyAnimeList XML {file_path}: {e}")
            return []

    @staticmethod
    def parse_anime_entry(anime_element, author):
        """
        Parses an individual <anime> entry from the XML.
        :param anime_element: XML element containing anime details.
        :param author: MyAnimeList username.
        :return: Dictionary of parsed anime data.
        """

        def get_text(element, default=""):
            """Helper function to safely extract text from XML elements."""
            return (
                element.text.strip()
                if element is not None and element.text
                else default
            )

        # Extract anime details
        series_title = get_text(anime_element.find("series_title"))
        series_episodes = get_text(anime_element.find("series_episodes"), "Unknown")
        my_status = get_text(anime_element.find("my_status"), "Unknown")

        # Determine the best timestamp
        my_finish_date = get_text(anime_element.find("my_finish_date"))
        my_start_date = get_text(anime_element.find("my_start_date"))

        timestamp = (
            my_finish_date
            if my_finish_date != "0000-00-00"
            else (my_start_date if my_start_date != "0000-00-00" else "1970-01-01")
        )

        # Build record
        record = {
            "author": author,
            "timestamp": timestamp,
            "detail": f"{my_status} {series_title} {series_episodes} via MyAnimeListParser on {timestamp}",
        }
        return record
