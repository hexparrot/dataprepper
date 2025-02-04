import os
import logging
import xml.etree.ElementTree as ET
from structs.myanimelist_record import MyAnimeListRecord


class MyAnimeListParser:
    """
    Parses MyAnimeList XML exports and extracts structured data.
    """

    def parse(self, file_path):
        """
        Parses the MyAnimeList XML file.
        :param file_path: Path to the XML file.
        :return: List of structured anime records.
        """
        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            return []

        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
        except ET.ParseError as e:
            logging.error(f"Error parsing XML file {file_path}: {e}")
            return []

        # Extract the author (username)
        user_info = root.find("myinfo")
        author = (
            user_info.find("user_name").text if user_info is not None else "Unknown"
        )

        # Extract all anime entries
        anime_entries = root.findall("anime")
        records = []

        for anime in anime_entries:
            record = MyAnimeListRecord(author=author)
            record.parse(anime)
            if record.is_valid:
                records.append(record._fields)

        return records
