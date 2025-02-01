import sys
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from xform.base_parser import BaseParser


class MyAnimeListParser(BaseParser):
    """
    Parser for MyAnimeList XML exports.
    Extracts anime details including title, status, score, priority, episodes, and watch history.
    """

    def _extract_records(self, xml_content: str) -> list[dict]:
        """
        Extract anime records from MyAnimeList XML.
        :param xml_content: Raw XML content as a string.
        :return: List of dictionaries with structured data.
        """
        records = []
        root = ET.ElementTree(ET.fromstring(xml_content)).getroot()
        user_name = (
            root.find("myinfo/user_name").text.strip()
            if root.find("myinfo/user_name") is not None
            else "MAL User"
        )

        for anime in root.findall("anime"):
            try:

                def get_text(element, default=""):
                    return (
                        element.text.strip()
                        if element is not None and element.text
                        else default
                    )

                start_date = anime.find("my_start_date").text.strip()
                finish_date = anime.find("my_finish_date").text.strip()
                timestamp_source = (
                    finish_date
                    if finish_date and finish_date != "0000-00-00"
                    else start_date
                    if start_date and start_date != "0000-00-00"
                    else "1970-01-01"
                )
                timestamp = self._format_timestamp(timestamp_source)

                record = {
                    "anime_id": get_text(anime.find("series_animedb_id")),
                    "title": get_text(anime.find("series_title")),
                    "type": get_text(anime.find("series_type")),
                    "episodes": get_text(anime.find("series_episodes")),
                    "watched_episodes": get_text(anime.find("my_watched_episodes")),
                    "start_date": start_date,
                    "finish_date": finish_date,
                    "score": get_text(anime.find("my_score")),
                    "status": get_text(anime.find("my_status")),
                    "priority": get_text(anime.find("my_priority")),
                    "times_watched": get_text(anime.find("my_times_watched")),
                    "rewatching": get_text(anime.find("my_rewatching")),
                    "rewatching_ep": get_text(anime.find("my_rewatching_ep")),
                    "discuss": get_text(anime.find("my_discuss")),
                    "author": user_name,
                    "timestamp": timestamp,
                    "message": f"Tracked anime: {get_text(anime.find('series_title'))} on MyAnimeList",
                    "product": "MyAnimeList",
                }

                records.append(record)

            except Exception as e:
                print(f"[ERROR] Error parsing entry: {e}", file=sys.stderr)
                continue

        return records

    @staticmethod
    def _format_timestamp(date_str: str) -> str:
        """
        Generate timestamp using a single date string, appending 00:00:00 time.
        """
        return f"{date_str}T00:00:00"


def main():
    """Reads MyAnimeList XML from stdin and outputs structured JSON."""
    xml_content = sys.stdin.read()
    parser = MyAnimeListParser()
    parsed_data = parser.parse(xml_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
