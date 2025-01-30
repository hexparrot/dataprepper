from bs4 import BeautifulSoup
from datetime import datetime
import html
import re
import dateutil.parser
from xform.base_parser import BaseParser


class YtParser(BaseParser):
    """
    Parser for Google Takeout YouTube watch history HTML.
    Extracts timestamps, video names as 'author', and URLs as 'message'.
    """

    def _extract_records(self, html_content: str) -> list[dict]:
        """
        Extract watched videos from the given HTML content.
        :param html_content: Raw HTML content as a string.
        :return: List of dictionaries with 'author', 'message', and 'timestamp'.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        records = []

        # Find all activity entries
        outer_cells = soup.find_all(
            "div", class_="outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp"
        )

        for cell in outer_cells:
            try:
                # Find the content cell containing the watch activity
                content_cell = cell.find(
                    "div",
                    class_="content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1",
                )
                if not content_cell:
                    continue

                # Extract the first link (video URL) and its text (video title)
                video_link = content_cell.find("a")
                if not video_link:
                    continue

                video_url = video_link.get("href", "").strip()
                video_name = video_link.get_text(strip=True)

                # If video name is just the URL, extract another <a> tag (sometimes it's separate)
                if video_name == video_url:
                    additional_links = content_cell.find_all("a")
                    if len(additional_links) > 1:
                        video_name = additional_links[1].get_text(strip=True)

                # Extract timestamp explicitly from the last text node in the content cell
                raw_text = content_cell.find_all(string=True, recursive=False)
                timestamp_text = raw_text[-1].strip() if raw_text else None

                # Fix formatting issues in timestamp (e.g., missing space before AM/PM)
                timestamp_text = re.sub(
                    r"(\d{2}:\d{2}:\d{2})(AM|PM)", r"\1 \2", timestamp_text
                )

                # Format timestamp properly
                timestamp = (
                    self._format_timestamp(timestamp_text)
                    if timestamp_text
                    else "1970-01-01T00:00:00Z"
                )

                # Append extracted data to the list
                records.append(
                    {
                        "author": video_name
                        if video_name
                        else video_url,  # Video title as author
                        "message": video_url,  # URL as message
                        "timestamp": timestamp,
                    }
                )

            except Exception as e:
                print(f"[DEBUG] Error parsing video entry: {e}")
                continue

        return records

    def _format_timestamp(self, raw_timestamp: str) -> str:
        """
        Convert Google Takeout timestamp into ISO 8601 format.
        :param raw_timestamp: Timestamp string (e.g., "Dec 19, 2014, 2:44:21PM CST").
        :return: Formatted timestamp as 'YYYY-MM-DDTHH:MM:SSZ'.
        """
        try:
            if not raw_timestamp:
                return "1970-01-01T00:00:00Z"

            # Use dateutil.parser for more robust timestamp parsing
            dt_obj = dateutil.parser.parse(raw_timestamp)

            # Convert to UTC (Z-suffix)
            return dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")

        except (ValueError, TypeError) as e:
            print(f"[DEBUG] Failed to parse timestamp: {raw_timestamp} -> {e}")
            return "1970-01-01T00:00:00Z"  # Fallback for errors
