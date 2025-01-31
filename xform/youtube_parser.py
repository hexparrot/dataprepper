#!/usr/bin/env python3
import sys
import json
import re
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from xform.base_parser import BaseParser


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
            print("[DEBUG] No content blocks found", file=sys.stderr)

        for block in content_blocks:
            try:
                # print(f"[DEBUG] Processing block: {block}", file=sys.stderr)

                # Extract video URL and title
                video_link = block.find(
                    "a", href=re.compile(r"^https://www\.youtube\.com/watch\?v=")
                )
                video_url = video_link["href"] if video_link else None
                video_title = (
                    video_link.text.strip()
                    if video_link and video_link.text.strip()
                    else "Unknown Title"
                )

                # Extract channel name
                channel_link = (
                    block.find_all("a")[1] if len(block.find_all("a")) > 1 else None
                )
                channel_name = (
                    channel_link.text.strip()
                    if channel_link and channel_link.text.strip()
                    else "Unknown Channel"
                )

                # Extract timestamp
                text_content = block.get_text(separator=" ", strip=True)

                # More robust timestamp regex
                timestamp_match = re.search(
                    r"([A-Za-z]{3} \d{1,2}, \d{4}, \d{1,2}:\d{2}:\d{2} ?(?:AM|PM)?)",
                    text_content,
                )
                timestamp = timestamp_match.group(1) if timestamp_match else None

                # Normalize timestamp into ISO 8601 format without timezone
                iso_timestamp = "1970-01-01T00:00:00"  # Default for failed parsing
                if timestamp:
                    try:
                        # Remove any trailing timezone like "CST" before parsing
                        timestamp_cleaned = re.sub(r"\s[A-Z]{2,4}$", "", timestamp)
                        dt = date_parser.parse(timestamp_cleaned)
                        iso_timestamp = dt.strftime(
                            "%Y-%m-%dT%H:%M:%S"
                        )  # Remove timezone completely
                    except ValueError:
                        pass  # print(f"[DEBUG] Failed to parse timestamp: {timestamp}", file=sys.stderr)

                # Extract product (should always be "YouTube")
                product_name = "YouTube"

                # Construct message field
                message = f"Playing {video_title} on {product_name}"

                # Ensure all required fields exist
                record = {
                    "title": video_title,
                    "url": video_url,
                    "channel": channel_name,
                    "timestamp": iso_timestamp,
                    "product": product_name,
                    "author": "unspecified",
                    "message": message,
                }

                # Debugging: Print the record before validation
                # print(f"[DEBUG] Parsed Record: {record}", file=sys.stderr)

                # Append the record
                records.append(record)

            except Exception as e:
                print(f"[ERROR] Error parsing entry: {e}", file=sys.stderr)
                continue

        # print(f"[DEBUG] Total Records Extracted: {len(records)}", file=sys.stderr)
        return records


def main():
    """Reads HTML from stdin and outputs structured JSON."""
    html_content = sys.stdin.read()
    parser = YouTubeParser()
    parsed_data = parser.parse(html_content)

    # Debugging: Print the parsed output before final JSON conversion
    print(f"[DEBUG] Final Parsed Output: {parsed_data}", file=sys.stderr)

    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
