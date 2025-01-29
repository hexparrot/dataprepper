from bs4 import BeautifulSoup, NavigableString, Tag
from datetime import datetime
import html
import re
from xform.base_parser import BaseParser


class GchatParser(BaseParser):
    """
    Parser for chat logs with the 'hChatLog hfeed' structure.
    Extracts timestamps, authors (by phone number), and messages.
    """

    def _extract_records(self, html_content: str) -> list[dict]:
        """
        Extract chat messages from the given HTML content.
        :param html_content: Raw HTML content as a string.
        :return: List of dictionaries with 'author', 'message', and 'timestamp'.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        messages = []

        # Find all <div class="message"> elements
        message_divs = soup.find_all("div", class_="message")

        for div in message_divs:
            try:
                # Extract timestamp from <abbr class="dt" title="ISO8601 Timestamp">
                timestamp_abbr = div.find("abbr", class_="dt")
                if not timestamp_abbr or not timestamp_abbr.get("title"):
                    continue  # Skip if no timestamp is found

                timestamp = timestamp_abbr["title"]  # ISO 8601 format

                # Extract author phone number from <a class="tel" href="tel:+1234567890">
                sender_cite = div.find("cite", class_="sender")
                if not sender_cite:
                    continue

                phone_tag = sender_cite.find("a", class_="tel")
                if phone_tag and phone_tag.get("href"):
                    phone_number = phone_tag["href"].replace(
                        "tel:+", ""
                    )  # Remove '+' prefix
                else:
                    phone_number = (
                        "Unknown"  # Assign "Unknown" if phone number is missing
                    )

                # Extract message from <q> (quoted text)
                message_q = div.find("q")
                if not message_q:
                    continue

                message = html.unescape(message_q.get_text(strip=True))

                # Append extracted data to list
                messages.append(
                    {
                        "author": phone_number,
                        "message": message,
                        "timestamp": timestamp,
                    }
                )

            except Exception as e:
                # print(f"[DEBUG] Error processing message div: {e}")
                continue

        return messages
