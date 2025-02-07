import sys
import json
import logging
import html
from bs4 import BeautifulSoup
from xform.base_parser import BaseParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class GvoiceParser(BaseParser):
    """
    Parser for Google Voice chat logs with the 'hChatLog hfeed' structure.
    Extracts timestamps, authors (by phone number), and messages.
    Reads from stdin and outputs structured JSON to stdout.
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
                    logging.warning(
                        "(GvoiceParser) Skipping message with missing timestamp."
                    )
                    continue  # Skip if no timestamp is found
                timestamp = timestamp_abbr["title"]  # ISO 8601 format

                # Extract author phone number from <a class="tel" href="tel:+1234567890">
                sender_cite = div.find("cite", class_="sender")
                if not sender_cite:
                    logging.warning("Skipping message with missing sender.")
                    continue

                phone_tag = sender_cite.find("a", class_="tel")
                if phone_tag and phone_tag.get("href"):
                    phone_number = phone_tag["href"].replace(
                        "tel:+", ""
                    )  # Remove '+' prefix
                else:
                    phone_number = "Unknown"

                # Extract message from <q> (quoted text)
                message_q = div.find("q")
                if not message_q:
                    logging.warning("Skipping message with missing content.")
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
                logging.error(f"Error processing message div: {e}")
                continue

        return messages


def main():
    """
    Reads Google Voice chat logs in HTML from stdin and outputs structured JSON to stdout.
    """
    html_content = sys.stdin.read()
    parser = GvoiceParser()
    parsed_data = parser.parse(html_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
