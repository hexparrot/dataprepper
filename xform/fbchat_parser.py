import sys
import json
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from xform.base_parser import BaseParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class FbchatParser(BaseParser):
    """
    Parser for Facebook Messenger chat logs in HTML format.
    Reads from stdin and outputs structured JSON to stdout.
    """

    def _extract_records(self, html_content: str) -> list[dict]:
        """
        Extract raw records from the HTML content.
        :param html_content: Raw HTML content as a string.
        :return: List of dictionaries with 'author', 'message', and 'timestamp'.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        raw_records = []

        message_divs = soup.find_all("div", class_="message")
        for div in message_divs:
            try:
                # Extract timestamp
                timestamp_attr = div.get("timestamp")
                if not timestamp_attr:
                    logging.warning(
                        "(FbchatParser) Skipping message without timestamp."
                    )
                    continue
                timestamp = self._format_timestamp(timestamp_attr)

                # Extract author
                buddy_span = div.find("span", class_="buddy")
                if not buddy_span:
                    logging.warning("Skipping message without author.")
                    continue
                author = buddy_span.get_text(strip=True)

                # Extract message
                msgcontent_span = div.find("span", class_="msgcontent")
                if not msgcontent_span:
                    logging.warning("Skipping message without content.")
                    continue
                message = self._extract_message(msgcontent_span)

                if message:
                    raw_records.append(
                        {
                            "author": author,
                            "message": message,
                            "timestamp": timestamp,
                        }
                    )
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                continue

        return raw_records

    def _format_timestamp(self, raw_timestamp: str) -> str:
        """
        Convert raw timestamp into ISO 8601 format.
        """
        try:
            timestamp_obj = datetime.strptime(raw_timestamp, "%Y-%m-%d %H:%M:%S")
            return timestamp_obj.isoformat()
        except ValueError:
            logging.warning(f"Invalid timestamp format: {raw_timestamp}")
            return raw_timestamp

    def _extract_message(self, msgcontent_span):
        """
        Extract message content from the 'msgcontent' span.
        """
        pre_tag = msgcontent_span.find("pre")
        return (
            pre_tag.get_text(strip=True)
            if pre_tag
            else msgcontent_span.get_text(strip=True)
        )


def main():
    """
    Reads Facebook Messenger chat HTML from stdin and outputs JSON to stdout.
    """
    html_content = sys.stdin.read()
    parser = FbchatParser()
    parsed_data = parser.parse(html_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
