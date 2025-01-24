from bs4 import BeautifulSoup
from datetime import datetime
from base_parser import BaseParser


class FormatBParser(BaseParser):
    """
    Parser for chat logs in Format B.
    """

    def _extract_records(self, html_content: str) -> list[dict]:
        """
        Extract raw records from the HTML content for Format B.
        :param html_content: Sanitized HTML content as a string.
        :return: List of dictionaries with 'author', 'message', and 'timestamp'.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        raw_records = []

        # Find all divs with class 'message'
        message_divs = soup.find_all("div", class_="message")
        for div in message_divs:
            try:
                # Extract timestamp from 'timestamp' attribute
                timestamp_attr = div.get("timestamp")
                if not timestamp_attr:
                    continue
                timestamp = self._format_timestamp(timestamp_attr)

                # Extract author
                buddy_span = div.find("span", class_="buddy")
                if not buddy_span:
                    continue
                author = buddy_span.get_text(strip=True)

                # Extract message
                msgcontent_span = div.find("span", class_="msgcontent")
                if not msgcontent_span:
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
            except Exception:
                # Skip any records that fail extraction
                continue

        return raw_records

    def _format_timestamp(self, raw_timestamp: str) -> str:
        """
        Format the raw timestamp into ISO 8601.
        :param raw_timestamp: Raw timestamp string.
        :return: Formatted ISO 8601 timestamp or the original string if invalid.
        """
        try:
            timestamp_obj = datetime.strptime(raw_timestamp, "%Y-%m-%d %H:%M:%S")
            return timestamp_obj.isoformat()
        except ValueError:
            return raw_timestamp

    def _extract_message(self, msgcontent_span):
        """
        Extract the message content from the 'msgcontent' span.
        :param msgcontent_span: The span element containing the message content.
        :return: Extracted message as a string or None if empty.
        """
        pre_tag = msgcontent_span.find("pre")
        if pre_tag:
            return pre_tag.get_text(strip=True)
        return msgcontent_span.get_text(strip=True)
