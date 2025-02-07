#!/usr/bin/env python3
import sys
import os
import json
import logging
import dateutil.parser

# Ensure Python finds the project modules no matter where the script is run
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
from xform.base_parser import BaseParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class GchatParser(BaseParser):
    """
    Parser for Google Chat JSON logs.
    Extracts timestamps, authors (name and email), and messages.
    """

    def _extract_records(self, json_content: str) -> list[dict]:
        """
        Extract chat messages from Google Chat JSON logs.
        :param json_content: Raw JSON content as a string.
        :return: List of dictionaries formatted with 'author', 'message', and 'timestamp'.
        """
        messages = []

        if not json_content or not json_content.strip():
            logging.warning("Received empty or invalid input. Skipping parsing.")
            return messages

        try:
            # logging.info(f"Raw JSON input (first 100 chars): {json_content[:100]}")
            data = json.loads(json_content)

            if not isinstance(data, (dict, list)):
                logging.error(
                    "Parsed JSON is not a valid dictionary or list. Skipping."
                )
                return messages

            messages_list = (
                data.get("messages", data) if isinstance(data, dict) else data
            )

            for message in messages_list:
                try:
                    # Extract author details (name + email)
                    creator = message.get("creator", {})
                    author_name = creator.get("name", "Unknown")
                    author_email = creator.get("email", "unknown@example.com")
                    author = f"{author_name} <{author_email}>"

                    # Extract and format timestamp
                    raw_timestamp = message.get("created_date", "")
                    timestamp = self._format_timestamp(raw_timestamp)

                    # Extract message text
                    text = message.get("text", "").strip()

                    if text:
                        messages.append(
                            {
                                "author": author,
                                "message": text,
                                "timestamp": timestamp,
                            }
                        )
                    else:
                        logging.warning("Skipping empty message.")
                except Exception as e:
                    logging.warning(f"Skipping malformed message: {e}")
                    continue

        except json.JSONDecodeError as e:
            logging.warning(f"(GchatParser) Stdin not parsable as JSON.")
            return messages

        if not messages:
            logging.warning("No valid messages found after parsing.")

        return messages

    def _format_timestamp(self, raw_timestamp: str) -> str:
        """
        Convert Google Chat timestamp into ISO 8601 format.
        :param raw_timestamp: Timestamp string from JSON (e.g., "Friday, May 12, 2005 at 10:29:28 PM UTC").
        :return: Formatted timestamp as 'YYYY-MM-DDTHH:MM:SSZ'.
        """
        try:
            if not raw_timestamp:
                return "1970-01-01T00:00:00Z"

            dt_obj = dateutil.parser.parse(raw_timestamp)
            return dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")

        except (ValueError, TypeError) as e:
            logging.warning(f"Failed to parse timestamp: {raw_timestamp} -> {e}")
            return "1970-01-01T00:00:00Z"


def main():
    """
    Reads Google Chat JSON from stdin and outputs structured JSON to stdout.
    """
    json_content = sys.stdin.read().strip()
    if not json_content:
        logging.warning("No input provided. Exiting gracefully.")
        sys.exit(0)

    parser = GchatParser()
    parsed_data = parser.parse(json_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
