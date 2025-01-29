import json
from datetime import datetime
import dateutil.parser  # More flexible timestamp parsing
from xform.base_parser import BaseParser


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

        try:
            data = json.loads(json_content)

            # Google exports typically store messages in a list inside a root object
            messages_list = (
                data.get("messages", data) if isinstance(data, dict) else data
            )

            for message in messages_list:
                try:
                    # Extract author details (name + email)
                    creator = message.get("creator", {})
                    author_name = creator.get("name", "Unknown")
                    author_email = creator.get("email", "unknown@example.com")

                    # Format author as "name <email>"
                    author = f"{author_name} <{author_email}>"

                    # Extract and format timestamp
                    raw_timestamp = message.get("created_date", "")
                    timestamp = self._format_timestamp(raw_timestamp)

                    # Extract message text
                    text = message.get("text", "").strip()

                    # Append extracted message
                    messages.append(
                        {
                            "author": author,
                            "message": text,
                            "timestamp": timestamp,
                        }
                    )

                except Exception as e:
                    print(f"[DEBUG] Error parsing message: {e}")
                    continue  # Skip malformed messages

        except json.JSONDecodeError:
            print("[ERROR] Failed to parse JSON input.")

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

            # Use dateutil.parser to handle various formats automatically
            dt_obj = dateutil.parser.parse(raw_timestamp)

            # Convert to standard ISO 8601 format with UTC timezone
            return dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")

        except (ValueError, TypeError) as e:
            print(f"[DEBUG] Failed to parse timestamp: {raw_timestamp} -> {e}")
            return "1970-01-01T00:00:00Z"  # Default for failed cases
