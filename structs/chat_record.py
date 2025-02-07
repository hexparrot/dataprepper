import os
import json
import logging
import sys
from structs.base_record import BaseRecord
from xform.aimlogs_parser import AimLogsParser
from xform.aimlogs2_parser import AimLogs2Parser
from xform.fbchat_parser import FbchatParser
from xform.msn_parser import MsnParser
from xform.gvoice_parser import GvoiceParser
from xform.gchat_parser import GchatParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class ChatRecord(BaseRecord):
    """
    Represents a chat message parsed from various formats.
    Supports structured JSON output.
    """

    def __init__(self):
        super().__init__()

    def parse(self, message_data):
        """
        Parses an individual chat message from JSON.
        """
        author = message_data.get("author", "Unknown")
        timestamp = message_data.get("timestamp", "1970-01-01T00:00:00")
        message = message_data.get("message", "")

        self.set_field("author", author)
        self.set_field("timestamp", timestamp)
        self.set_field("message", message)

    @classmethod
    def process_chat_directory(cls, input_directory, output_directory):
        """
        Traverses a directory, processing all chat files and saving the JSON output.
        """
        logging.info(f"Processing chat files in directory: {input_directory}")

        if not os.path.isdir(input_directory):
            logging.error(f"Provided path is not a directory: {input_directory}")
            return

        os.makedirs(output_directory, exist_ok=True)

        for root, _, files in os.walk(input_directory):
            for file in files:
                file_path = os.path.join(root, file)
                if file.endswith(".html") or file.endswith(".json"):
                    records = cls.parse_chat_file(file_path)
                    output_file = os.path.join(
                        output_directory, f"{os.path.splitext(file)[0]}.json"
                    )
                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(records, f, indent=4)
                    logging.info(f"Saved processed chat records to {output_file}")

    @classmethod
    def parse_chat_file(cls, file_path):
        """
        Parses a chat file using the best available parser.
        """
        logging.info(f"Processing chat file: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            file_content = f.read()

        parsers = {
            "AimLogsParser": AimLogsParser(),
            "AimLogs2Parser": AimLogs2Parser(date_str="1970-01-01"),
            "FbchatParser": FbchatParser(),
            "MsnParser": MsnParser(),
            "GvoiceParser": GvoiceParser(),
            "GchatParser": GchatParser(),
        }

        best_parser_name = "Unknown"
        best_records = []

        for parser_name, parser in parsers.items():
            try:
                records = parser.parse(file_content)
                if len(records) > len(best_records):
                    best_parser_name = parser_name
                    best_records = records
            except Exception as e:
                logging.error(f"Error with {parser_name}: {e}")

        if not best_records:
            logging.warning(f"No valid chat records found in {file_path}.")
            return []

        logging.info(
            f"Using best parser: {best_parser_name} ({len(best_records)} records)"
        )

        chat_records = []
        for msg in best_records:
            chat_record = cls()
            chat_record.parse(msg)
            chat_records.append(chat_record._fields)

        return chat_records


if __name__ == "__main__":
    if len(sys.argv) > 2:
        ChatRecord.process_chat_directory(sys.argv[1], sys.argv[2])
    else:
        print("Usage: python chat_record.py <input_directory> <output_directory>")
