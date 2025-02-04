#!/usr/bin/env python3

import os
import json
import re
import logging
from datetime import datetime

# Import BaseRecord for standard formatting
from structs.base_record import BaseRecord

# Import all chat parsers
from xform.aimlogs_parser import AimLogsParser
from xform.fbchat_parser import FbchatParser
from xform.msn_parser import MsnParser
from xform.gvoice_parser import GvoiceParser
from xform.gchat_parser import GchatParser
from xform.format_a_parser import FormatAParser
from xform.format_b_parser import FormatBParser
from xform.format_c_parser import FormatCParser
from xform.format_d_parser import FormatDParser


class ChatRecord(BaseRecord):
    """
    Represents a single chat message, parsed from JSON, HTML, or other formats.
    Ensures all required fields are present:
      - `author`: Extracted from the parsed message.
      - `timestamp`: Either provided or constructed with a filename-based date.
      - `detail`: "Conversation via <ParserName> on <date>."

    Uses multiple parsers to find the best-performing one.
    """

    def __init__(self):
        super().__init__()

    def parse_chat_file(self, file_path):
        """
        Parses a chat file using the best parser.
        :param file_path: Path to the chat file (JSON, HTML)
        :return: List of processed ChatRecord instances
        """
        logging.info(f"Processing chat file: {file_path}")

        # Extract date from filename if available
        filename_date = self._extract_date_from_filename(file_path)
        logging.info(f"Extracted date from filename: {filename_date}")

        # Determine file type (JSON or HTML)
        is_json = file_path.endswith(".json")
        is_html = file_path.endswith((".html", ".htm"))

        if not (is_json or is_html):
            logging.warning(f"Skipping unsupported file: {file_path}")
            return []  # Skip non-supported file types

        file_content = self._read_file(file_path)  # Read file before parsing

        # Now passing `filename_date`
        best_parser_name, best_records = self._select_best_parser(
            file_content, is_json, filename_date
        )

        if not best_records:
            logging.warning(f"No valid chat records found for {file_path}.")
            return []

        logging.info(
            f"Using best parser: {best_parser_name} ({len(best_records)} records)"
        )

        # Convert parsed messages into structured ChatRecords
        chat_records = []
        for msg in best_records:
            record = ChatRecord()
            record.parse(
                msg, parser_name=best_parser_name, default_date=filename_date
            )  # Use extracted date
            if record.is_valid:
                chat_records.append(record._fields)

        return chat_records

    def _read_file(self, file_path):
        """
        Reads the file contents and returns as a string.
        This ensures the parsers receive actual HTML content, not a file path.
        :param file_path: Path to the chat file.
        :return: File content as a string.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {e}")
            return ""

    def _select_best_parser(self, file_content, is_json, filename_date):
        """
        Tries multiple parsers and selects the one that extracts the most records.
        :param file_content: Raw content of the chat file.
        :param is_json: Boolean indicating if the file is JSON.
        :param filename_date: Extracted date from the filename.
        :return: (Best parser name, Best records list)
        """
        # Available parsers
        json_parsers = {
            "GchatParser": GchatParser(),
        }
        html_parsers = {
            "AimLogsParser": AimLogsParser(
                date_str=filename_date
            ),  # Pass filename date
            "FbchatParser": FbchatParser(),
            "MsnParser": MsnParser(),
            "GvoiceParser": GvoiceParser(),
            # "FormatAParser": FormatAParser(),
            # "FormatBParser": FormatBParser(),
            # "FormatCParser": FormatCParser(),
            # "FormatDParser": FormatDParser(),
        }

        # Select parsers based on file type
        selected_parsers = json_parsers if is_json else html_parsers

        best_parser_name = "Unknown"
        best_records = []
        parser_performance = {}

        for parser_name, parser in selected_parsers.items():
            try:
                records = parser.parse(file_content)  # Now passing file content
                parser_performance[parser_name] = len(records)

                if len(records) > len(best_records):
                    best_parser_name = parser_name
                    best_records = records

            except Exception as e:
                logging.error(f"Error with {parser_name}: {e}")
                parser_performance[parser_name] = 0

        logging.info(f"Parser Performance: {parser_performance}")

        return best_parser_name, best_records

    def parse(self, message_data, parser_name="Unknown", default_date="1970-01-01"):
        """
        Parses an individual chat message.
        :param message_data: Dictionary containing message details.
        :param parser_name: Name of the parser that processed this chat.
        :param default_date: Default date extracted from filename.
        """

        # Extract fields from the parsed message
        author = message_data.get("author", "Unknown")
        raw_timestamp = message_data.get("timestamp", None)
        message_text = message_data.get("message", "")

        # Process timestamp (handle incomplete timestamps using filename date)
        timestamp = self._process_timestamp(raw_timestamp, default_date)

        # Populate required fields
        self.set_field("author", author)
        self.set_field("timestamp", timestamp)
        self.set_field("detail", f"Conversation via {parser_name} on {timestamp[:10]}")
        self.set_field("message", message_text)

    def _process_timestamp(self, raw_timestamp, default_date):
        """
        Converts raw timestamp into ISO8601 format.
        Handles cases where only a time (HH:MM:SS) is provided.
        :param raw_timestamp: Raw timestamp string.
        :param default_date: Default date extracted from filename.
        :return: ISO8601 timestamp (YYYY-MM-DDTHH:MM:SS)
        """
        if not raw_timestamp:
            return f"{default_date}T00:00:00"

        if re.match(r"^\d{2}:\d{2}:\d{2}$", raw_timestamp):  # Time-only case
            return f"{default_date}T{raw_timestamp}"

        # Standard timestamp formats
        known_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
        ]
        for fmt in known_formats:
            try:
                dt_obj = datetime.strptime(raw_timestamp, fmt)
                return dt_obj.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                continue

        return raw_timestamp  # Return as-is if parsing fails

    def _extract_date_from_filename(self, file_path):
        """
        Extracts a YYYY-MM-DD date from the filename, if present.
        :param file_path: Path to the chat file.
        :return: Extracted date string or "1970-01-01" if no date is found.
        """
        filename = os.path.basename(file_path)
        match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
        return match.group(1) if match else "1970-01-01"
