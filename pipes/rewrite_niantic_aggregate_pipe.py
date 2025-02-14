#!/usr/bin/env python3
import sys
import json
from datetime import datetime
from basepipe import BaseJSONPipe


class GeneralizedDataPipe(BaseJSONPipe):
    """Processes Niantic data where multiple fields are aggregated into a single string."""

    def normalize_timestamp(self, timestamp_str):
        """Converts various timestamp formats to ISO 8601 format if possible."""
        try:
            return datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S UTC").isoformat()
        except ValueError:
            try:
                return datetime.fromisoformat(
                    timestamp_str.replace("Z", "+00:00")
                ).isoformat()
            except ValueError:
                return timestamp_str  # Return original if parsing fails

    def convert_to_number(self, value):
        """Converts a value to an integer or float if possible."""
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value  # Return original if parsing fails

    def process_entry(self, entry):
        """Parses and restructures the generic data entry."""
        try:
            raw_key = list(entry.keys())[
                0
            ]  # Extract the key holding the aggregated data
            raw_data = entry[raw_key]
            fields = raw_data.split("\t")  # Split by tab character
            headers = raw_key.split("\t")  # Extract headers from key

            structured_entry = {}
            for index, header in enumerate(headers):
                value = fields[index] if index < len(fields) else ""
                structured_entry[header] = (
                    self.normalize_timestamp(value)
                    if "date" in header.lower() or "time" in header.lower()
                    else self.convert_to_number(value)
                )

            return structured_entry
        except (IndexError, ValueError) as e:
            self.log(f"Error processing entry: {e}")
            return None

    def run(self):
        """Reads, processes, and outputs JSON records using stdin/stdout."""
        entries = self.read_input()
        processed_entries = [self.process_entry(entry) for entry in entries if entry]
        self.write_output(processed_entries)


if __name__ == "__main__":
    GeneralizedDataPipe().run()
