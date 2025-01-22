#!/usr/bin/env python3
from baseparser import BaseJSONParser
from datetime import datetime
from collections import defaultdict


class VerifyTimestampParser(BaseJSONParser):
    """Parser to verify that each record contains a complete timestamp."""

    def __init__(self, verbose=True):
        super().__init__(verbose)
        self.summary = defaultdict(lambda: {"valid": 0, "invalid": 0})

    def is_valid_timestamp(self, timestamp):
        """
        Checks if the timestamp is complete (date and time) in ISO 8601 format.
        :param timestamp: The timestamp string to validate.
        :return: True if the timestamp is valid, False otherwise.
        """
        try:
            # Attempt to parse the timestamp in ISO 8601 format
            datetime.fromisoformat(timestamp)
            return True
        except (ValueError, TypeError):
            return False

    def process_entry(self, entry):
        """
        Processes a single entry, verifying its timestamp.
        Logs the result in tabular form and updates summary statistics.
        """
        sequence_id = entry.get("sequence_id", "NA")
        author = entry.get("author", "NA")
        timestamp = entry.get("timestamp", "NA")

        # Check if the timestamp is valid
        is_valid = self.is_valid_timestamp(timestamp)
        status = "VALID" if is_valid else "INVALID"

        # Update summary statistics
        if is_valid:
            self.summary[author]["valid"] += 1
        else:
            self.summary[author]["invalid"] += 1

        # Log the result in a tabular format
        self.log(
            f"{sequence_id:<12}{author:<{self.author_width}}{timestamp:<30}{status}"
        )

        return entry

    def run(self):
        """Overrides the run method to verify timestamps for all records."""
        entries = self.read_input()

        # Determine the width of the Author column
        self.author_width = max(
            max((len(entry.get("author", "NA")) for entry in entries), default=0),
            20,  # Minimum width of 20
        )

        # Log header
        self.log(
            f"{'Sequence ID':<12}{'Author':<{self.author_width}}{'Timestamp':<30}{'Status'}"
        )
        self.log("=" * (12 + self.author_width + 30 + 7))

        # Process and output entries
        processed_entries = [self.process_entry(entry) for entry in entries]

        # Summary
        self.log("=" * (12 + self.author_width + 30 + 7))
        self.log(f"Total records processed: {len(entries)}\n")

        # Display summary statistics
        self.log("Summary Statistics:")
        self.log("=" * (12 + self.author_width + 30 + 7))
        self.log(f"{'Author':<{self.author_width}}{'Valid':<10}{'Invalid':<10}")
        self.log("-" * (self.author_width + 20))
        for author, stats in self.summary.items():
            self.log(
                f"{author:<{self.author_width}}{stats['valid']:<10}{stats['invalid']:<10}"
            )
        self.log("=" * (12 + self.author_width + 30 + 7))

        self.write_output(processed_entries)


if __name__ == "__main__":
    parser = VerifyTimestampParser()
    parser.run()
