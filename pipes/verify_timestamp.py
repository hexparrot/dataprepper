#!/usr/bin/env python3
from basepipe import BaseJSONPipe
from datetime import datetime
from collections import defaultdict


class VerifyTimestampPipe(BaseJSONPipe):
    """Pipe to verify that each record contains a complete timestamp."""

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
            datetime.fromisoformat(timestamp)
            return True
        except (ValueError, TypeError):
            return False

    def process_entry(self, entry):
        """
        Processes a single entry, verifying its timestamp.
        Logs the result and updates summary statistics.
        """
        sequence_id = entry.get("sequence_id", "NA")
        author = entry.get("author", "NA")
        timestamp = entry.get("timestamp")  # Allow None here

        # Safely convert the timestamp to a string for logging
        timestamp_str = str(timestamp) if timestamp is not None else "NA"

        # Check if the timestamp is valid
        is_valid = self.is_valid_timestamp(timestamp) if timestamp else False
        status = "VALID" if is_valid else "INVALID"

        # Update summary statistics
        if is_valid:
            self.summary[author]["valid"] += 1
        else:
            self.summary[author]["invalid"] += 1

        # Log the result
        self.log(f"{sequence_id:<12}{author:<20}{timestamp_str:<30}{status}")

        return entry if is_valid else None  # Return None for invalid entries

    def run(self):
        """Overrides the run method to verify timestamps and display summary."""
        entries = self.read_input()

        # Log header
        self.log(f"{'Sequence ID':<12}{'Author':<20}{'Timestamp':<30}{'Status'}")
        self.log("=" * 70)

        # Process entries and filter out invalid ones
        processed_entries = [
            entry for entry in (self.process_entry(e) for e in entries) if entry
        ]

        # Summarize
        self.log("=" * 70)
        self.log(f"Total records processed: {len(entries)}\n")

        # Display summary using `log_summary`
        summary_rows = [
            {"Author": author, "Valid": stats["valid"], "Invalid": stats["invalid"]}
            for author, stats in self.summary.items()
        ]
        self.log_summary(
            "Summary Statistics", ["Author", "Valid", "Invalid"], summary_rows
        )

        self.write_output(processed_entries)


if __name__ == "__main__":
    parser = VerifyTimestampPipe()
    parser.run()
