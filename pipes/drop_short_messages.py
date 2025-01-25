#!/usr/bin/env python3
import sys
import json
from basepipe import BaseJSONPipe


class FilterByLengthPipe(BaseJSONPipe):
    """Pipe to filter records based on the length of the message field."""

    def __init__(self, min_length, verbose=True):
        """
        Initialize the pipe.
        :param min_length: Minimum message length to keep the record.
        :param verbose: Enable or disable logging to stderr.
        """
        super().__init__(verbose)
        self.min_length = min_length
        self.summary = {"kept": 0, "dropped": 0}

    def process_entry(self, entry):
        """
        Processes a single entry by checking the message length.
        :param entry: The JSON record.
        :return: The same record if it meets the length requirement, otherwise None.
        """
        message = entry.get("message", "")
        message_length = len(message)

        if message_length >= self.min_length:
            self.summary["kept"] += 1
            return entry
        else:
            self.summary["dropped"] += 1
            return None

    def run(self):
        """Overrides the run method to filter records based on message length."""
        entries = self.read_input()

        # Filter entries
        filtered_entries = [
            entry for entry in (self.process_entry(e) for e in entries) if entry
        ]

        # Log summary
        self.log(f"Filtered by Message Length (Min Length: {self.min_length})")
        self.log("=" * 50)
        self.log(f"Total records processed: {len(entries)}")
        self.log(f"Records kept: {self.summary['kept']}")
        self.log(f"Records dropped: {self.summary['dropped']}")
        self.log("=" * 50)

        # Write filtered entries to stdout
        self.write_output(filtered_entries)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ./filter_by_length.py <min_length>", file=sys.stderr)
        sys.exit(1)

    try:
        min_length = int(sys.argv[1])
    except ValueError:
        print("Error: <min_length> must be an integer.", file=sys.stderr)
        sys.exit(1)

    parser = FilterByLengthPipe(min_length)
    parser.run()
