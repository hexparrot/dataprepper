#!/usr/bin/env python3
from basepipe import BaseJSONPipe


class RewriteNewlinesPipe(BaseJSONPipe):
    """Pipe to normalize newlines and backslash characters to spaces."""

    def __init__(self, verbose=True):
        """
        Initialize the pipe.
        :param verbose: Enable or disable logging to stderr.
        """
        super().__init__(verbose)

    def process_entry(self, entry):
        """
        Processes a single entry by replacing newlines and backslash characters with spaces.
        :param entry: The JSON record.
        :return: The modified JSON record.
        """
        for key, value in entry.items():
            if isinstance(value, str):  # Only process string fields
                entry[key] = value.replace("\n", " ").replace("\\", " ")
        return entry

    def run(self):
        """Overrides the run method to normalize newlines for all records."""
        entries = self.read_input()

        # Normalize newlines for each record
        processed_entries = [self.process_entry(entry) for entry in entries]

        # Log summary
        summary_rows = [
            {"Metric": "Total Records Processed", "Value": len(processed_entries)},
        ]
        self.log_summary(
            "Newline Normalization Summary", ["Metric", "Value"], summary_rows
        )

        # Write the output
        self.write_output(processed_entries)


if __name__ == "__main__":
    import sys

    parser = RewriteNewlinesPipe()
    parser.run()
