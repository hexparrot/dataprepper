#!/usr/bin/env python3
from baseparser import BaseJSONParser


class AddSequenceIDParser(BaseJSONParser):
    """Parser to append a sequence_id to each JSON record."""

    def __init__(self, verbose=True):
        super().__init__(verbose)

    def process_entry(self, entry, index):
        """
        Processes a single entry by appending a sequence_id.
        :param entry: The JSON record.
        :param index: The sequence ID to append.
        :return: The modified JSON record.
        """
        entry["sequence_id"] = index
        return entry

    def run(self):
        """Overrides the run method to append sequence IDs to all records."""
        entries = self.read_input()

        # Add sequence_id to each record
        processed_entries = [
            self.process_entry(entry, idx) for idx, entry in enumerate(entries)
        ]

        # Log summary
        summary_rows = [
            {"Metric": "Total Records Processed", "Value": len(processed_entries)},
            {
                "Metric": "Sequence ID Range",
                "Value": (
                    f"0 to {len(processed_entries) - 1}" if processed_entries else "N/A"
                ),
            },
        ]
        self.log_summary(
            "Sequence ID Assignment Summary", ["Metric", "Value"], summary_rows
        )

        # Write the output
        self.write_output(processed_entries)


if __name__ == "__main__":
    parser = AddSequenceIDParser()
    parser.run()
