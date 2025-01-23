#!/usr/bin/env python3
import random
import string
from baseparser import BaseJSONParser


class AugmentConvoIDParser(BaseJSONParser):
    """Parser to add a convo_id field to each JSON record."""

    def __init__(self, verbose=True):
        super().__init__(verbose)
        self.random_prefix = self.generate_random_string()

    def generate_random_string(self, length=8):
        """
        Generates a random alphanumeric string of the specified length.
        :param length: Length of the string to generate (default is 8).
        :return: A random string.
        """
        return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))

    def process_entry(self, entry, sequence_id):
        """
        Processes a single entry by adding a convo_id field.
        :param entry: The JSON record.
        :param sequence_id: The sequence ID used to generate the convo_id.
        :return: The modified JSON record.
        """
        padded_sequence = f"{sequence_id:05d}"  # Zero-padded to 5 digits
        entry["convo_id"] = f"{self.random_prefix}-{padded_sequence}"
        return entry

    def run(self):
        """Overrides the run method to add convo_id fields to all records."""
        entries = self.read_input()

        # Add convo_id to each record
        processed_entries = [
            self.process_entry(entry, idx) for idx, entry in enumerate(entries)
        ]

        # Log summary
        summary_rows = [
            {"Metric": "Total Records Processed", "Value": len(processed_entries)},
            {"Metric": "Random Prefix Used", "Value": self.random_prefix},
        ]
        self.log_summary(
            "Convo ID Assignment Summary", ["Metric", "Value"], summary_rows
        )

        # Write the output
        self.write_output(processed_entries)


if __name__ == "__main__":
    parser = AugmentConvoIDParser()
    parser.run()
