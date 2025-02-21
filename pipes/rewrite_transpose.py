#!/usr/bin/env python3
import sys
import json
from basepipe import BaseJSONPipe


class AutoTransposeJSONPipe(BaseJSONPipe):
    """Pipe to automatically transpose improperly structured JSON into grouped records."""

    def __init__(self, verbose=True):
        super().__init__(verbose)
        self.transposed_data = {}

    def process_entry(self, entry):
        """
        Processes a single malformed entry, dynamically transposing fields.
        """
        if not isinstance(entry, dict) or len(entry) != 2:
            self.log(f"Identified malformed entry: {entry}")
            return

        # Extract the two keys dynamically
        keys = list(entry.keys())

        # Identify which key contains an email
        email_key = next((key for key in keys if "@" in key), None)
        field_name_key = next((key for key in keys if key != email_key), None)

        if not email_key or not field_name_key:
            self.log(f"Skipping malformed entry: {entry}")
            return

        field_name = entry[field_name_key]  # Get the actual field name from value
        value = entry[email_key]  # Get the value from the email key

        # Ensure email exists in transposed data
        if email_key not in self.transposed_data:
            self.transposed_data[email_key] = {
                "email": email_key
            }  # Initialize with email field

        # Store the correct field-value pair
        self.transposed_data[email_key][field_name] = value

    def run(self):
        """Overrides the run method to automatically transpose JSON data from stdin."""
        try:
            # Read input strictly from stdin
            raw_input = sys.stdin.read().strip()
            if not raw_input:
                self.log("ERROR: No input received from stdin.")
                sys.exit(1)

            entries = json.loads(raw_input)  # Convert input to JSON

        except json.JSONDecodeError:
            self.log("ERROR: Failed to decode JSON from stdin.")
            sys.exit(1)

        # Process each entry dynamically
        for entry in entries:
            self.process_entry(entry)

        # Convert dictionary to a list of structured records
        processed_entries = list(self.transposed_data.values())

        # Log summary
        self.log(
            f"AutoTranspose Processed {len(entries)} entries into {len(processed_entries)} records."
        )

        self.write_output(processed_entries)


if __name__ == "__main__":
    parser = AutoTransposeJSONPipe()
    parser.run()
