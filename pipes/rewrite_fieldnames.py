#!/usr/bin/env python3
import sys
import json
from basepipe import BaseJSONPipe


class FieldRenameJSONPipe(BaseJSONPipe):
    """Pipe to rename JSON entry fields based on a provided list of field names, dropping extra fields."""

    def __init__(self, field_names, verbose=True):
        """
        Initializes the pipe with a list of new field names.

        :param field_names: List of new field names to apply to each entry.
        :param verbose: Enable verbose logging.
        """
        super().__init__(verbose)
        self.field_names = field_names
        self.num_fields = len(field_names)  # Number of fields to take

    def process_entry(self, entry):
        """
        Processes a single entry, taking the first X fields and renaming them.
        Extra fields are discarded.
        """
        if not isinstance(entry, dict):
            self.log(f"Skipping non-dictionary entry: {entry}")
            return None

        # Remove any empty (unnamed) fields
        filtered_entry = {k: v for k, v in entry.items() if k.strip()}

        # Take the first X fields based on the provided field names
        selected_keys = list(filtered_entry.keys())[: self.num_fields]
        selected_values = [filtered_entry[key] for key in selected_keys]

        if len(selected_values) != self.num_fields:
            self.log(f"Skipping entry due to insufficient fields: {filtered_entry}")
            return None

        # Create a new dictionary with renamed fields
        return dict(zip(self.field_names, selected_values))

    def run(self):
        """Overrides the run method to process JSON data from stdin with renamed fields."""
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

        # Process each entry and filter out any None results
        processed_entries = [
            self.process_entry(entry) for entry in entries if self.process_entry(entry)
        ]

        # Log summary
        self.log(
            f"Field Rename Processed {len(entries)} entries into {len(processed_entries)} records."
        )

        self.write_output(processed_entries)


if __name__ == "__main__":
    # Example usage: Pass the field names as command-line arguments
    if len(sys.argv) < 2:
        print("Usage: ./field_rename_pipe.py field1 field2 field3 ...")
        sys.exit(1)

    field_names = sys.argv[1:]  # Extract field names from arguments
    parser = FieldRenameJSONPipe(field_names)
    parser.run()
