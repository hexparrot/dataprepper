#!/usr/bin/env python3
import sys
import json
from baseparser import BaseJSONPipe


class CheckNonEmptyValuesParser(BaseJSONPipe):
    """Parser to validate records for required non-empty fields."""

    def __init__(self, required_fields, verbose=True):
        """
        Initialize the parser with required fields.
        :param required_fields: List of fields to check for non-empty values.
        :param verbose: Enable or disable verbose logging.
        """
        super().__init__(verbose)
        self.required_fields = required_fields

    def is_valid_entry(self, entry):
        """
        Validates a record to ensure all required fields are non-empty.
        :param entry: The JSON record.
        :return: True if all required fields are non-empty, False otherwise.
        """
        for field in self.required_fields:
            if field not in entry or not str(entry[field]).strip():
                return False
        return True

    def process_entry(self, entry):
        """
        Dummy implementation of the process_entry method.
        This parser doesn't modify individual entries directly.
        :param entry: The JSON record.
        :return: The same entry unmodified.
        """
        return entry

    def process_entries(self, entries):
        """
        Processes all entries and filters out invalid ones.
        :param entries: List of JSON records.
        :return: Tuple of (valid_entries, invalid_entries).
        """
        valid_entries = []
        invalid_entries = []

        for entry in entries:
            if self.is_valid_entry(entry):
                valid_entries.append(entry)
            else:
                invalid_entries.append(entry)

        return valid_entries, invalid_entries

    def run(self):
        """Main method to validate records and generate summary."""
        entries = self.read_input()

        # Process entries
        valid_entries, invalid_entries = self.process_entries(entries)

        # Log invalid entries to stderr
        for invalid_entry in invalid_entries:
            self.log(
                f"Dropped record due to missing or empty fields: {json.dumps(invalid_entry, indent=2)}"
            )

        # Log summary
        total_records = len(entries)
        valid_count = len(valid_entries)
        invalid_count = len(invalid_entries)
        valid_proportion = (
            (valid_count / total_records) * 100 if total_records > 0 else 0
        )

        summary_rows = [
            {"Metric": "Total Records Processed", "Value": total_records},
            {"Metric": "Valid Records", "Value": valid_count},
            {"Metric": "Invalid Records", "Value": invalid_count},
            {"Metric": "Proportion Valid (%)", "Value": f"{valid_proportion:.2f}%"},
        ]
        self.log_summary(
            "Non-Empty Fields Validation Summary", ["Metric", "Value"], summary_rows
        )

        # Write only valid entries to stdout
        self.write_output(valid_entries)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: ./check_nonempty_values.py <field1,field2,...>", file=sys.stderr)
        sys.exit(1)

    # Parse required fields from command line arguments
    required_fields = sys.argv[1].split(",")
    parser = CheckNonEmptyValuesParser(required_fields)
    parser.run()
