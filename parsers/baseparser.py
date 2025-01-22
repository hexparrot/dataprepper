import sys
import json
from abc import ABC, abstractmethod


class BaseJSONParser(ABC):
    """Base class for JSON parsing."""

    def __init__(self):
        pass

    def read_input(self):
        """Reads JSON data from stdin."""
        try:
            input_data = sys.stdin.read()
            json_entries = json.loads(input_data)
            if not isinstance(json_entries, list):
                raise ValueError("Input JSON must be a list of entries.")
            return json_entries
        except Exception as e:
            sys.stderr.write(f"Error reading input JSON: {e}\n")
            sys.exit(1)

    @abstractmethod
    def process_entry(self, entry):
        """
        Abstract method to process a single JSON entry.
        Subclasses must override this method.
        """
        pass

    def write_output(self, processed_entries):
        """Writes JSON data to stdout."""
        try:
            sys.stdout.write(json.dumps(processed_entries, indent=2))
        except Exception as e:
            sys.stderr.write(f"Error writing output JSON: {e}\n")
            sys.exit(1)

    def run(self):
        """Main method to execute the parsing pipeline."""
        entries = self.read_input()
        processed_entries = [self.process_entry(entry) for entry in entries]
        self.write_output(processed_entries)
