import sys
import json
from abc import ABC, abstractmethod


class BaseJSONParser(ABC):
    """Base class for JSON parsing."""

    def __init__(self, verbose=True):
        """
        Initialize the parser.
        :param verbose: Enable or disable logging to stderr.
        """
        self.verbose = verbose

    def log(self, message):
        """
        Logs a message to stderr if verbose logging is enabled.
        :param message: The message to log.
        """
        if self.verbose:
            sys.stderr.write(f"{message}\n")

    def log_summary(self, title, headers, rows):
        """
        Logs tabular summary statistics dynamically adjusted to the data.
        :param title: Title for the summary table.
        :param headers: List of column headers.
        :param rows: List of dictionaries representing rows of data.
        """
        if not rows:
            self.log("No data to display in summary.")
            return

        # Ensure all rows are dictionaries and convert values to strings
        sanitized_rows = [
            {header: str(row.get(header, "")) for header in headers} for row in rows
        ]

        # Determine column widths dynamically
        column_widths = [
            max(len(header), *(len(row[header]) for row in sanitized_rows))
            for header in headers
        ]

        # Print the title, if provided
        if title:
            self.log(title)

        # Calculate the total width for separators
        total_width = sum(column_widths) + len(column_widths) - 1

        # Print header row
        self.log("=" * total_width)
        self.log(
            "  ".join(
                f"{header:<{width}}" for header, width in zip(headers, column_widths)
            )
        )
        self.log("-" * total_width)

        # Print data rows
        for row in sanitized_rows:
            self.log(
                "  ".join(
                    f"{row[header]:<{width}}"
                    for header, width in zip(headers, column_widths)
                )
            )

        # Print footer
        self.log("=" * total_width)

    def read_input(self):
        """Reads JSON data from stdin."""
        try:
            input_data = sys.stdin.read()
            json_entries = json.loads(input_data)
            if not isinstance(json_entries, list):
                raise ValueError("Input JSON must be a list of entries.")
            return json_entries
        except Exception as e:
            self.log(f"Error reading input JSON: {e}")
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
            self.log(f"Error writing output JSON: {e}")
            sys.exit(1)

    def run(self):
        """Main method to execute the parsing pipeline."""
        entries = self.read_input()
        processed_entries = [self.process_entry(entry) for entry in entries]
        self.write_output(processed_entries)
