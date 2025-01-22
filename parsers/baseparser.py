import sys
import json
from abc import ABC, abstractmethod
from datetime import datetime


class BaseJSONParser(ABC):
    """Base class for JSON parsing."""

    def __init__(self, verbose=True):
        """
        Initialize the parser.
        :param verbose: Enable or disable logging to stderr.
        """
        self.verbose = verbose
        self.details_log_path = "pipe_summaries"

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
        Also appends the summary with a timestamp to a file.
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

        # Get the class name of the subclass
        subclass_name = self.__class__.__name__

        # Prepare the log content
        log_lines = []

        # Add title with subclass name and timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_lines.append(f"{title} - {subclass_name} (Logged at: {timestamp})")
        total_width = sum(column_widths) + len(column_widths) - 1

        # Add header row
        log_lines.append("=" * total_width)
        log_lines.append(
            "  ".join(
                f"{header:<{width}}" for header, width in zip(headers, column_widths)
            )
        )
        log_lines.append("-" * total_width)

        # Add data rows
        for row in sanitized_rows:
            log_lines.append(
                "  ".join(
                    f"{row[header]:<{width}}"
                    for header, width in zip(headers, column_widths)
                )
            )

        # Add footer
        log_lines.append("=" * total_width)

        # Log to stderr (console)
        for line in log_lines:
            self.log(line)

        # Append to the file
        with open(self.details_log_path, "a") as log_file:
            log_file.write("\n".join(log_lines) + "\n\n")
            log_file.write("\n")

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
