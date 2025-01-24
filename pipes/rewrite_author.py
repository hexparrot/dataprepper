#!/usr/bin/env python3
from basepipe import BaseJSONPipe


class RenameAuthorPipe(BaseJSONPipe):
    """Pipe to update the author field of all JSON records to a single value."""

    def __init__(self, new_author, verbose=True):
        """
        Initialize the pipe with the new author value.
        :param new_author: The value to assign to the author field.
        :param verbose: Enable or disable logging to stderr.
        """
        super().__init__(verbose)
        self.new_author = new_author

    def process_entry(self, entry):
        """
        Processes a single entry by updating the author field.
        :param entry: The JSON record.
        :return: The modified JSON record.
        """
        entry["author"] = self.new_author
        return entry

    def run(self):
        """Overrides the run method to update the author field of all records."""
        entries = self.read_input()

        # Update the author field for each record
        processed_entries = [self.process_entry(entry) for entry in entries]

        # Log summary
        summary_rows = [
            {"Metric": "Total Records Processed", "Value": len(processed_entries)},
            {"Metric": "New Author Value", "Value": self.new_author},
        ]
        self.log_summary("Author Update Summary", ["Metric", "Value"], summary_rows)

        # Write the output
        self.write_output(processed_entries)


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: ./rename_author.py <new_author_value>", file=sys.stderr)
        sys.exit(1)

    new_author_value = sys.argv[1]
    parser = RenameAuthorPipe(new_author_value)
    parser.run()
