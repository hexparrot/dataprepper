#!/usr/bin/env python3
from basepipe import BaseJSONPipe


class MergeAuthorsPipe(BaseJSONPipe):
    """Pipe to merge multiple authors into a single replacement value."""

    def __init__(self, authors_to_replace, replacement_author, verbose=True):
        """
        Initialize the pipe with authors to replace and the replacement value.
        :param authors_to_replace: A list of authors to replace.
        :param replacement_author: The value to replace the authors with.
        :param verbose: Enable or disable logging to stderr.
        """
        super().__init__(verbose)
        self.authors_to_replace = set(authors_to_replace.split(","))
        self.replacement_author = replacement_author

    def process_entry(self, entry):
        """
        Processes a single entry by updating the author field if it matches the target authors.
        :param entry: The JSON record.
        :return: The modified JSON record.
        """
        if entry.get("author") in self.authors_to_replace:
            entry["author"] = self.replacement_author
        return entry

    def run(self):
        """Overrides the run method to merge authors in all records."""
        entries = self.read_input()

        # Update the author field for matching records
        processed_entries = [self.process_entry(entry) for entry in entries]

        # Count replacements
        total_replacements = sum(
            1 for entry in entries if entry.get("author") in self.authors_to_replace
        )

        # Log summary
        summary_rows = [
            {"Metric": "Total Records Processed", "Value": len(processed_entries)},
            {"Metric": "Authors Replaced", "Value": total_replacements},
            {"Metric": "Replacement Author Value", "Value": self.replacement_author},
        ]
        self.log_summary("Authors Merge Summary", ["Metric", "Value"], summary_rows)

        # Write the output
        self.write_output(processed_entries)
