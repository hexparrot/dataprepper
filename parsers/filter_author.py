#!/usr/bin/env python3
import sys
import json
from collections import defaultdict
from baseparser import BaseJSONParser


class FilterAuthorsParser(BaseJSONParser):
    """Parser to filter JSON records by a list of allowed authors."""

    def __init__(self, allowed_authors, verbose=True):
        super().__init__(verbose)
        self.allowed_authors = set(allowed_authors.split(","))
        self.kept_records = defaultdict(int)
        self.omitted_count = 0
        self.total_records = 0
        self.total_kept_length = 0
        self.total_omitted_length = 0

    def process_entry(self, entry):
        """
        Processes a single entry. Retains it if the author is in the allowed list.
        Tracks statistics for kept and omitted records.
        """
        self.total_records += 1
        author = entry.get("author", "unknown")
        record_length = len(json.dumps(entry))

        if author in self.allowed_authors:
            self.kept_records[author] += 1
            self.total_kept_length += record_length
            return entry  # Keep this record
        else:
            self.omitted_count += 1
            self.total_omitted_length += record_length
            return None  # Omit this record

    def run(self):
        """Overrides the run method to filter records and summarize results."""
        if not self.allowed_authors:
            self.log("No authors provided to retain. Exiting.")
            sys.exit(1)

        entries = self.read_input()

        # Process and retain only matching entries
        processed_entries = list(
            filter(None, (self.process_entry(entry) for entry in entries))
        )

        # Calculate summary statistics
        retained_percent = (
            (len(processed_entries) / self.total_records) * 100
            if self.total_records > 0
            else 0
        )
        average_kept_length = (
            (self.total_kept_length / len(processed_entries))
            if processed_entries
            else 0
        )
        average_omitted_length = (
            (self.total_omitted_length / self.omitted_count)
            if self.omitted_count > 0
            else 0
        )

        # General summary
        general_summary_rows = [
            {"Metric": "Total Records", "Value": self.total_records},
            {"Metric": "Retained Records", "Value": len(processed_entries)},
            {"Metric": "Omitted Records", "Value": self.omitted_count},
            {"Metric": "Percent Retained", "Value": f"{retained_percent:.2f}%"},
            {
                "Metric": "Average Length Retained",
                "Value": f"{average_kept_length:.2f} bytes",
            },
            {
                "Metric": "Average Length Omitted",
                "Value": f"{average_omitted_length:.2f} bytes",
            },
        ]

        # Author-specific summary
        author_summary_rows = [
            {"Author": author, "Kept Records": count}
            for author, count in self.kept_records.items()
        ]

        # Log summaries
        self.log_summary("General Summary", ["Metric", "Value"], general_summary_rows)
        self.log_summary(
            "Author Breakdown", ["Author", "Kept Records"], author_summary_rows
        )

        # Write retained records to stdout
        self.write_output(processed_entries)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: filter_authors.py <author1,author2,...>", file=sys.stderr)
        sys.exit(1)

    allowed_authors = sys.argv[1]
    parser = FilterAuthorsParser(allowed_authors)
    parser.run()
