#!/usr/bin/env python3
import sys
import json
from collections import defaultdict
from baseparser import BaseJSONParser


class RemoveFieldsParser(BaseJSONParser):
    """Parser to remove specified fields from JSON records."""

    def __init__(self, fields, verbose=True):
        super().__init__(verbose)
        self.fields = [field.strip() for field in fields.split(",")]
        self.removed_count = 0
        self.total_removed_size = 0
        self.author_stats = defaultdict(
            lambda: {"records_modified": 0, "size_removed": 0}
        )

    def process_entry(self, entry):
        """
        Processes a single entry, removing specified fields.
        Tracks modifications and size of removed fields, broken down by author.
        """
        removed_size = 0
        modified = False
        author = entry.get("author", "unknown")

        for field in self.fields:
            if field in entry:
                # Track size of removed field
                removed_size += len(json.dumps(entry[field]))
                del entry[field]
                modified = True

        if modified:
            self.removed_count += 1
            self.total_removed_size += removed_size
            self.author_stats[author]["records_modified"] += 1
            self.author_stats[author]["size_removed"] += removed_size

        return entry

    def run(self):
        """Overrides the run method to remove fields from all records."""
        if not self.fields:
            self.log("No fields provided to remove. Exiting.")
            sys.exit(1)

        entries = self.read_input()

        # Process and output entries
        processed_entries = [self.process_entry(entry) for entry in entries]

        # Calculate overall stats
        total_records = len(entries)
        mean_size_removed = (
            self.total_removed_size / self.removed_count
            if self.removed_count > 0
            else 0
        )

        # Summary for authors
        author_summary_rows = [
            {
                "Author": author,
                "Records Modified": stats["records_modified"],
                "Total Size Removed": stats["size_removed"],
                "Mean Size Removed": (
                    stats["size_removed"] / stats["records_modified"]
                    if stats["records_modified"] > 0
                    else 0
                ),
            }
            for author, stats in self.author_stats.items()
        ]

        # General summary
        general_summary_rows = [
            {"Metric": "Total Records", "Value": total_records},
            {"Metric": "Records Modified", "Value": self.removed_count},
            {
                "Metric": "Aggregate Size Removed (bytes)",
                "Value": self.total_removed_size,
            },
            {
                "Metric": "Mean Size Removed (bytes)",
                "Value": f"{mean_size_removed:.2f}",
            },
        ]

        # Log general summary
        self.log_summary("General Summary", ["Metric", "Value"], general_summary_rows)

        # Log author-specific summary
        self.log_summary(
            "Author Breakdown",
            ["Author", "Records Modified", "Total Size Removed", "Mean Size Removed"],
            author_summary_rows,
        )

        self.write_output(processed_entries)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: remove_fields.py <field1,field2,...>", file=sys.stderr)
        sys.exit(1)

    fields_to_remove = sys.argv[1]
    parser = RemoveFieldsParser(fields_to_remove)
    parser.run()
