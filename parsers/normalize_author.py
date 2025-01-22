#!/usr/bin/env python3
from baseparser import BaseJSONParser
import unicodedata
import re


class NormalizeAuthorParser(BaseJSONParser):
    """Parser to normalize author names."""

    def __init__(self, verbose=True):
        super().__init__(verbose)
        self.adjusted_count = 0
        self.unchanged_count = 0

    def normalize_author(self, author):
        """
        Normalizes the author name by:
        1. Removing all whitespace.
        2. Converting to lowercase.
        3. Ensuring only ASCII characters.
        :param author: The original author name.
        :return: The normalized author name.
        """
        if author is None:
            return "unknown"

        # Remove whitespace
        author = re.sub(r"\s+", "", author)

        # Convert to lowercase
        author = author.lower()

        # Normalize to ASCII characters
        author = (
            unicodedata.normalize("NFKD", author)
            .encode("ascii", "ignore")
            .decode("ascii")
        )

        return author

    def process_entry(self, entry):
        """
        Processes a single entry, normalizing the author name.
        Logs only when the author name is adjusted.
        """
        original_author = entry.get("author", None)
        normalized_author = self.normalize_author(original_author)

        if original_author != normalized_author:
            # Log the change
            self.log(
                f"Original: {original_author:<30} | Normalized: {normalized_author}"
            )
            self.adjusted_count += 1
        else:
            self.unchanged_count += 1

        # Update the entry with the normalized author name
        entry["author"] = normalized_author

        return entry

    def run(self):
        """Overrides the run method to normalize author names for all records."""
        entries = self.read_input()

        # Log header
        self.log("Normalizing author names:")
        self.log("=" * 60)

        # Process and output entries
        processed_entries = [self.process_entry(entry) for entry in entries]

        # Summarize
        total_records = len(entries)
        summary_rows = [
            {
                "Category": "Adjusted Records",
                "Count": self.adjusted_count,
                "Proportion": f"{(self.adjusted_count / total_records):.2%}",
            },
            {
                "Category": "Unchanged Records",
                "Count": self.unchanged_count,
                "Proportion": f"{(self.unchanged_count / total_records):.2%}",
            },
        ]

        # Use the base parser's `log_summary` to log summary statistics
        self.log("")
        self.log_summary(
            "Summary Statistics",
            ["Category", "Count", "Proportion"],
            summary_rows,
        )

        self.write_output(processed_entries)


if __name__ == "__main__":
    parser = NormalizeAuthorParser()
    parser.run()
