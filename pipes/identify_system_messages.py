#!/usr/bin/env python3
from collections import Counter
from baseparser import BaseJSONPipe


class IdentifySystemMessagesParser(BaseJSONPipe):
    """Parser to identify main authors and reformat system/auto-reply messages."""

    def __init__(self, verbose=True):
        super().__init__(verbose)
        self.main_authors = []
        self.secondary_author_count = 0

    def identify_main_authors(self, entries):
        """
        Identifies the two main authors based on frequency.
        :param entries: List of JSON records.
        :return: List of the two most frequent authors.
        """
        author_counts = Counter(entry.get("author", "unknown") for entry in entries)
        self.main_authors = [author for author, _ in author_counts.most_common(2)]
        return self.main_authors

    def process_entry(self, entry):
        """
        Processes a single entry. Reformats system messages.
        :param entry: The JSON record.
        :return: The modified JSON record.
        """
        author = entry.get("author", "")
        message = entry.get("message", "")

        if author.startswith("autoresponsefrom"):
            # Extract the original author name
            original_author = author[len("autoresponsefrom") :]
            # Reformat the entry as a system message
            entry["author"] = "system"
            entry["message"] = f"{original_author} autoresponse: {message}"
            self.secondary_author_count += 1

        return entry

    def run(self):
        """Overrides the run method to process the JSON and identify main authors."""
        entries = self.read_input()

        # Identify main authors
        self.identify_main_authors(entries)

        # Process each entry
        processed_entries = [self.process_entry(entry) for entry in entries]

        # Log summary
        summary_rows = [
            {"Metric": "Total Records Processed", "Value": len(entries)},
            {
                "Metric": "Main Authors Identified",
                "Value": ", ".join(self.main_authors),
            },
            {
                "Metric": "Secondary Authors (e.g., system)",
                "Value": self.secondary_author_count,
            },
        ]
        self.log_summary(
            "System Message Identification Summary", ["Metric", "Value"], summary_rows
        )

        # Write the output
        self.write_output(processed_entries)


if __name__ == "__main__":
    parser = IdentifySystemMessagesParser()
    parser.run()
