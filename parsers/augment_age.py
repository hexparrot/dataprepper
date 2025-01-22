#!/usr/bin/env python3
from datetime import datetime
from baseparser import BaseJSONParser


class AugmentAgeParser(BaseJSONParser):
    """Parser to augment JSON records with author age at the time of their message."""

    def __init__(self, birthdate, authors, verbose=True):
        super().__init__(verbose)
        self.birthdate = datetime.strptime(birthdate, "%Y-%m-%d")
        self.authors = set(authors.split(","))

    def calculate_age(self, timestamp):
        """
        Calculates the age based on the birthdate and the provided timestamp.
        :param timestamp: ISO 8601 timestamp of the record.
        :return: Age as an integer, or None if the timestamp is invalid.
        """
        try:
            message_date = datetime.fromisoformat(timestamp)
            age = (
                message_date.year
                - self.birthdate.year
                - (
                    (message_date.month, message_date.day)
                    < (self.birthdate.month, self.birthdate.day)
                )
            )
            return age
        except (ValueError, TypeError):
            return None

    def process_entry(self, entry):
        """
        Processes a single entry. Adds `author_age` if the author matches.
        :param entry: The JSON record.
        :return: The modified JSON record.
        """
        author = entry.get("author", "")
        timestamp = entry.get("timestamp", "")

        if author in self.authors:
            age = self.calculate_age(timestamp)
            if age is not None:
                entry["author_age"] = age
            else:
                self.log(
                    f"Invalid timestamp for entry with author '{author}': {timestamp}"
                )

        return entry

    def run(self):
        """Overrides the run method to augment records with author age."""
        entries = self.read_input()

        # Process records
        processed_entries = [self.process_entry(entry) for entry in entries]

        # Log summary
        matched_authors = [
            entry["author"] for entry in processed_entries if "author_age" in entry
        ]
        unmatched_count = len(entries) - len(matched_authors)
        summary_rows = [
            {"Metric": "Total Records Processed", "Value": len(entries)},
            {"Metric": "Records Augmented", "Value": len(matched_authors)},
            {"Metric": "Records Skipped", "Value": unmatched_count},
        ]
        self.log_summary("Age Augmentation Summary", ["Metric", "Value"], summary_rows)

        # Write output
        self.write_output(processed_entries)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print(
            "Usage: ./augment_age.py <birthdate> <author1,author2,...>", file=sys.stderr
        )
        sys.exit(1)

    birthdate = sys.argv[1]
    authors = sys.argv[2]
    parser = AugmentAgeParser(birthdate, authors)
    parser.run()
