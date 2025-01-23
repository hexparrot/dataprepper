#!/usr/bin/env python3
from datetime import datetime
from collections import defaultdict, Counter
from baseparser import BaseJSONPipe


class AugmentReplyDeltaParser(BaseJSONPipe):
    """Parser to add reply deltas for the two main authors in a conversation."""

    def __init__(self, verbose=True):
        super().__init__(verbose)
        self.last_reply_times = defaultdict(
            lambda: None
        )  # Tracks last reply times for authors
        self.first_message_flags = defaultdict(
            lambda: True
        )  # Tracks if it's the first message for each author
        self.main_authors = []

    def identify_main_authors(self, entries):
        """
        Identifies the two main authors based on their frequency of contributions.
        :param entries: List of JSON records.
        :return: List of the two main authors.
        """
        author_counts = Counter(
            entry.get("author", "unknown")
            for entry in entries
            if entry.get("author") != "system"
        )
        self.main_authors = [author for author, _ in author_counts.most_common(2)]
        return self.main_authors

    def calculate_delta(self, current_time, last_time, is_first_message):
        """
        Calculates the time delta in seconds between two timestamps.
        :param current_time: Current message timestamp as a datetime object.
        :param last_time: Last message timestamp as a datetime object.
        :param is_first_message: Whether this is the author's first message.
        :return: Delta in seconds or 0 for the first message.
        """
        if is_first_message:
            return 0
        if last_time is None:
            return -1
        delta = (current_time - last_time).total_seconds()
        return max(0, int(delta))  # Ensure the delta is always a positive integer

    def process_entry(self, entry):
        """
        Processes a single entry by adding author_replydelta and recipient_replydelta for main authors.
        :param entry: The JSON record.
        :return: The modified JSON record.
        """
        author = entry.get("author", "unknown")
        recipient = (
            self.main_authors[0]
            if author == self.main_authors[1]
            else self.main_authors[1]
        )
        timestamp = entry.get("timestamp", None)

        if timestamp is None:
            self.log(f"Missing timestamp for entry: {entry}")
            return entry

        try:
            current_time = datetime.fromisoformat(timestamp)
        except ValueError:
            self.log(f"Invalid timestamp format for entry: {entry}")
            return entry

        # Process author_replydelta
        if author in self.main_authors:
            is_first_message = self.first_message_flags[author]
            author_delta = self.calculate_delta(
                current_time, self.last_reply_times[author], is_first_message
            )
            entry["author_replydelta"] = author_delta
            self.last_reply_times[
                author
            ] = current_time  # Update last reply time for the author
            self.first_message_flags[author] = False
        else:
            entry.pop(
                "author_replydelta", None
            )  # Ensure no replydelta for non-main authors

        # Process recipient_replydelta
        if recipient in self.main_authors and author in self.main_authors:
            is_first_message = self.first_message_flags[recipient]
            recipient_delta = self.calculate_delta(
                current_time, self.last_reply_times[recipient], is_first_message
            )
            entry["recipient_replydelta"] = recipient_delta
        else:
            entry.pop(
                "recipient_replydelta", None
            )  # Ensure no replydelta for non-main authors

        return entry

    def run(self):
        """Overrides the run method to add reply deltas to records."""
        entries = self.read_input()

        # Identify the two main authors
        self.identify_main_authors(entries)

        # Process records
        processed_entries = [self.process_entry(entry) for entry in entries]

        # Log summary
        summary_rows = [
            {"Metric": "Total Records Processed", "Value": len(entries)},
            {
                "Metric": "Main Authors Identified",
                "Value": ", ".join(self.main_authors),
            },
        ]
        self.log_summary(
            "Reply Delta Augmentation Summary", ["Metric", "Value"], summary_rows
        )

        # Write output
        self.write_output(processed_entries)


if __name__ == "__main__":
    parser = AugmentReplyDeltaParser()
    parser.run()
