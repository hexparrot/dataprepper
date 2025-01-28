#!/usr/bin/env python3
import sys
import json
from basepipe import BaseJSONPipe


class FilterByWordPipe(BaseJSONPipe):
    """Pipe to filter records containing specified words (case insensitive)."""

    def __init__(self, words_to_filter, verbose=True):
        """
        Initialize the pipe.
        :param words_to_filter: List of words to filter.
        :param verbose: Enable or disable logging to stderr.
        """
        super().__init__(verbose)
        self.words_to_filter = [word.lower() for word in words_to_filter]
        self.summary = {"kept": 0, "dropped": 0}

    def contains_filtered_word(self, message):
        """
        Checks if the message contains any of the filtered words (case insensitive).
        :param message: The message string to check.
        :return: True if the message contains any filtered word, otherwise False.
        """
        message_lower = message.lower()
        return any(word in message_lower for word in self.words_to_filter)

    def process_entry(self, entry):
        """
        Processes a single entry by checking if it contains any filtered words.
        :param entry: The JSON record.
        :return: The same record if it does not contain filtered words, otherwise None.
        """
        message = entry.get("message", "")

        if not self.contains_filtered_word(message):
            self.summary["kept"] += 1
            return entry
        else:
            self.summary["dropped"] += 1
            return None

    def run(self):
        """Overrides the run method to filter records based on the presence of filtered words."""
        entries = self.read_input()

        # Filter entries
        filtered_entries = [
            entry for entry in (self.process_entry(e) for e in entries) if entry
        ]

        # Log summary
        self.log("Filtered by Words")
        self.log("=" * 50)
        self.log(f"Total records processed: {len(entries)}")
        self.log(f"Records kept: {self.summary['kept']}")
        self.log(f"Records dropped: {self.summary['dropped']}")
        self.log("=" * 50)

        # Write filtered entries to stdout
        self.write_output(filtered_entries)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: ./filter_by_word.py <comma_separated_words>", file=sys.stderr)
        sys.exit(1)

    words_to_filter = sys.argv[1].split(",")
    parser = FilterByWordPipe(words_to_filter)
    parser.run()
