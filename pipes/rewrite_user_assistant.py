#!/usr/bin/env python3
import sys
import json
from basepipe import BaseJSONPipe


class RewriteUserAssistantPipe(BaseJSONPipe):
    """Pipe to rename authors to 'user' and 'assistant'."""

    def __init__(self, user_authors, verbose=True):
        """
        Initialize the pipe.
        :param user_authors: List of names to rename to 'user'.
        :param verbose: Enable or disable logging to stderr.
        """
        super().__init__(verbose)
        self.user_authors = set(user_authors)
        self.summary = {"user": 0, "assistant": 0}

    def process_entry(self, entry):
        """
        Renames the author of a record.
        :param entry: The JSON record.
        :return: The record with renamed author.
        """
        original_author = entry.get("author", "")
        if original_author in self.user_authors:
            entry["author"] = "user"
            self.summary["user"] += 1
        else:
            entry["author"] = "assistant"
            self.summary["assistant"] += 1

        return entry

    def run(self):
        """Overrides the run method to rename authors and display summary."""
        entries = self.read_input()

        # Process entries
        processed_entries = [self.process_entry(entry) for entry in entries]

        # Log summary
        self.log("Author Renaming Summary")
        self.log("=" * 40)
        self.log(f"Total records processed: {len(entries)}")
        self.log(f"Records renamed to 'user': {self.summary['user']}")
        self.log(f"Records renamed to 'assistant': {self.summary['assistant']}")
        self.log("=" * 40)

        # Write the processed entries to stdout
        self.write_output(processed_entries)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "Usage: ./rename_authors.py <comma-separated-user-names>", file=sys.stderr
        )
        sys.exit(1)

    user_authors = sys.argv[1].split(",")
    parser = RewriteUserAssistantPipe(user_authors)
    parser.run()
