#!/usr/bin/env python3
import sys
import json
from basepipe import BaseJSONPipe


class FilterSpamPipe(BaseJSONPipe):
    """Pipe to filter spam records if the entire input is a single record."""

    def __init__(self, verbose=True):
        """
        Initialize the pipe.
        :param verbose: Enable or disable logging to stderr.
        """
        super().__init__(verbose)

    def is_spam(self, entries):
        """
        Determines if the input is spam based on the rules.
        :param entries: The list of JSON records in the conversation.
        :return: True if the conversation is spam, otherwise False.
        """
        # Check if the entire input consists of a single record
        return len(entries) == 1

    def process_entry(self, entry):
        """
        Dummy implementation to satisfy the abstract method.
        :param entry: A single JSON record.
        :return: The record itself (no processing needed).
        """
        return entry

    def run(self):
        """Overrides the run method to filter spam records based on input size."""
        entries = self.read_input()

        # Check if the entire input is a single record and should be dropped
        if self.is_spam(entries):
            self.log("Input is a single record. Dropping it as spam.")
            self.write_output([])  # Output an empty list
            return

        # Otherwise, write the original entries to stdout
        self.write_output(entries)


if __name__ == "__main__":
    parser = FilterSpamPipe()
    parser.run()
