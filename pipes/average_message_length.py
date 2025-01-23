#!/usr/bin/env python3
from baseparser import BaseJSONParser
from collections import defaultdict
import math


class AverageMessageLengthParser(BaseJSONParser):
    """Parser to calculate average message length per author."""

    def calculate_stats(self, entries):
        """
        Calculates mean and standard deviation of message lengths per author.
        Returns a dictionary with mean and standard deviation for each author.
        """
        stats = defaultdict(lambda: {"sum": 0, "count": 0, "squared_sum": 0})

        for entry in entries:
            author = entry.get("author", "unknown")
            message = entry.get("message", "")
            length = len(message)
            stats[author]["sum"] += length
            stats[author]["count"] += 1
            stats[author]["squared_sum"] += length**2

        # Calculate mean and standard deviation
        for author, data in stats.items():
            mean = data["sum"] / data["count"]
            variance = (data["squared_sum"] / data["count"]) - (mean**2)
            std_dev = math.sqrt(max(variance, 0))  # Ensure no negative variance
            stats[author]["mean"] = mean
            stats[author]["std_dev"] = std_dev

        return stats

    def classify_deviation(self, length, mean, std_dev):
        """
        Classifies the message length deviation using a bell curve.
        :param length: Message length.
        :param mean: Mean message length.
        :param std_dev: Standard deviation of message length.
        :return: A symbol (`++`, `+`, `=`, `-`, `--`).
        """
        if std_dev == 0:  # Handle the edge case where std_dev is zero
            return "="

        deviation = (length - mean) / std_dev

        if deviation > 2:
            return "++"
        elif deviation > 1:
            return "+"
        elif deviation > -1:
            return "="
        elif deviation > -2:
            return "-"
        else:
            return "--"

    def process_entry(self, entry):
        """
        Processes a single entry. Logs how the message length compares to the mean and standard deviation.
        """
        author = entry.get("author", "unknown")
        message = entry.get("message", "")
        message_length = len(message)

        # Get mean and std_dev for the author
        author_stats = self.author_stats.get(author, {"mean": 0, "std_dev": 0})
        mean = author_stats["mean"]
        std_dev = author_stats["std_dev"]

        # Classify the message length
        classification = self.classify_deviation(message_length, mean, std_dev)

        # Update summary statistics
        self.summary[author][classification] += 1

        # Log the formatted entry
        log_message = f"{author:<{self.author_field_width}} | Length: {message_length:5} | {classification:2} | Mean: {mean:6.2f} | StdDev: {std_dev:6.2f}"
        self.log(log_message)

        return entry

    def run(self):
        """Overrides the run method to calculate and log message length statistics."""
        entries = self.read_input()

        # Determine the width of the author column
        self.author_field_width = max(
            max((len(entry.get("author", "unknown")) for entry in entries), default=0),
            20,  # Minimum width of 20
        )

        # Calculate statistics
        self.author_stats = self.calculate_stats(entries)

        # Initialize summary statistics
        self.summary = defaultdict(lambda: defaultdict(int))

        # Log header
        self.log("Processing records:")
        self.log("=" * (self.author_field_width + 50))

        # Process and output entries
        processed_entries = [self.process_entry(entry) for entry in entries]

        # Summarize
        self.log("=" * (self.author_field_width + 50))
        self.log(f"Total records processed: {len(entries)}\n")

        # Convert summary data for tabular display
        summary_rows = [
            {
                "Author": author,
                "++": counts["++"],
                "+": counts["+"],
                "=": counts["="],
                "-": counts["-"],
                "--": counts["--"],
            }
            for author, counts in self.summary.items()
        ]

        # Use the base parser's `log_summary` to log summary statistics
        self.log_summary(
            "Summary Statistics", ["Author", "++", "+", "=", "-", "--"], summary_rows
        )

        self.write_output(processed_entries)


if __name__ == "__main__":
    parser = AverageMessageLengthParser()
    parser.run()
