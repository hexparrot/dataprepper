import unittest
import json
import sys
import os
from io import StringIO
from contextlib import redirect_stdout, redirect_stderr

# Ensure the parsers directory is in the Python path
sys.path.insert(0, os.path.dirname(__file__))

# Import parsers directly from the local directory
from augment_convo_id import AugmentConvoIDParser
from augment_replydeltas import AugmentReplyDeltaParser
from augment_age import AugmentAgeParser
from duplicate_field import AddDuplicatedFieldParser
from filter_author import FilterAuthorsParser
from normalize_author import NormalizeAuthorParser
from remove_fields import RemoveFieldsParser
from verify_nonempty_values import CheckNonEmptyValuesParser


class TestParsers(unittest.TestCase):
    def setUp(self):
        """Set up test JSON input and reusable variables."""
        self.test_json = [
            {
                "author": "itsame",
                "message": "hello there",
                "timestamp": "2014-02-25T00:31:28",
            },
            {
                "author": "mario",
                "message": "hola",
                "timestamp": "2014-02-25T00:31:32",
            },
            {
                "author": "itsame",
                "message": "whats up!!",
                "timestamp": "2014-02-25T00:31:41",
            },
            {
                "author": "mario",
                "message": "thinking about rescuing a princess",
                "timestamp": "2014-02-25T00:31:59",
            },
        ]
        self.test_input = json.dumps(self.test_json)

    def run_parser(self, parser_class, input_json, *args):
        """Helper method to run a parser with given input JSON."""
        stdin = StringIO(input_json)
        stdout = StringIO()
        stderr = StringIO()

        parser = parser_class(*args)
        original_stdin = sys.stdin  # Backup the original stdin
        try:
            sys.stdin = stdin  # Redirect stdin to the StringIO object
            with redirect_stdout(stdout), redirect_stderr(stderr):
                parser.run()
        finally:
            sys.stdin = original_stdin  # Restore the original stdin

        return stdout.getvalue(), stderr.getvalue()

    def test_add_convo_id(self):
        """Test the AugmentConvoIDParser."""
        stdout, stderr = self.run_parser(AugmentConvoIDParser, self.test_input)
        output = json.loads(stdout)

        # Verify convo_id is added and formatted correctly
        for i, record in enumerate(output):
            self.assertIn("convo_id", record)
            self.assertTrue(record["convo_id"].endswith(f"-{i:05d}"))

        # Ensure summary log is present in stderr
        self.assertIn("Convo ID Assignment Summary", stderr)
        self.assertIn("Total Records Processed", stderr)
        self.assertIn("Random Prefix Used", stderr)

    def test_add_reply_deltas(self):
        """Test the AugmentReplyDeltaParser."""
        stdout, stderr = self.run_parser(AugmentReplyDeltaParser, self.test_input)
        output = json.loads(stdout)

        # Verify reply deltas are calculated correctly
        self.assertEqual(output[0]["author_replydelta"], 0)
        self.assertEqual(output[1]["author_replydelta"], 0)
        self.assertEqual(output[1]["recipient_replydelta"], 4)
        self.assertEqual(output[2]["author_replydelta"], 13)
        self.assertEqual(output[2]["recipient_replydelta"], 9)

        # Ensure summary log is present in stderr
        self.assertIn("Reply Delta Augmentation Summary", stderr)
        self.assertIn("Total Records Processed", stderr)
        self.assertIn("Main Authors Identified", stderr)

    def test_verify_nonempty_values(self):
        """Test the CheckNonEmptyValuesParser."""
        incomplete_json = json.dumps(
            [
                {
                    "author": "itsame",
                    "message": "",
                    "timestamp": "2014-02-25T00:31:28",
                },
                {
                    "author": "mario",
                    "message": "hola",
                    "timestamp": "2014-02-25T00:31:32",
                },
            ]
        )
        stdout, stderr = self.run_parser(
            CheckNonEmptyValuesParser,
            incomplete_json,
            ["author", "message", "timestamp"],
        )
        output = json.loads(stdout)

        # Verify that valid records are retained
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0]["author"], "mario")

        # Verify invalid records are logged to stderr
        self.assertIn("Dropped record", stderr)
        self.assertIn('"message": ""', stderr)

        # Ensure summary log is present in stderr
        self.assertIn("Non-Empty Fields Validation Summary", stderr)
        self.assertIn("Total Records Processed", stderr)
        self.assertIn("Proportion Valid (%)", stderr)

    def test_augment_age(self):
        """Test the AugmentAgeParser."""
        stdout, stderr = self.run_parser(
            AugmentAgeParser, self.test_input, "2000-10-11", "itsame"
        )
        output = json.loads(stdout)

        # Verify ages are added
        self.assertIn("author_age", output[0])
        self.assertEqual(output[0]["author_age"], 13)  # Example age calculation
        self.assertNotIn("author_age", output[1])

    def test_filter_author(self):
        """Test the FilterAuthorParser."""
        stdout, stderr = self.run_parser(
            FilterAuthorsParser, self.test_input, "ramenten4buck,suddenwar"
        )
        output = json.loads(stdout)

        # Verify only specified authors are retained
        for record in output:
            self.assertIn(record["author"], ["itsame", "mario"])

    def test_normalize_author(self):
        """Test the NormalizeAuthorParser."""
        normalize_test_json = [
            {
                "author": "  its a me   ",
                "message": "Hello!",
                "timestamp": "2014-02-25T00:31:28",
            },
            {
                "author": "MARIo",
                "message": "Hi there!",
                "timestamp": "2014-02-25T00:31:32",
            },
        ]
        stdout, stderr = self.run_parser(
            NormalizeAuthorParser, json.dumps(normalize_test_json)
        )
        output = json.loads(stdout)

        # Verify authors are normalized
        self.assertEqual(output[0]["author"], "itsame")
        self.assertEqual(output[1]["author"], "mario")

    def test_remove_fields(self):
        """Test the RemoveFieldsParser."""
        stdout, stderr = self.run_parser(
            RemoveFieldsParser, self.test_input, "timestamp"
        )
        output = json.loads(stdout)

        # Verify specified fields are removed
        for record in output:
            self.assertNotIn("timestamp", record)


if __name__ == "__main__":
    unittest.main()
