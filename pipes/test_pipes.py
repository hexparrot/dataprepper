import unittest
import json
import sys
import os
from io import StringIO
from contextlib import redirect_stdout, redirect_stderr

# Ensure the parsers directory is in the Python path
sys.path.insert(0, os.path.dirname(__file__))

# Import parsers directly from the local directory
from augment_author_age import AugmentAgePipe
from augment_convo_id import AugmentConvoIDPipe
from augment_duplicate_field import AddDuplicatedFieldPipe
from augment_replydeltas import AugmentReplyDeltaPipe
from drop_empty_values import DropEmptyValuesPipe
from drop_invalid_timestamp import VerifyTimestampPipe
from drop_nonmatching_authors import KeepAuthorsPipe
from drop_short_messages import FilterByLengthPipe
from rewrite_author import RenameAuthorPipe
from rewrite_author_merge import MergeAuthorsPipe
from rewrite_author_norm import NormalizeAuthorPipe
from rewrite_newlines import RewriteNewlinesPipe
from rewrite_omit_fields import RemoveFieldsPipe
from rewrite_user_assistant import RewriteUserAssistantPipe


class TestPipes(unittest.TestCase):
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
        """Test the AugmentConvoIDPipe."""
        stdout, stderr = self.run_parser(AugmentConvoIDPipe, self.test_input)
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
        """Test the AugmentReplyDeltaPipe."""
        stdout, stderr = self.run_parser(AugmentReplyDeltaPipe, self.test_input)
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

    def test_verify_timestamps(self):
        """Test the VerifyTimestampPipe."""
        test_json = json.dumps(
            [
                {
                    "author": "itsame",
                    "message": "Hello!",
                    "timestamp": "2014-02-25T00:31:28",  # Valid timestamp
                },
                {
                    "author": "mario",
                    "message": "Hola",
                    "timestamp": "invalid-timestamp",  # Invalid timestamp
                },
                {
                    "author": "luigi",
                    "message": "Hi!",
                    "timestamp": None,  # Missing timestamp
                },
            ]
        )
        stdout, stderr = self.run_parser(VerifyTimestampPipe, test_json)
        output = json.loads(stdout)

        # Verify that valid records are retained
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0]["author"], "itsame")
        self.assertEqual(output[0]["timestamp"], "2014-02-25T00:31:28")

    def test_verify_nonempty_values(self):
        """Test the DropEmptyValuesPipe."""
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
            DropEmptyValuesPipe,
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
        """Test the AugmentAgePipe."""
        stdout, stderr = self.run_parser(
            AugmentAgePipe, self.test_input, "2000-10-11", "itsame"
        )
        output = json.loads(stdout)

        # Verify ages are added
        self.assertIn("author_age", output[0])
        self.assertEqual(output[0]["author_age"], 13)  # Example age calculation
        self.assertNotIn("author_age", output[1])

    def test_keep_author(self):
        """Test the KeepAuthorPipe."""
        stdout, stderr = self.run_parser(
            KeepAuthorsPipe, self.test_input, "ramenten4buck,suddenwar"
        )
        output = json.loads(stdout)

        # Verify only specified authors are retained
        for record in output:
            self.assertIn(record["author"], ["itsame", "mario"])

    def test_normalize_author(self):
        """Test the NormalizeAuthorPipe."""
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
            NormalizeAuthorPipe, json.dumps(normalize_test_json)
        )
        output = json.loads(stdout)

        # Verify authors are normalized
        self.assertEqual(output[0]["author"], "itsame")
        self.assertEqual(output[1]["author"], "mario")

    def test_remove_fields(self):
        """Test the RemoveFieldsPipe."""
        stdout, stderr = self.run_parser(RemoveFieldsPipe, self.test_input, "timestamp")
        output = json.loads(stdout)

        # Verify specified fields are removed
        for record in output:
            self.assertNotIn("timestamp", record)

    def test_filter_by_length(self):
        """Test the FilterByLengthPipe."""
        # Run the parser with a minimum length of 12
        stdout, stderr = self.run_parser(FilterByLengthPipe, self.test_input, 15)
        output = json.loads(stdout)

        # Verify that only records meeting the length requirement are retained
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0]["author"], "mario")
        self.assertEqual(output[0]["message"], "thinking about rescuing a princess")

    def test_rewrite_authors(self):
        """Test the RewriteUserAssistantPipe."""
        # Specify authors to rename to 'user'
        user_authors = ["itsame"]

        # Run the parser
        stdout, stderr = self.run_parser(
            RewriteUserAssistantPipe, self.test_input, user_authors
        )
        output = json.loads(stdout)

        # Verify the authors are correctly renamed
        self.assertEqual(output[0]["author"], "user")
        self.assertEqual(output[1]["author"], "assistant")
        self.assertEqual(output[2]["author"], "user")
        self.assertEqual(output[3]["author"], "assistant")

    def test_rename_authors(self):
        """Test the RenameAuthorPipe."""
        # Specify authors to rename to 'user'
        user_authors = "user"

        # Run the parser
        stdout, stderr = self.run_parser(
            RenameAuthorPipe, self.test_input, user_authors
        )
        output = json.loads(stdout)

        # Verify the authors are correctly renamed
        self.assertEqual(output[0]["author"], "user")
        self.assertEqual(output[1]["author"], "user")
        self.assertEqual(output[2]["author"], "user")
        self.assertEqual(output[3]["author"], "user")

    def test_merge_authors(self):
        """Test the MergeAuthorsPipe."""
        # Specify authors to replace and the replacement value
        authors_to_replace = "itsame,mario"
        replacement_author = "luigi"

        # Run the parser
        stdout, stderr = self.run_parser(
            MergeAuthorsPipe, self.test_input, authors_to_replace, replacement_author
        )
        output = json.loads(stdout)

        # Verify the authors are correctly rewritten
        self.assertEqual(output[0]["author"], "luigi")  # itsame replaced
        self.assertEqual(output[1]["author"], "luigi")  # mario replaced
        self.assertEqual(output[2]["author"], "luigi")  # itsame replaced
        self.assertEqual(output[3]["author"], "luigi")  # mario replaced

        # Verify no data is dropped
        self.assertEqual(len(output), len(self.test_json))

    def test_normalize_newlines(self):
        """Test the NormalizeNewlinesPipe."""
        # Input with newlines and backslashes
        test_json_with_newlines = json.dumps(
            [
                {
                    "author": "itsame",
                    "message": "hello\nthere\\",
                    "timestamp": "2014-02-25T00:31:28",
                },
                {
                    "author": "mario",
                    "message": "hola\namigo\\",
                    "timestamp": "2014-02-25T00:31:32",
                },
            ]
        )

        # Expected output after normalization
        expected_output = [
            {
                "author": "itsame",
                "message": "hello there ",
                "timestamp": "2014-02-25T00:31:28",
            },
            {
                "author": "mario",
                "message": "hola amigo ",
                "timestamp": "2014-02-25T00:31:32",
            },
        ]

        # Run the parser
        stdout, stderr = self.run_parser(RewriteNewlinesPipe, test_json_with_newlines)
        output = json.loads(stdout)

        # Verify the normalization
        self.assertEqual(output, expected_output)

        # Verify no data is dropped
        self.assertEqual(len(output), len(json.loads(test_json_with_newlines)))


if __name__ == "__main__":
    unittest.main()
