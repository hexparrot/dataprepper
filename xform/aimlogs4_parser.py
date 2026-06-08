#!/usr/bin/env python3
import sys
import os
import json
import re
import logging
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
from xform.base_parser import BaseParser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

TS_PATTERN = re.compile(r"\((\d{1,2}:\d{2}:\d{2}\s+(?:AM|PM))\)")


def _build_author_regex(name):
    """Build a regex that matches an AIM screenname with optional spaces,
    case-insensitive. E.g. 'korrinaisonfire' matches 'korrina is on fire',
    'KorrinaIsOnFire', etc."""
    # Strip spaces from the canonical name so we work with the base letters/digits
    base = name.replace(" ", "").lower()
    # Insert optional whitespace between every character
    spaced = r"\s*".join(re.escape(ch) for ch in base)
    return re.compile(spaced, re.IGNORECASE)


class AimLogs4Parser(BaseParser):
    def __init__(self, date_str, author1, author2):
        self.date_str = date_str
        self.author1 = author1
        self.author2 = author2
        self.author1_re = _build_author_regex(author1)
        self.author2_re = _build_author_regex(author2)
        # Combined pattern: match either author name at the end of a string,
        # right before the timestamp region
        self.either_author_re = re.compile(
            r"(" + self.author1_re.pattern + r"|" + self.author2_re.pattern + r")\s*$",
            re.IGNORECASE,
        )

    def _format_timestamp(self, raw_ts):
        try:
            ts_obj = date_parser.parse(f"{self.date_str} {raw_ts}")
            return ts_obj.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            logging.warning(f"Failed to parse timestamp: {raw_ts}")
            return None

    def _identify_author(self, matched_text):
        """Given a matched author string, normalize it back to one of the two
        canonical author names."""
        stripped = matched_text.replace(" ", "").lower()
        if stripped == self.author1.replace(" ", "").lower():
            return self.author1
        elif stripped == self.author2.replace(" ", "").lower():
            return self.author2
        else:
            return matched_text.strip()

    def _extract_records(self, html_content):
        records = []

        # Step 1: Flatten HTML to plain text
        soup = BeautifulSoup(html_content, "html.parser")
        flat_text = soup.get_text()

        # Step 2: Split on timestamps. re.split with a capture group keeps
        # the captured timestamp in the result list.
        # Result: [pre0, ts1, post1, ts2, post2, ...]
        parts = TS_PATTERN.split(flat_text)

        if len(parts) < 3:
            logging.warning("No timestamps found in document")
            return records

        # Step 3: Build raw triples of (pre_text, timestamp, post_text).
        # pre_text for record N is everything between the end of record N-1's
        # timestamp+colon and the start of record N's author name.
        # But since we split only on timestamps, the structure is:
        #   parts[0] = text before first timestamp (contains first author)
        #   parts[1] = first timestamp
        #   parts[2] = text after first timestamp, up to second timestamp
        #              (contains message, then next author)
        #   parts[3] = second timestamp
        #   ...

        triples = []
        for i in range(1, len(parts), 2):
            pre_text = parts[i - 1]
            ts_str = parts[i]
            post_text = parts[i + 1] if (i + 1) < len(parts) else ""
            triples.append((pre_text, ts_str, post_text))

        # Step 4: For each triple, extract author from pre_text and message
        # from post_text. The author is at the END of pre_text. The message
        # is everything in post_text UNTIL the next author name appears
        # (which will be consumed by the next triple's pre_text extraction).

        for idx, (pre_text, ts_str, post_text) in enumerate(triples):
            timestamp = self._format_timestamp(ts_str)
            if not timestamp:
                continue

            # --- Extract author from pre_text ---
            # The author name is at the tail end of pre_text.
            author_match = self.either_author_re.search(pre_text)
            if author_match:
                author = self._identify_author(author_match.group(1))
            else:
                logging.debug(
                    f"Could not identify author in pre_text for ts={ts_str!r}: "
                    f"{pre_text[-80:]!r}"
                )
                author = "unknown"

            # --- Extract message from post_text ---
            # post_text starts with ": message text..." and ends with
            # the next record's author name (or EOF for the last record).
            # Strip leading colon/whitespace.
            message_text = re.sub(r"^[\s:]+", "", post_text)

            # Find where the next author name starts at the END of message_text.
            # We search for either author pattern followed by only whitespace
            # until end of string.
            tail_author_re = re.compile(
                r"("
                + self.author1_re.pattern
                + r"|"
                + self.author2_re.pattern
                + r")\s*$",
                re.IGNORECASE,
            )
            tail_match = tail_author_re.search(message_text)
            if tail_match:
                message_text = message_text[: tail_match.start()]

            # Clean up
            message_text = message_text.strip()
            message_text = re.sub(r"\s+", " ", message_text)

            if not message_text:
                logging.debug(f"Empty message for {author!r} at {timestamp}, skipping")
                continue

            records.append(
                {
                    "author": author,
                    "timestamp": timestamp,
                    "message": message_text,
                }
            )

        return records


def main():
    if len(sys.argv) < 2:
        print("Usage: python aimlogs4_parser.py <YYYY-MM-DD>", file=sys.stderr)
        sys.exit(1)

    date_str = sys.argv[1]
    parser = AimLogs4Parser(date_str)
    html_content = sys.stdin.read()
    parsed_data = parser.parse(html_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
