#!/usr/bin/env python3
"""
Chat log parser - processes organized chat directories and outputs structured JSON.

Expected input structure:
    userdata/raw/chat/{messenger}/{owner}/{participant}/<file>

Output naming:
    {messenger}-{owner}-{participant}-{original_filename}.json
"""
import os
import sys
import json
import logging
from pathlib import Path

from xform.aimlogs_parser import AimLogsParser
from xform.aimlogs2_parser import AimLogs2Parser
from xform.fbchat_parser import FbchatParser
from xform.msn_parser import MsnParser
from xform.gvoice_parser import GvoiceParser
from xform.gchat_parser import GchatParser

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.resolve()
RAW_CHAT_DIR = BASE_DIR / "userdata" / "raw" / "chat"
TRANSFORMED_DIR = BASE_DIR / "userdata" / "transformed" / "chat"

VALID_EXTENSIONS = {".html", ".htm", ".json"}


def get_parsers():
    """Instantiate all available chat parsers."""
    return {
        "AimLogsParser": AimLogsParser(),
        "AimLogs2Parser": AimLogs2Parser(date_str="1970-01-01"),
        "FbchatParser": FbchatParser(),
        "MsnParser": MsnParser(),
        "GvoiceParser": GvoiceParser(),
        "GchatParser": GchatParser(),
    }


def run_all_parsers(file_content):
    """
    Run all parsers on file content.
    Returns dict of {parser_name: {"records": [...], "count": int}}
    """
    parsers = get_parsers()
    results = {}

    for name, parser in parsers.items():
        try:
            records = parser.parse(file_content)
            count = len(records) if records else 0
        except Exception as e:
            logger.debug(f"    {name}: error - {e}")
            records = []
            count = 0
        results[name] = {"records": records, "count": count}

    return results


def select_best_parser(results):
    """Select parser with most records. Returns (name, records) or (None, [])."""
    if not results:
        return None, []

    best_name = max(results, key=lambda k: results[k]["count"])
    best = results[best_name]

    if best["count"] == 0:
        return None, []

    return best_name, best["records"]


def log_parser_results(results, chosen_name, file_path):
    """Log the record count for each parser and which was selected."""
    logger.info(f"  Parser results for {file_path.name}:")
    for name, data in sorted(results.items(), key=lambda x: -x[1]["count"]):
        marker = " <-- SELECTED" if name == chosen_name else ""
        logger.info(f"    {name}: {data['count']} records{marker}")


def build_output_filename(messenger, owner, participant, original_name):
    """Build output filename: messenger-owner-participant-original_name.json"""
    stem = Path(original_name).stem
    return f"{messenger}-{owner}-{participant}-{stem}.json"


def normalize_record(msg):
    """Normalize a parsed message dict to standard output format."""
    return {
        "author": msg.get("author", "Unknown"),
        "timestamp": msg.get("timestamp", "1970-01-01T00:00:00"),
        "message": msg.get("message", ""),
    }


def process_file(file_path, messenger, owner, participant, output_dir):
    """Process a single chat file through all parsers."""
    logger.info(f"Processing: {file_path}")

    try:
        content = file_path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error(f"  Failed to read file: {e}")
        return False

    results = run_all_parsers(content)
    chosen_name, records = select_best_parser(results)

    log_parser_results(results, chosen_name, file_path)

    if not records:
        logger.warning(f"  No records extracted from {file_path.name}")
        return False

    normalized = [normalize_record(msg) for msg in records]

    output_filename = build_output_filename(
        messenger, owner, participant, file_path.name
    )
    output_path = output_dir / output_filename

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            for record in normalized:
                json.dump(record, f)
                f.write("\n")
        logger.info(f"  Saved {len(normalized)} records to {output_path.name}")
        return True
    except Exception as e:
        logger.error(f"  Failed to write output: {e}")
        return False


def walk_chat_directory(base_dir):
    """
    Yield (file_path, messenger, owner, participant) for each chat file.

    Expected structure: base_dir/{messenger}/{owner}/{participant}/<file>
    Recurses below the participant directory and matches extensions
    case-insensitively so all chat logs (.html/.htm/.json) are found.
    """
    for messenger_dir in sorted(base_dir.iterdir()):
        if not messenger_dir.is_dir():
            continue
        messenger = messenger_dir.name

        for owner_dir in sorted(messenger_dir.iterdir()):
            if not owner_dir.is_dir():
                continue
            owner = owner_dir.name

            for participant_dir in sorted(owner_dir.iterdir()):
                if not participant_dir.is_dir():
                    continue
                participant = participant_dir.name

                for file_path in sorted(participant_dir.rglob("*")):
                    if (
                        file_path.is_file()
                        and file_path.suffix.lower() in VALID_EXTENSIONS
                    ):
                        yield file_path, messenger, owner, participant


def main():
    logger.info("=" * 60)
    logger.info("Chat Log Parser - Starting")
    logger.info("=" * 60)

    if not RAW_CHAT_DIR.exists():
        logger.error(f"Input directory does not exist: {RAW_CHAT_DIR}")
        sys.exit(1)

    TRANSFORMED_DIR.mkdir(parents=True, exist_ok=True)

    total_files = 0
    successful = 0
    failed = 0

    for file_path, messenger, owner, participant in walk_chat_directory(RAW_CHAT_DIR):
        total_files += 1
        if process_file(file_path, messenger, owner, participant, TRANSFORMED_DIR):
            successful += 1
        else:
            failed += 1

    logger.info("=" * 60)
    logger.info("Chat Log Parser - Complete")
    logger.info(f"  Total files: {total_files}")
    logger.info(f"  Successful:  {successful}")
    logger.info(f"  Failed:      {failed}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
