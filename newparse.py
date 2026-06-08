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
from collections import Counter
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

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
REPORT_DIR = BASE_DIR / "userdata" / "reports"

VALID_EXTENSIONS = {".html", ".htm", ".json"}

# Successfully-parsed files whose JSON output is below this fraction of the
# original file size are flagged as "suspected incomplete parse". Heuristic and
# tunable: HTML markup overhead means good parses are already a small fraction
# of the input, so this threshold is deliberately low.
LOW_YIELD_RATIO = 0.05

# Cap how many entries we enumerate in the stderr summary lists.
MAX_LIST = 50


@dataclass
class FileOutcome:
    """Per-file processing result used for the parse-quality report."""

    file_path: str
    messenger: str
    owner: str
    participant: str
    status: str  # parsed | no_records | read_error | write_error
    input_bytes: int = 0
    output_bytes: int = 0
    record_count: int = 0
    chosen_parser: Optional[str] = None
    yield_ratio: float = 0.0


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
    """Process a single chat file through all parsers. Returns a FileOutcome."""
    logger.info(f"Processing: {file_path}")

    input_bytes = file_path.stat().st_size

    def outcome(status, **kw):
        return FileOutcome(
            file_path=str(file_path),
            messenger=messenger,
            owner=owner,
            participant=participant,
            status=status,
            input_bytes=input_bytes,
            **kw,
        )

    try:
        content = file_path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error(f"  Failed to read file: {e}")
        return outcome("read_error")

    results = run_all_parsers(content)
    chosen_name, records = select_best_parser(results)

    log_parser_results(results, chosen_name, file_path)

    if not records:
        logger.warning(f"  No records extracted from {file_path.name}")
        return outcome("no_records", chosen_parser=chosen_name)

    normalized = [normalize_record(msg) for msg in records]
    payload = "".join(json.dumps(record) + "\n" for record in normalized)
    output_bytes = len(payload.encode("utf-8"))
    ratio = output_bytes / input_bytes if input_bytes else 0.0

    output_filename = build_output_filename(
        messenger, owner, participant, file_path.name
    )
    output_path = output_dir / output_filename

    try:
        output_path.write_text(payload, encoding="utf-8")
        logger.info(f"  Saved {len(normalized)} records to {output_path.name}")
    except Exception as e:
        logger.error(f"  Failed to write output: {e}")
        return outcome(
            "write_error",
            chosen_parser=chosen_name,
            record_count=len(normalized),
            output_bytes=output_bytes,
            yield_ratio=ratio,
        )

    return outcome(
        "parsed",
        chosen_parser=chosen_name,
        record_count=len(normalized),
        output_bytes=output_bytes,
        yield_ratio=ratio,
    )


def scan_chat_files(base_dir):
    """
    Yield (file_path, messenger, owner, participant, is_match) for every file
    under base_dir/{messenger}/{owner}/{participant}/. `is_match` is True when
    the extension is one we attempt to parse.

    Yielding non-matching files too lets the caller report a true "files seen"
    count and a skipped-by-extension breakdown instead of silently dropping
    them. Recurses below the participant directory and matches case-insensitively.
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
                    if not file_path.is_file():
                        continue
                    is_match = file_path.suffix.lower() in VALID_EXTENSIONS
                    yield file_path, messenger, owner, participant, is_match


def walk_chat_directory(base_dir):
    """Yield (file_path, messenger, owner, participant) for matching chat files."""
    for file_path, messenger, owner, participant, is_match in scan_chat_files(
        base_dir
    ):
        if is_match:
            yield file_path, messenger, owner, participant


def build_report(outcomes, files_seen, skipped_ext, start_time, end_time):
    """Assemble the parse-quality report dict from per-file outcomes."""
    produced = [o for o in outcomes if o.status == "parsed"]
    unparsed = [o for o in outcomes if o.status != "parsed"]
    low_yield = sorted(
        (o for o in produced if o.yield_ratio < LOW_YIELD_RATIO),
        key=lambda o: o.yield_ratio,
    )
    return {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "summary": {
            "files_seen": files_seen,
            "files_attempted": len(outcomes),
            "files_skipped_by_extension": sum(skipped_ext.values()),
            "skipped_extensions": dict(sorted(skipped_ext.items())),
            "produced_output": len(produced),
            "unparsed": len(unparsed),
            "low_yield": len(low_yield),
            "low_yield_ratio_threshold": LOW_YIELD_RATIO,
        },
        "unparsed_files": [asdict(o) for o in unparsed],
        "low_yield_files": [asdict(o) for o in low_yield],
        "produced_files": [asdict(o) for o in produced],
    }


def log_summary(report):
    """Print the run summary and quality lists to the logger (stderr)."""
    s = report["summary"]
    logger.info("=" * 60)
    logger.info("Chat Log Parser - Complete")
    logger.info(f"  Files seen:           {s['files_seen']}")
    logger.info(f"  Files attempted:      {s['files_attempted']}")
    logger.info(
        f"  Skipped by extension: {s['files_skipped_by_extension']} "
        f"{s['skipped_extensions']}"
    )
    logger.info(f"  Produced output:      {s['produced_output']}")
    logger.info(f"  Unparsed:             {s['unparsed']}")
    logger.info(f"  Low-yield (<{LOW_YIELD_RATIO:.0%} of input): {s['low_yield']}")

    unparsed = report["unparsed_files"]
    if unparsed:
        logger.info("-" * 60)
        logger.info(f"UNPARSED FILES ({len(unparsed)}):")
        for o in unparsed[:MAX_LIST]:
            logger.info(f"    [{o['status']}] {o['file_path']}")
        if len(unparsed) > MAX_LIST:
            logger.info(f"    ... and {len(unparsed) - MAX_LIST} more")

    low_yield = report["low_yield_files"]
    if low_yield:
        logger.info("-" * 60)
        logger.info(
            f"LOW-YIELD FILES (suspected incomplete parse) ({len(low_yield)}):"
        )
        for o in low_yield[:MAX_LIST]:
            logger.info(
                f"    ratio={o['yield_ratio']:.3f} "
                f"({o['output_bytes']}/{o['input_bytes']} bytes, "
                f"{o['record_count']} records)  {o['file_path']}"
            )
        if len(low_yield) > MAX_LIST:
            logger.info(f"    ... and {len(low_yield) - MAX_LIST} more")
    logger.info("=" * 60)


def write_report(report, report_path):
    """Write the parse-quality report as JSON."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    logger.info(f"Report written to: {report_path}")


def main():
    logger.info("=" * 60)
    logger.info("Chat Log Parser - Starting")
    logger.info("=" * 60)

    if not RAW_CHAT_DIR.exists():
        logger.error(f"Input directory does not exist: {RAW_CHAT_DIR}")
        sys.exit(1)

    TRANSFORMED_DIR.mkdir(parents=True, exist_ok=True)

    start_time = datetime.now()
    outcomes = []
    files_seen = 0
    skipped_ext = Counter()

    for file_path, messenger, owner, participant, is_match in scan_chat_files(
        RAW_CHAT_DIR
    ):
        files_seen += 1
        if not is_match:
            skipped_ext[file_path.suffix.lower() or "<none>"] += 1
            continue
        outcomes.append(
            process_file(file_path, messenger, owner, participant, TRANSFORMED_DIR)
        )

    end_time = datetime.now()
    report = build_report(outcomes, files_seen, skipped_ext, start_time, end_time)
    log_summary(report)

    timestamp = end_time.strftime("%Y%m%d_%H%M%S")
    write_report(report, REPORT_DIR / f"newparse_report_{timestamp}.json")


if __name__ == "__main__":
    main()
