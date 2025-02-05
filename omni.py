#!/usr/bin/env python3
"""
Skeleton omni-parser script.

 - Iterates over userdata/raw, looking for recognized directory names.
 - For "images", uses the EXIF extractor to parse JPEG files.
 - Writes structured JSON to userdata/transformed/images/images_exif.json
 - Logs iteration details to stderr.
"""

import re
import os
import sys
import json
import logging
import hashlib
from datetime import datetime

from structs.exif_extractor import ExifRecord
from structs.chat_record import ChatRecord
from structs.myanimelist_record import MyAnimeListRecord
from structs.youtube_record import YouTubeRecord
from structs.csv_record import CSVRecord

# Configure logging to stderr
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Adjust these paths as needed
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # location of script
USERDATA_DIR = os.path.join(BASE_DIR, "userdata")
RAW_DIR = os.path.join(USERDATA_DIR, "raw")
TRANSFORMED_DIR = os.path.join(USERDATA_DIR, "transformed")


def parse_chat(raw_dir, transformed_dir):
    """
    Parses chat logs from raw_dir, finds the best parser,
    saves full conversation records to separate JSON files,
    and writes a summary of conversations to transformed_dir/chat_messages.json.
    """

    def generate_conversation_id(filename):
        """
        Generate a short, unique conversation ID based on filename.
        Uses a hash function for uniqueness while keeping it short.
        """
        hash_obj = hashlib.md5(filename.encode())
        return hash_obj.hexdigest()[:10]

    def filter_authors(authors):
        """
        Filters out authors whose names contain 'AUTO-REPLY' or 'Auto response from'.
        :param authors: A set of author names.
        :return: A filtered set of authors.
        """
        return {
            author
            for author in authors
            if not re.search(r"(AUTO-REPLY|Auto response from)", author, re.IGNORECASE)
        }

    def calculate_duration(start_time, end_time):
        """
        Calculates the duration in minutes between two timestamps.
        :param start_time: The earliest timestamp in the conversation.
        :param end_time: The latest timestamp in the conversation.
        :return: Duration in minutes as an integer.
        """
        try:
            start_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
            end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")
            return int((end_dt - start_dt).total_seconds() / 60)
        except ValueError:
            return 0

    logging.info(f"Parsing chat logs in: {raw_dir}")
    ensure_output_dir_exists(transformed_dir)

    summary_output_path = os.path.join(transformed_dir, "chat_messages.json")
    conversation_summaries = []

    conversations_output_dir = os.path.join(transformed_dir, "parsed")
    ensure_output_dir_exists(conversations_output_dir)

    conversation_data = {}

    for root, _, files in os.walk(raw_dir):
        for filename in files:
            file_path = os.path.join(root, filename)

            try:
                chat_parser = ChatRecord()
                records = chat_parser.parse_chat_file(file_path)

                if not records:
                    continue

                conversation_id = generate_conversation_id(filename)

                if conversation_id not in conversation_data:
                    conversation_data[conversation_id] = {
                        "conversation_id": conversation_id,
                        "authors": set(),
                        "earliest_timestamp": None,
                        "latest_timestamp": None,
                        "parser_name": "Unknown",
                        "messages": [],
                    }

                for record in records:
                    conversation_data[conversation_id]["authors"].add(record["author"])

                    record_timestamp = record["timestamp"]
                    if (
                        not conversation_data[conversation_id]["earliest_timestamp"]
                        or record_timestamp
                        < conversation_data[conversation_id]["earliest_timestamp"]
                    ):
                        conversation_data[conversation_id][
                            "earliest_timestamp"
                        ] = record_timestamp

                    if (
                        not conversation_data[conversation_id]["latest_timestamp"]
                        or record_timestamp
                        > conversation_data[conversation_id]["latest_timestamp"]
                    ):
                        conversation_data[conversation_id][
                            "latest_timestamp"
                        ] = record_timestamp

                    conversation_data[conversation_id]["parser_name"] = (
                        record["detail"].split(" via ")[-1].split(" on ")[0]
                    )

                    conversation_data[conversation_id]["messages"].append(record)

            except Exception as e:
                logging.error(f"Error processing {file_path}: {e}")

    for conversation_id, convo in conversation_data.items():
        filtered_authors = filter_authors(convo["authors"])
        authors_str = (
            ", ".join(sorted(filtered_authors)) if filtered_authors else "Unknown"
        )
        timestamp = convo["earliest_timestamp"] or "1970-01-01T00:00:00"
        parser_name = convo["parser_name"]
        duration = calculate_duration(
            convo["earliest_timestamp"], convo["latest_timestamp"]
        )

        msg_count = len(convo["messages"])

        for msg_num, record in enumerate(convo["messages"], start=1):
            record[
                "detail"
            ] = f"Message {msg_num}/{msg_count} via {parser_name} on {timestamp[:10]}"
            record["conversation_id"] = conversation_id

        conversation_file_path = os.path.join(
            conversations_output_dir, f"{conversation_id}.json"
        )
        with open(conversation_file_path, "w", encoding="utf-8") as f:
            json.dump(convo["messages"], f, indent=2)

        summary_record = {
            "conversation_id": conversation_id,
            "author": authors_str,
            "timestamp": timestamp,
            "duration": duration,
            "exchanges": msg_count,
            "detail": f"Conversation with ({authors_str}) via {parser_name} on {timestamp[:10]}",
        }

        if authors_str != "Unknown":
            conversation_summaries.append(summary_record)

    with open(summary_output_path, "w", encoding="utf-8") as f:
        json.dump(conversation_summaries, f, indent=2)

    logging.info(f"Full conversations saved to {conversations_output_dir}")
    logging.info(f"Chat summary saved to {summary_output_path}")


def parse_images(raw_dir, transformed_dir):
    """
    Parse image data (JPEG EXIF) from raw_dir, write results as JSON to transformed_dir.

    - Recursively scans `raw_dir` for .jpg/.jpeg files.
    - Uses ExifRecord to parse each file's EXIF data.
    - Collects all records and saves them in images_exif.json.
    """
    logging.info(f"Parsing images in directory: {raw_dir}")

    # Ensure output directory exists
    if not os.path.exists(transformed_dir):
        os.makedirs(transformed_dir, exist_ok=True)

    output_path = os.path.join(transformed_dir, "images_exif.json")
    all_records = []

    # Instantiate ExifRecord with base image directory
    exif_parser = ExifRecord(base_image_dir=raw_dir)

    # Recursively walk the raw_dir
    for root, _, files in os.walk(raw_dir):
        for filename in files:
            if filename.lower().endswith((".jpg", ".jpeg")):
                file_path = os.path.join(root, filename)
                logging.info(f"Found image: {file_path}")

                try:
                    exif_parser.parse(file_path)

                    if exif_parser.is_valid:
                        all_records.append(exif_parser._fields)
                        logging.info(f"Successfully parsed EXIF from {file_path}")
                    else:
                        logging.warning(
                            f"Skipping {file_path}; required fields missing."
                        )
                except Exception as e:
                    logging.error(f"Error parsing {file_path}: {e}")

    # Write the collected records to a single JSON file
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2)

    logging.info(f"Saved EXIF data to {output_path}")


def parse_myanimelist(raw_dir, transformed_dir):
    """
    Parses MyAnimeList XML exports from raw_dir and writes structured JSON to transformed_dir.
    """
    logging.info(f"Parsing MyAnimeList data in: {raw_dir}")
    ensure_output_dir_exists(transformed_dir)

    output_path = os.path.join(transformed_dir, "myanimelist.json")
    all_records = []

    for root, _, files in os.walk(raw_dir):
        for filename in files:
            if filename.endswith(".xml"):
                file_path = os.path.join(root, filename)
                records = MyAnimeListRecord.parse_file(file_path)
                all_records.extend(records)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2)

    logging.info(f"MyAnimeList data saved to {output_path}")


def parse_csv(raw_dir, transformed_dir):
    """
    Processes CSV data stored in `raw_dir`.
    - Traverses all subdirectories within `raw_dir`
    - Converts each CSV file into JSON
    - Saves the output in `transformed_dir`
    """
    if not os.path.exists(raw_dir):
        logging.error(f"The directory {raw_dir} does not exist.")
        return

    logging.info(f"Processing CSV data in {raw_dir}...")

    # Invoke CSV processing with the transformed directory
    record_handler = CSVRecord(raw_dir, transformed_dir)
    record_handler.process_directory()

    logging.info(f"CSV processing complete. JSON files saved in {transformed_dir}")


def parse_youtube(raw_dir, transformed_dir):
    """
    Parses YouTube watch history from raw_dir and writes structured JSON to transformed_dir.
    """
    logging.info(f"Parsing YouTube watch history in: {raw_dir}")
    ensure_output_dir_exists(transformed_dir)

    output_path = os.path.join(transformed_dir, "youtube.json")
    all_records = []

    for root, _, files in os.walk(raw_dir):
        for filename in files:
            file_path = os.path.join(root, filename)

            if filename.endswith(".html"):  # Only process HTML files
                records = YouTubeRecord.parse_file(file_path)
                all_records.extend(records)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2)

    logging.info(f"YouTube watch history saved to {output_path}")


PARSERS = {
    "chat": parse_chat,
    "images": parse_images,
    "myanimelist": parse_myanimelist,
    "lyft": parse_csv,
    "niantic": parse_csv,
    "reddit": parse_csv,
    "youtube": parse_youtube,
}


def ensure_output_dir_exists(directory):
    """Ensure that the output directory exists."""
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def main():
    logging.info("Starting omni-parser skeleton.")

    ensure_output_dir_exists(TRANSFORMED_DIR)

    # Loop through subdirectories of userdata/raw
    if not os.path.isdir(RAW_DIR):
        logging.error(f"RAW_DIR does not exist or is not a directory: {RAW_DIR}")
        sys.exit(1)

    for entry in os.listdir(RAW_DIR):
        raw_subdir_path = os.path.join(RAW_DIR, entry)
        if os.path.isdir(raw_subdir_path):
            logging.info(f"Found directory: {entry}")
            parser_func = PARSERS.get(entry)
            if parser_func:
                # Create corresponding output dir under transformed, if needed
                transformed_subdir_path = os.path.join(TRANSFORMED_DIR, entry)
                ensure_output_dir_exists(transformed_subdir_path)

                # Call the relevant parser
                parser_func(raw_subdir_path, transformed_subdir_path)
            else:
                # No parser mapped for this directory
                logging.warning(f"No parser found for directory: {entry}")
        else:
            # It's a file directly under userdata/raw (uncommon use-case)
            logging.warning(f"Found file (not a directory) in raw root: {entry}")

    logging.info("Finished omni-parser skeleton.")


if __name__ == "__main__":
    main()
