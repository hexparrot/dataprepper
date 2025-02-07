import os
import sys
import json
import logging
from structs.chat_record import ChatRecord
from structs.csv_record import CSVRecord, WazeCSVRecord
from structs.myanimelist_record import MyAnimeListRecord

# Configure logging to stderr
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Adjust paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USERDATA_DIR = os.path.join(BASE_DIR, "userdata")
RAW_DIR = os.path.join(USERDATA_DIR, "raw")
TRANSFORMED_DIR = os.path.join(USERDATA_DIR, "transformed")


def parse_chat(raw_dir, transformed_dir):
    """
    Parses chat logs from raw_dir, processes with ChatRecord,
    and writes structured JSON to transformed_dir.
    """
    logging.info(f"Processing chat logs in {raw_dir}...")
    ChatRecord.process_chat_directory(raw_dir, transformed_dir)
    logging.info(f"Chat processing complete. JSON files saved in {transformed_dir}")


def parse_csv(raw_dir, transformed_dir):
    """
    Processes CSV data stored in `raw_dir` and converts it to JSON.
    """
    logging.info(f"Processing CSV data in {raw_dir}...")
    record_handler = CSVRecord(raw_dir, transformed_dir)
    record_handler.process_directory()
    logging.info(f"CSV processing complete. JSON files saved in {transformed_dir}")


def parse_waze(raw_dir, transformed_dir):
    """
    Processes Waze CSV data, splitting stanzas and structuring JSON output.
    """
    logging.info(f"Processing Waze CSV data in {raw_dir}...")
    record_handler = WazeCSVRecord(raw_dir, transformed_dir)
    record_handler.process_waze_csv()
    logging.info(f"Waze processing complete. JSON files saved in {transformed_dir}")


def parse_myanimelist(raw_dir, transformed_dir):
    """
    Processes MyAnimeList XML exports and converts them to JSON.
    """
    logging.info(f"Processing MyAnimeList XML in {raw_dir}...")
    for file in os.listdir(raw_dir):
        file_path = os.path.join(raw_dir, file)
        if file.endswith(".xml"):
            with open(file_path, "r", encoding="utf-8") as f:
                xml_content = f.read()
            records = MyAnimeListRecord.parse(xml_content)
            output_file = os.path.join(
                transformed_dir, f"{os.path.splitext(file)[0]}.json"
            )
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=4)
            logging.info(f"Saved processed MyAnimeList records to {output_file}")


PARSERS = {
    "chat": parse_chat,
    "lyft": parse_csv,
    "niantic": parse_csv,
    "reddit": parse_csv,
    "waze": parse_waze,
    "myanimelist": parse_myanimelist,
}


def ensure_output_dir_exists(directory):
    """Ensure that the output directory exists."""
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def main():
    logging.info("Starting omni-parser.")
    ensure_output_dir_exists(TRANSFORMED_DIR)

    if not os.path.isdir(RAW_DIR):
        logging.error(f"RAW_DIR does not exist: {RAW_DIR}")
        sys.exit(1)

    for entry in os.listdir(RAW_DIR):
        raw_subdir_path = os.path.join(RAW_DIR, entry)
        if os.path.isdir(raw_subdir_path):
            logging.info(f"Found directory: {entry}")
            parser_func = PARSERS.get(entry)
            if parser_func:
                transformed_subdir_path = os.path.join(TRANSFORMED_DIR, entry)
                ensure_output_dir_exists(transformed_subdir_path)
                parser_func(raw_subdir_path, transformed_subdir_path)
            else:
                logging.warning(f"No parser found for directory: {entry}")
        else:
            logging.warning(f"Skipping file in raw root: {entry}")

    logging.info("Finished omni-parser.")


if __name__ == "__main__":
    main()
