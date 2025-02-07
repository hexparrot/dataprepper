import os
import json
import csv
import logging
from structs.base_record import BaseRecord
from structs.csv_record import CSVRecord, WazeCSVRecord

# Configure logging format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Adjust these paths as needed
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # location of script
USERDATA_DIR = os.path.join(BASE_DIR, "userdata")
RAW_DIR = os.path.join(USERDATA_DIR, "raw")
TRANSFORMED_DIR = os.path.join(USERDATA_DIR, "transformed")


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


def parse_waze(raw_dir, transformed_dir):
    """
    Processes Waze CSV data stored in `raw_dir`.
    - Splits stanzas in Waze files
    - Converts each stanza to JSON
    - Saves the output in `transformed_dir`
    """
    if not os.path.exists(raw_dir):
        logging.error(f"The directory {raw_dir} does not exist.")
        return

    logging.info(f"Processing Waze CSV data in {raw_dir}...")

    # Invoke Waze CSV processing with stanza handling
    record_handler = WazeCSVRecord(raw_dir, transformed_dir)
    record_handler.process_waze_csv()

    logging.info(f"Waze CSV processing complete. JSON files saved in {transformed_dir}")


# PARSERS dictionary including CSV and Waze CSV processing
PARSERS = {
    "lyft": parse_csv,
    "niantic": parse_csv,
    "reddit": parse_csv,
    "waze": parse_waze,
    # "chat": parse_chat,
    # "images": parse_images,
    # "myanimelist": parse_myanimelist,
    # "youtube": parse_youtube,
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
                logging.warning(f"No parser found for directory: {entry}")
        else:
            logging.warning(f"Found file (not a directory) in raw root: {entry}")

    logging.info("Finished omni-parser skeleton.")


if __name__ == "__main__":
    main()
