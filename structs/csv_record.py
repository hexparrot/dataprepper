import os
import json
import csv
import logging
from structs.base_record import BaseRecord

# Configure logging to match omni-parser's format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class CSVRecord(BaseRecord):
    """
    Handles:
    - Traversing directories for CSV files.
    - Processing CSV data.
    - Saving structured JSON output.
    """

    def __init__(self, input_dir: str, output_dir: str):
        """
        :param input_dir: Root directory containing CSV files.
        :param output_dir: Directory where JSON files will be saved.
        """
        super().__init__(
            required_fields=[]
        )  # No required fields, keeps all original data
        self.input_dir = os.path.abspath(input_dir)
        self.output_root = os.path.abspath(output_dir)
        os.makedirs(self.output_root, exist_ok=True)

    def parse(self, raw_data):
        """
        Required by BaseRecord, but not used in batch processing.
        """
        pass  # Not used since CSVRecord processes entire files

    def process_csv_file(self, csv_path: str, json_path: str):
        """
        Reads a CSV file and converts it to JSON.
        """
        try:
            with open(csv_path, "r", encoding="utf-8") as csv_file:
                reader = csv.DictReader(csv_file)
                records = [row for row in reader]  # Keep CSV structure as-is

            self.save_to_file(json_path, records)
            logging.info(f"Successfully processed CSV file: {csv_path}")

        except Exception as e:
            logging.error(f"Failed to process {csv_path}: {e}")

    def save_to_file(self, json_path: str, records: list):
        """
        Saves the JSON data to a file.
        """
        try:
            os.makedirs(os.path.dirname(json_path), exist_ok=True)
            with open(json_path, "w", encoding="utf-8") as json_file:
                json.dump(records, json_file, indent=4)
            logging.info(f"Saved {len(records)} records to {json_path}")
        except Exception as e:
            logging.error(f"Failed to save {json_path}: {e}")

    def process_directory(self):
        """
        Traverses the directory and converts all CSV files to JSON.
        """
        logging.info(f"Processing CSV files in directory: {self.input_dir}")

        file_count = 0
        for root, _, files in os.walk(self.input_dir):
            for file in files:
                if file.endswith(".csv") or file.endswith(
                    ".tsv"
                ):  # Process CSV,TSV files
                    file_count += 1
                    csv_path = os.path.join(root, file)

                    # Compute relative path from the input directory
                    relative_path = os.path.relpath(csv_path, self.input_dir)

                    # Construct JSON path in the output directory
                    json_path = os.path.join(
                        self.output_root, os.path.splitext(relative_path)[0] + ".json"
                    )

                    # Process CSV file
                    self.process_csv_file(csv_path, json_path)

        if file_count == 0:
            logging.warning(f"No CSV files found in {self.input_dir}")
        else:
            logging.info(
                f"Successfully processed {file_count} CSV files in {self.input_dir}"
            )
