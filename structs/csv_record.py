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


class WazeCSVRecord(CSVRecord):
    """
    A specialized CSV parser for Waze artificial CSVs.
    """

    def __init__(self, input_dir: str, output_dir: str):
        super().__init__(input_dir, output_dir)
        self.input_dir = os.path.abspath(input_dir)
        self.output_root = os.path.abspath(output_dir)
        os.makedirs(self.output_root, exist_ok=True)

    def process_waze_csv(self):
        """
        Processes a Waze CSV file by splitting stanzas and processing each as an independent CSV.
        """
        stanzas = self._split_stanzas()

        for stanza_name, stanza_data in stanzas.items():
            logging.info(f"Processing Waze stanza: {stanza_name}")

            temp_csv_path = os.path.join(self.output_root, f"{stanza_name}.csv")
            json_path = os.path.join(self.output_root, f"{stanza_name}.json")

            try:
                # Save stanza data as a valid CSV
                with open(temp_csv_path, "w", encoding="utf-8") as temp_csv:
                    temp_csv.write("\n".join(stanza_data))

                # Use existing CSV processing method
                self.process_csv_file(temp_csv_path, json_path)

                # Remove temporary CSV
                os.remove(temp_csv_path)

            except Exception as e:
                logging.error(f"Failed to process stanza {stanza_name}: {e}")

    def _split_stanzas(self):
        """
        Splits the artificial Waze CSV file into separate stanzas.
        :return: Dictionary with {stanza_name: list_of_csv_lines}
        """
        logging.info(f"Splitting artificial CSV into stanzas: {self.input_file}")

        stanzas = {}
        current_stanza_name = None
        current_stanza_data = []

        with open(self.input_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()

                if not line:  # Blank line indicates end of a stanza
                    if current_stanza_name and current_stanza_data:
                        stanzas[current_stanza_name] = current_stanza_data
                    current_stanza_name = None
                    current_stanza_data = []
                    continue

                if current_stanza_name is None:  # First line is the stanza name
                    current_stanza_name = line.replace(" ", "_").lower()
                else:
                    current_stanza_data.append(line)

        if current_stanza_name and current_stanza_data:
            stanzas[current_stanza_name] = current_stanza_data  # Final stanza

        return stanzas

    def process_directory(self):
        """
        Processes all Waze CSV files in a directory.

        - Iterates through all `.csv` files in the directory.
        - Calls `process_waze_csv()` on each file.
        - Saves structured JSON outputs in the configured `output_dir`.

        :param input_dir: Directory containing Waze artificial CSV files.
        """
        input_dir = os.path.abspath(self.input_dir)
        logging.info(f"Processing Waze CSV files in directory: {input_dir}")

        if not os.path.isdir(input_dir):
            logging.error(f"Invalid directory: {input_dir}")
            return

        file_count = 0
        for file in os.listdir(input_dir):
            file_path = os.path.join(input_dir, file)

            if file.endswith(".csv") and os.path.isfile(file_path):
                file_count += 1
                logging.info(f"Processing Waze CSV: {file_path}")

                # Update instance with new file path
                self.input_file = file_path
                self.process_waze_csv()

        if file_count == 0:
            logging.warning(f"No Waze CSV files found in {input_dir}")
        else:
            logging.info(f"Successfully processed {file_count} Waze CSV files.")
