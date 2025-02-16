import os
import json
import logging
from structs.base_record import BaseRecord
from xform.csv_parser import CSVParser  # Import CSVParser

# Configure logging format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class CSVRecord(BaseRecord):
    """
    Handles CSV/TSV file parsing and batch processing.
    """

    def __init__(self, input_dir: str, output_dir: str):
        """
        :param input_dir: Directory containing CSV files.
        :param output_dir: Directory where JSON files will be saved.
        """
        super().__init__()
        self.input_dir = os.path.abspath(input_dir)
        self.output_root = os.path.abspath(output_dir)
        os.makedirs(self.output_root, exist_ok=True)
        self.parser = CSVParser()  # Initialize CSVParser

    def parse(self, raw_data):
        """
        Placeholder implementation to satisfy abstract method requirement.
        Since CSVRecord processes files in batch mode, this is not used.
        """
        return []

    def process_csv_file(self, csv_path: str, json_path: str):
        """
        Reads a CSV/TSV file and converts it to JSON using CSVParser.
        """
        try:
            with open(csv_path, "r", encoding="utf-8") as csv_file:
                csv_content = csv_file.read()
                records = self.parser.parse(csv_content)  # Use CSVParser

            self.save_to_file(json_path, records)
            logging.info(f"Successfully processed CSV file: {csv_path}")

        except Exception as e:
            logging.error(f"Failed to process {csv_path}: {e}")

    def save_to_file(self, json_path: str, records: list):
        """
        Saves parsed data to a JSON file.
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
        Traverses directory and converts all CSV/TSV files to JSON.
        """
        logging.info(f"Processing CSV files in directory: {self.input_dir}")

        file_count = 0
        for root, _, files in os.walk(self.input_dir):
            for file in files:
                if file.endswith(".csv") or file.endswith(".tsv"):
                    file_count += 1
                    csv_path = os.path.join(root, file)
                    json_path = os.path.join(
                        self.output_root, os.path.splitext(file)[0] + ".json"
                    )
                    self.process_csv_file(csv_path, json_path)

        if file_count == 0:
            logging.warning(f"No CSV files found in {self.input_dir}")
        else:
            logging.info(f"Successfully processed {file_count} CSV files.")


class WazeCSVRecord(CSVRecord):
    """
    Specialized CSV parser for Waze data, handling custom formats including stanza splitting.
    """

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
        logging.info(
            f"Splitting artificial CSV into stanzas in directory: {self.input_dir}"
        )

        stanzas = {}
        current_stanza_name = None
        current_stanza_data = []

        for root, _, files in os.walk(self.input_dir):
            for file in files:
                if file.endswith(".csv"):
                    file_path = os.path.join(root, file)
                    with open(file_path, "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()

                            if not line:  # Blank line indicates end of a stanza
                                if current_stanza_name and current_stanza_data:
                                    stanzas[current_stanza_name] = current_stanza_data
                                current_stanza_name = None
                                current_stanza_data = []
                                continue

                            if (
                                current_stanza_name is None
                            ):  # First line is the stanza name
                                current_stanza_name = line.replace(" ", "_").lower()
                            else:
                                current_stanza_data.append(line)

        if current_stanza_name and current_stanza_data:
            stanzas[current_stanza_name] = current_stanza_data  # Final stanza

        return stanzas
