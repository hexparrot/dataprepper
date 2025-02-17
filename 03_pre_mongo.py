#!/usr/bin/env python3
import os
import json
import re
import subprocess
import logging
from collections import defaultdict
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class DataIngest:
    """Handles the ingestion and transformation of Niantic and Lyft JSON data."""

    SOURCE_PATHS = {
        "pokemongo": "userdata/transformed/pokemongo",
        "lyft": "userdata/transformed/lyft",
        "waze": "userdata/transformed/waze",
    }
    OUTPUT_PATHS = {
        "pokemongo": "userdata/purposed/pokemongo",
        "lyft": "userdata/purposed/lyft",
        "waze": "userdata/purposed/waze",
    }
    PIPE_SCRIPT = "pipes/rewrite_pipe_of_sorts.py"

    TIMESTAMP_FIELDS = {
        "Timestamp",
        "timestamp",
        "Date and time of logging (UTC)",
        "Date and Time",
        "Date and time",
        "Event Date",
        "Entry Date",
        "Last Login",
        "Date",
        "Login Time",
        "Logout Time",
    }
    TIMESTAMP_SUFFIXES = {"_at", "_timestamp", "_time", "friendship start"}
    TIMESTAMP_FORMATS = [
        "%m/%d/%Y %H:%M:%S UTC",
        "%Y-%m-%d %H:%M:%S.%f UTC",
        "%Y-%m-%d %H:%M:%S UTC",
        "%Y-%m-%d %H:%M:%S GMT",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S.%f",  # Added format for call_completed_at
    ]

    LAT_LONG_FIELDS = {"latitude", "longitude", "lat", "lng"}
    LAT_LONG_SUFFIXES = {
        "_lat",
        "_lng",
        "_latitude",
        "_longitude",
        "location reported by game",
    }

    def __init__(self):
        """Initialize output directories and aggregated data stores."""
        for path in self.OUTPUT_PATHS.values():
            os.makedirs(path, exist_ok=True)
        self.aggregated_data = {
            "pokemongo": defaultdict(list),
            "lyft": defaultdict(list),
            "waze": defaultdict(list),
        }

    def convert_lat_long_fields(self, data):
        """Convert all latitude/longitude fields to float values if they appear valid."""
        for record in data:
            for key, value in record.items():
                if (
                    (
                        key.lower() in self.LAT_LONG_FIELDS
                        or any(
                            key.lower().endswith(suffix)
                            for suffix in self.LAT_LONG_SUFFIXES
                        )
                    )
                    and isinstance(value, str)
                    and value.strip()
                ):
                    try:
                        converted_value = float(value)
                        if (
                            -90 <= converted_value <= 90
                            or -180 <= converted_value <= 180
                        ):
                            record[key] = converted_value
                    except ValueError:
                        pass  # Retain original value if conversion fails
        return data

    def convert_iso_timestamps(self, data):
        """Convert all timestamp-like fields to ISO 8601 format."""
        for record in data:
            for key, value in record.items():
                if key in self.TIMESTAMP_FIELDS or any(
                    key.lower().endswith(suffix) for suffix in self.TIMESTAMP_SUFFIXES
                ):
                    value = value.strip() if isinstance(value, str) else None
                    if not value:
                        record[key] = None
                        continue

                    for fmt in self.TIMESTAMP_FORMATS:
                        try:
                            record[key] = datetime.strptime(value, fmt).isoformat()
                            break
                        except ValueError:
                            continue

                    if not isinstance(record[key], str):
                        record[key] = None
        return data

    def transform(self, dataset, json_path):
        """Determine the correct transformation for a given file."""
        filename = os.path.basename(json_path)
        base_filename = re.sub(r"\d+", "", filename).strip()

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            logging.warning(f"Skipping {filename}: Invalid JSON format")
            return

        data = self.convert_lat_long_fields(data)
        data = self.convert_iso_timestamps(data)
        self.aggregated_data[dataset][base_filename].extend(data)

    def process_files(self, dataset):
        """Process all JSON files in the source directory."""
        source_path = self.SOURCE_PATHS[dataset]
        output_path = self.OUTPUT_PATHS[dataset]

        for filename in os.listdir(source_path):
            input_filepath = os.path.join(source_path, filename)

            if not filename.endswith(".json"):
                continue  # Skip non-JSON files

            # Special handling for "waze/general_info.json"
            if dataset == "waze" and filename == "general_info.json":
                logging.info(f"Handling edge-case file: {filename}")
                output_filepath = os.path.join(output_path, filename)
                filename = os.path.basename(input_filepath)
                base_filename = re.sub(r"\d+", "", filename).strip()

                try:
                    # Read the original JSON content
                    with open(input_filepath, "r", encoding="utf-8") as f:
                        original_data = json.load(f)

                    # Process through rewrite_transpose.py
                    result = subprocess.run(
                        ["./pipes/rewrite_transpose.py"],
                        input=json.dumps(original_data, indent=4),
                        text=True,
                        capture_output=True,
                        check=True,
                    )

                    # Load the modified JSON
                    processed_data = json.loads(result.stdout)

                    # Apply transformations
                    processed_data = self.convert_lat_long_fields(processed_data)
                    processed_data = self.convert_iso_timestamps(processed_data)

                    # Write transformed data
                    with open(output_filepath, "w", encoding="utf-8") as f:
                        json.dump(processed_data, f, indent=4)

                    logging.info(f"Post-processed: {filename} → {output_filepath}")
                except subprocess.CalledProcessError as e:
                    logging.error(
                        f"Error processing {filename} with rewrite_transpose.py: {e}"
                    )
            elif dataset == "waze" and filename == "search_history.json":
                logging.info(f"Handling edge-case file: {filename}")
                output_filepath = os.path.join(output_path, filename)
                filename = os.path.basename(input_filepath)
                base_filename = re.sub(r"\d+", "", filename).strip()

                try:
                    # Read the original JSON content
                    with open(input_filepath, "r", encoding="utf-8") as f:
                        original_data = json.load(f)

                    # Process through rewrite_transpose.py
                    field_names = [
                        "timestamp",
                        "search_terms",
                        "latitude",
                        "longitude",
                    ]  # Example field names

                    result = subprocess.run(
                        ["./pipes/rewrite_fieldnames.py"]
                        + field_names,  # Pass field names as arguments
                        input=json.dumps(original_data, indent=4),
                        text=True,
                        capture_output=True,
                        check=True,
                    )

                    # Load the modified JSON
                    processed_data = json.loads(result.stdout)

                    # Apply transformations
                    processed_data = self.convert_lat_long_fields(processed_data)
                    processed_data = self.convert_iso_timestamps(processed_data)

                except TypeError as e:
                    logging.error(
                        f"Error processing {filename} with rewrite_fieldnames.py: {e}"
                    )
                except subprocess.CalledProcessError as e:
                    logging.error(
                        f"Error processing {filename} with rewrite_fieldnames.py: {e}"
                    )
                else:
                    # define a local test
                    def is_valid_lat_long(record):
                        """Checks if the record has valid numerical latitude and longitude."""
                        try:
                            lat = record.get("latitude")
                            lon = record.get("longitude")

                            # Ensure latitude & longitude are present and convertible to float
                            if lat is None or lon is None:
                                return False

                            lat = float(lat)
                            lon = float(lon)

                            # Ensure they are within valid geographic bounds
                            return -90 <= lat <= 90 and -180 <= lon <= 180

                        except (ValueError, TypeError):
                            return False  # Non-numeric or invalid data

                    # Write transformed data
                    record_count = len(processed_data)
                    processed_data = [
                        record for record in processed_data if is_valid_lat_long(record)
                    ]
                    logging.warning(
                        f"Dropped {record_count - len(processed_data)} invalid records."
                    )
                    with open(output_filepath, "w", encoding="utf-8") as f:
                        json.dump(processed_data, f, indent=4)

                    logging.info(f"Post-processed: {filename} → {output_filepath}")
            else:
                # Process all other JSON files normally
                self.transform(dataset, input_filepath)

    def write_output(self, dataset):
        """Write all aggregated output files to the destination directory."""
        output_path = self.OUTPUT_PATHS[dataset]
        for base_filename, data in self.aggregated_data[dataset].items():
            output_filepath = os.path.join(output_path, base_filename)

            # Write JSON normally
            with open(output_filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            logging.info(f"Saved: {base_filename} → {output_filepath}")

    def run(self):
        """Run the full ingestion process for both datasets."""
        for dataset in ["pokemongo", "lyft", "waze"]:
            self.process_files(dataset)
            self.write_output(dataset)


if __name__ == "__main__":
    DataIngest().run()
