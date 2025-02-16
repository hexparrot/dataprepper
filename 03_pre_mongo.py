#!/usr/bin/env python3
import os
import json
import re
import subprocess
from collections import defaultdict
from datetime import datetime


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
        "Date and time of logging (UTC)",
        "Date and Time",
        "Date and time",
        "Event Date",
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
            print(f"Skipping {filename}: Invalid JSON format")
            return

        data = self.convert_lat_long_fields(data)
        data = self.convert_iso_timestamps(data)
        self.aggregated_data[dataset][base_filename].extend(data)

    def process_files(self, dataset):
        """Process all JSON files in the source directory."""
        source_path = self.SOURCE_PATHS[dataset]
        for filename in os.listdir(source_path):
            input_filepath = os.path.join(source_path, filename)
            if not filename.endswith(".json"):
                continue  # Skip non-JSON files
            self.transform(dataset, input_filepath)

    def write_output(self, dataset):
        """Write all aggregated output files to the destination directory."""
        output_path = self.OUTPUT_PATHS[dataset]
        for base_filename, data in self.aggregated_data[dataset].items():
            output_filepath = os.path.join(output_path, base_filename)
            with open(output_filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            print(f"Aggregated and saved: {base_filename} → {output_filepath}")

    def run(self):
        """Run the full ingestion process for both datasets."""
        for dataset in ["pokemongo", "lyft", "waze"]:
            self.process_files(dataset)
            self.write_output(dataset)


if __name__ == "__main__":
    DataIngest().run()
