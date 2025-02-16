#!/usr/bin/env python3
import os
import json
import re
import subprocess
from collections import defaultdict
from datetime import datetime


class PokemonGoIngest:
    """Handles the ingestion and transformation of Niantic JSON data."""

    SOURCE_PATH = "userdata/transformed/pokemongo"
    OUTPUT_PATH = "userdata/purposed/pokemongo"
    PIPE_SCRIPT = "pipes/rewrite_pipe_of_sorts.py"

    def __init__(self):
        """Initialize output directory and aggregated data store."""
        os.makedirs(self.OUTPUT_PATH, exist_ok=True)
        self.aggregated_data = defaultdict(list)

    @staticmethod
    def convert_lat_long_fields(data):
        """Convert all latitude/longitude fields to float values."""
        lat_long_pattern = re.compile(r".*latitude.*|.*longitude.*", re.IGNORECASE)

        for record in data:
            for key, value in record.items():
                if (
                    lat_long_pattern.match(key)
                    and isinstance(value, str)
                    and value.strip()
                ):
                    try:
                        record[key] = float(value)
                    except ValueError:
                        record[key] = None
        return data

    @staticmethod
    def rename_fields(data, original, adjusted):
        """
        Rename a specified field in all records.

        Parameters:
        - data (list of dicts): The dataset containing records.
        - original (str): The original field name to rename.
        - adjusted (str): The new field name.

        Returns:
        - list of dicts: Updated dataset with renamed fields.
        """
        for record in data:
            if original in record:
                record[adjusted] = record.pop(original)
        return data

    @staticmethod
    def convert_iso_timestamps(data):
        """Convert all timestamp-like fields to ISO 8601 format."""
        timestamp_formats = [
            "%Y-%m-%d %H:%M:%S.%f UTC",
            "%Y-%m-%d %H:%M:%S UTC",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%z",
        ]

        timestamp_pattern = re.compile(r".*(date|time|timestamp).*", re.IGNORECASE)

        for record in data:
            for key, value in record.items():
                if timestamp_pattern.match(key):
                    value = value.strip() if isinstance(value, str) else None
                    if not value:
                        record[key] = None
                        continue

                    for fmt in timestamp_formats:
                        try:
                            record[key] = datetime.strptime(value, fmt).isoformat()
                            break
                        except ValueError:
                            continue

                    if not isinstance(record[key], str):
                        record[key] = None
        return data

    def run_pipe_script(self, data):
        """Run an external pipe script for specific transformations."""
        process = subprocess.run(
            ["python3", self.PIPE_SCRIPT],
            input=json.dumps(data),
            text=True,
            capture_output=True,
        )
        if process.returncode == 0:
            return json.loads(process.stdout)
        else:
            print(f"Error processing with pipe: {process.stderr}")
            return data

    def transform(self, json_path):
        """Determine the correct transformation for a given file."""
        filename = os.path.basename(json_path)
        base_filename = re.sub(r"\d+", "", filename).strip()

        # Read JSON file
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            print(f"Skipping {filename}: Invalid JSON format")
            return

        def deploy_pokemon(data):
            """Sequentially process Deploy_Pokemon.json transformations."""
            data = self.convert_lat_long_fields(data)
            data = self.convert_iso_timestamps(data)
            return data

        def fitness_data(data):
            data = self.convert_iso_timestamps(data)
            return data

        def friend_list(data):
            data = self.convert_iso_timestamps(data)
            return data

        def gameplay_location(data):
            data = self.convert_iso_timestamps(data)
            return data

        # Define transformation functions
        transformations = {
            "App_Sessions.json": self.convert_iso_timestamps,
            "Deploy_Pokemon.json": deploy_pokemon,
            "GameplayLocationHistory.json": gameplay_location,
            "Pokestop_spin.json": deploy_pokemon,
            "SupportInteractions.json": lambda d: d,
            "User_Attribution_Installs.json": lambda d: d,
            "User_Attribution_Sessions.json": lambda d: d,
            "Gym_battle.json": self.convert_iso_timestamps,
            "InAppPurchases.json": lambda d: d,
            "Incense_encounter.json": self.convert_iso_timestamps,
            "Join_Raid_lobby.json": deploy_pokemon,
            "LiveEventLeaderboard.json": lambda d: d,
            "LiveEventRegistrationHistory_AsPurchaser.json": lambda d: d,
            "LiveEventRegistrationHistory_GiftedOrRedeemedTickets.json": lambda d: d,
            "LiveEventRegistrationHistory_Refund.json": lambda d: d,
            "Lure_encounter.json": deploy_pokemon,
            "Map_Pokemon_encounter.json": deploy_pokemon,
            "Sfida_capture.json": deploy_pokemon,
            "FriendList.json": friend_list,
            "FitnessData.json": fitness_data,
        }

        # Apply transformation if it exists
        if base_filename in transformations:
            print(f"Applying transformation for {filename} → {base_filename}")
            transformed_data = transformations[base_filename](data)
            self.aggregated_data[base_filename].extend(transformed_data)

    def process_files(self):
        """Process all JSON files in the source directory."""
        for filename in os.listdir(self.SOURCE_PATH):
            input_filepath = os.path.join(self.SOURCE_PATH, filename)
            if not filename.endswith(".json"):
                continue  # Skip non-JSON files

            self.transform(input_filepath)

    def write_output(self):
        """Write all aggregated output files to the destination directory."""
        for base_filename, data in self.aggregated_data.items():
            output_filepath = os.path.join(self.OUTPUT_PATH, base_filename)
            with open(output_filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            print(f"Aggregated and saved: {base_filename} → {output_filepath}")

    def run(self):
        """Run the full ingestion process."""
        self.process_files()
        self.write_output()


# Run the ingestion process
if __name__ == "__main__":
    PokemonGoIngest().run()
