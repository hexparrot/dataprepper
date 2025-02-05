#!/usr/bin/env python3
import os
import sys
import json
import logging
import re
from fractions import Fraction  # Needed to handle fractional EXIF values

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def parse_exif_list(exif_string):
    """Parses EXIF GPS list (e.g., "[31, 13 1/4]") and converts it to a list of floats."""
    try:
        items = exif_string.strip("[]").split(",")
        parsed_items = []
        for item in items:
            item = item.strip()
            if "/" in item:
                parsed_items.append(float(Fraction(item)))  # Convert fraction to float
            else:
                parsed_items.append(float(item))
        return parsed_items
    except Exception as e:
        logging.warning(f"Failed to parse EXIF list: {exif_string}. Error: {e}")
        return None


def exif_to_decimal(degrees_list, ref):
    """Converts EXIF GPS coordinates stored as lists to decimal format."""
    try:
        degrees = float(degrees_list[0])
        minutes = float(degrees_list[1])
        seconds = float(degrees_list[2])

        decimal = degrees + (minutes / 60) + (seconds / 3600)

        # Make latitude/longitude negative if in S or W hemisphere
        if ref in ["S", "W"]:
            decimal *= -1

        return decimal

    except Exception as e:
        logging.warning(
            f"Failed to convert EXIF GPS: {degrees_list}, {ref}. Error: {e}"
        )
        return None


def extract_standard_gps(entry, file_path):
    """
    Extracts Player_Latitude and Player_Longitude data.
    Returns a list of structured records.
    """
    extracted_records = []
    lat = entry.get("Player_Latitude")
    lon = entry.get("Player_Longitude")
    timestamp = entry.get("Timestamp")

    if lat is not None and lon is not None and timestamp:
        extracted_records.append(
            {
                "author": "unspecified",
                "detail": "Pokemon Go ping",
                "latitude": float(lat),
                "longitude": float(lon),
                "timestamp": timestamp,
                "source_file": file_path,
                "source_type": "Player_Location",
            }
        )

    return extracted_records


def extract_exif_gps(entry, file_path):
    """
    Extracts EXIF GPS data from image metadata.
    Returns a list of structured records.
    """
    extracted_records = []
    lat = entry.get("GPS GPSLatitude")
    lon = entry.get("GPS GPSLongitude")
    lat_ref = entry.get("GPS GPSLatitudeRef", "")
    lon_ref = entry.get("GPS GPSLongitudeRef", "")
    timestamp = entry.get("Timestamp") or entry.get("EXIF DateTimeOriginal")

    if isinstance(lat, str) and lat.startswith("["):
        lat = parse_exif_list(lat)
    if isinstance(lon, str) and lon.startswith("["):
        lon = parse_exif_list(lon)

    if isinstance(lat, list) and lat_ref:
        lat = exif_to_decimal(lat, lat_ref)
    if isinstance(lon, list) and lon_ref:
        lon = exif_to_decimal(lon, lon_ref)

    if lat is not None and lon is not None and timestamp:
        extracted_records.append(
            {
                "author": "unspecified",
                "detail": "Taking picture",
                "latitude": float(lat),
                "longitude": float(lon),
                "timestamp": timestamp,
                "source_file": file_path,
                "source_type": "EXIF_GPS",
            }
        )

    return extracted_records


def extract_lyft_gps(entry, file_path):
    """
    Extracts Lyft ride data (requested, pickup, dropoff).
    Returns a list of structured records.
    """
    extracted_records = []
    for loc_type, lat_key, lon_key, time_key in [
        (
            "rideshare requested",
            "requested_lat",
            "requested_lng",
            "requested_timestamp",
        ),
        ("rideshare pickup", "pickup_lat", "pickup_lng", "pickup_timestamp"),
        ("rideshare dropoff", "dropoff_lat", "dropoff_lng", "dropoff_timestamp"),
    ]:
        lat = entry.get(lat_key)
        lon = entry.get(lon_key)
        timestamp = entry.get(time_key)

        if lat is not None and lon is not None and timestamp:
            extracted_records.append(
                {
                    "author": "unspecified",
                    "detail": "Rideshare event",
                    "latitude": float(lat),
                    "longitude": float(lon),
                    "timestamp": timestamp,
                    "location_type": loc_type,
                    "source_file": file_path,
                    "source_type": "Lyft_Ride",
                }
            )

    return extracted_records


def process_json_entry(entry, file_path):
    """
    Processes a single JSON object for location data.
    Calls each extraction function to collect structured records.
    """
    extracted_records = []
    extracted_records.extend(extract_standard_gps(entry, file_path))
    extracted_records.extend(extract_exif_gps(entry, file_path))
    extracted_records.extend(extract_lyft_gps(entry, file_path))

    return extracted_records


def extract_coordinates_from_json(file_path):
    """
    Reads a JSON file and extracts GPS data using modularized functions.
    Returns a list of structured records.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        extracted_data = []

        if isinstance(data, list):  # JSON array (multiple entries)
            for entry in data:
                extracted_data.extend(process_json_entry(entry, file_path))
        else:  # Single JSON object
            extracted_data.extend(process_json_entry(data, file_path))

        return extracted_data

    except Exception as e:
        logging.warning(f"Failed to process {file_path}: {e}")
        return []


def scan_directory_for_coordinates(root_dir):
    """
    Recursively scans a directory for JSON files and extracts location data.
    """
    all_coordinates = []

    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".json"):  # Only process JSON files
                file_path = os.path.join(root, file)
                logging.info(f"Processing {file_path}")

                extracted_data = extract_coordinates_from_json(file_path)
                all_coordinates.extend(extracted_data)

    return all_coordinates


def main():
    """
    Reads a directory path as an argument, scans it for JSON files,
    extracts latitude/longitude/timestamp, and writes results to stdout in JSON format.
    """
    if len(sys.argv) != 2:
        sys.stderr.write("Usage: python3 extract_coordinates.py <directory>\n")
        sys.exit(1)

    root_dir = sys.argv[1]

    if not os.path.isdir(root_dir):
        sys.stderr.write(f"Error: {root_dir} is not a valid directory.\n")
        sys.exit(1)

    logging.info(f"Scanning directory: {root_dir}")

    all_coordinates = scan_directory_for_coordinates(root_dir)

    # Output to stdout as JSON
    json.dump(all_coordinates, sys.stdout, indent=2)


if __name__ == "__main__":
    main()
