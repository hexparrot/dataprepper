#!/usr/bin/env python3
# process_images.py
import os
import sys
import json
import exifread
import hashlib
from datetime import datetime


def calculate_checksum(file_path):
    """Compute SHA-256 checksum for unique mapping."""
    hash_func = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def convert_gps_to_decimal(gps_values):
    """Convert GPS coordinates from [degrees, minutes, seconds/fraction] to decimal format."""
    try:
        degrees = float(gps_values[0])
        minutes = float(gps_values[1]) / 60
        seconds = (
            float(gps_values[2].num) / float(gps_values[2].den) / 3600
            if hasattr(gps_values[2], "num")
            else float(gps_values[2]) / 3600
        )
        return degrees + minutes + seconds
    except Exception:
        return None


def extract_exif(file_path):
    """Extract key EXIF metadata from an image."""
    metadata = {}
    with open(file_path, "rb") as img_file:
        tags = exifread.process_file(img_file, details=True)

        if not tags:
            print(f"WARNING: No EXIF data found for {file_path}")
            return None

        metadata["FilePath"] = file_path
        metadata["Checksum"] = calculate_checksum(file_path)
        metadata["Make"] = str(tags.get("Image Make", "Unknown"))
        metadata["Model"] = str(tags.get("Image Model", "Unknown"))
        metadata["Orientation"] = str(tags.get("Image Orientation", "Unknown"))

        # Extract timestamp and ensure correct format
        datetime_str = str(tags.get("EXIF DateTimeOriginal", ""))
        if datetime_str and datetime_str != "Unknown":
            try:
                metadata["DateTime"] = datetime.strptime(
                    datetime_str, "%Y:%m:%d %H:%M:%S"
                )
            except ValueError:
                metadata["DateTime"] = None  # Mark invalid timestamps
        else:
            metadata["DateTime"] = None  # Missing timestamps

        # Convert GPS coordinates to decimal format
        lat_values = tags.get("GPS GPSLatitude", None)
        lon_values = tags.get("GPS GPSLongitude", None)
        if lat_values and lon_values:
            metadata["GPS Latitude"] = convert_gps_to_decimal(lat_values.values)
            metadata["GPS Longitude"] = convert_gps_to_decimal(lon_values.values)
        else:
            metadata["GPS Latitude"] = None
            metadata["GPS Longitude"] = None

        return metadata


def process_directory(directory):
    """Process all images in a directory and output metadata to stdout."""
    all_metadata = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith((".jpg", ".jpeg")):
                image_path = os.path.join(root, file)
                metadata = extract_exif(image_path)
                if metadata:
                    all_metadata.append(metadata)

    if not all_metadata:
        print("ERROR: No EXIF data extracted from any files!")

    print(json.dumps(all_metadata, indent=4, default=str))
    return all_metadata


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python process_images.py <image_directory>")
        sys.exit(1)

    directory = sys.argv[1]
    process_directory(directory)
