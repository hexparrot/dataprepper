#!/usr/bin/env python3
import sys
import json
import exifread
import logging
import io
import os
from datetime import datetime

# Ensure Python finds the project modules no matter where the script is run
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
from xform.base_parser import BaseParser  # Inherit from BaseParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class ExifParser(BaseParser):
    """
    Extracts EXIF metadata from an image received via stdin.
    Outputs structured JSON to stdout.
    """

    def _extract_records(self, raw_content):
        """
        Extracts EXIF metadata from raw binary image data.
        :param raw_content: Binary image data.
        :return: Dictionary containing structured EXIF metadata.
        """
        # Convert bytes to a file-like object
        image_file = io.BytesIO(raw_content)

        tags = exifread.process_file(image_file, details=False)

        exif_data = {}

        # Store all EXIF tags
        for tag_name, tag_value in tags.items():
            exif_data[tag_name] = str(tag_value)

        # Extract camera make/model
        make_tag = str(tags.get("Image Make", "UnknownMake"))
        model_tag = str(tags.get("Image Model", "UnknownModel"))

        # Extract timestamp from EXIF metadata
        exif_dt = tags.get("EXIF DateTimeOriginal", None)
        iso_timestamp = ""
        if exif_dt:
            dt_str = str(exif_dt)
            try:
                dt_obj = datetime.strptime(dt_str, "%Y:%m:%d %H:%M:%S")
                iso_timestamp = dt_obj.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                logging.warning(f"Failed to parse EXIF timestamp: {dt_str}")

        # Add structured fields
        exif_data["timestamp"] = iso_timestamp
        exif_data[
            "detail"
        ] = f"Picture taken by {make_tag} {model_tag} at {iso_timestamp}"

        return exif_data


if __name__ == "__main__":
    """
    Command-line interface for ExifParser.
    Reads image binary from stdin and outputs JSON to stdout.
    """
    parser = ExifParser()

    # Read binary image data from stdin
    with sys.stdin.buffer as f:
        image_data = f.read()

    exif_data = parser._extract_records(image_data)

    # Print JSON output to stdout
    print(json.dumps(exif_data, indent=2))
