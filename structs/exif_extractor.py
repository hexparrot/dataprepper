#!/usr/bin/env python3

import os
import json
import logging
from datetime import datetime
import exifread  # pip install exifread

from structs.base_record import BaseRecord  # Adjust import as needed

# Configure logging for debugging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class ExifRecord(BaseRecord):
    """
    A record that represents EXIF metadata from a JPEG file.
    Stores *all* EXIF tags intact in the final record,
    plus the required fields (author, detail, timestamp, filepath).
    """

    def __init__(self, base_image_dir):
        """
        :param base_image_dir: The root directory for image files (userdata/raw/images).
                               This is used to generate relative file paths.
        """
        super().__init__()
        self.base_image_dir = (
            base_image_dir  # Store base directory for relative path calculations
        )

    def parse(self, file_path):
        """
        Extract EXIF data from the given file_path, then populate
        self._fields with *all* EXIF tags. Also set the required
        fields:
          - author: Default "unspecified"
          - timestamp: From EXIF DateTimeOriginal (ISO8601 format)
          - detail: "Picture taken by <make> <model> at <timestamp>"
          - filepath: Relative path of the image from `userdata/raw/images/`
        """
        if not os.path.isfile(file_path):
            logging.warning(f"File not found: {file_path}")
            return None

        # Clear previous file data to prevent carryover issues
        self._fields = {}

        with open(file_path, "rb") as f:
            tags = exifread.process_file(f, details=False)

        # Store every EXIF tag intact:
        for tag_name, tag_value in tags.items():
            self.set_field(tag_name, str(tag_value))

        # Attempt to get "Make" and "Model" from EXIF if available
        make_tag = str(tags.get("Image Make", "UnknownMake"))
        model_tag = str(tags.get("Image Model", "UnknownModel"))

        # Attempt to parse the date/time the picture was taken
        exif_dt = tags.get("EXIF DateTimeOriginal", None)
        iso_timestamp = ""
        if exif_dt:
            dt_str = str(exif_dt)
            try:
                dt_obj = datetime.strptime(dt_str, "%Y:%m:%d %H:%M:%S")
                iso_timestamp = dt_obj.strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                logging.warning(f"Failed to parse EXIF timestamp: {dt_str}")

        # Compute the relative path from `userdata/raw/images`
        relative_path = os.path.relpath(file_path, self.base_image_dir)

        # Set required fields, ensuring each file gets unique values
        self.set_field("author", "unspecified")
        self.set_field("timestamp", iso_timestamp)
        self.set_field(
            "detail", f"Picture taken by {make_tag} {model_tag} at {iso_timestamp}"
        )
        self.set_field("filepath", relative_path)  # Store relative file path

        logging.info(
            f"Processed {file_path} - Timestamp: {iso_timestamp}, Make: {make_tag}, Model: {model_tag}"
        )

        return self._fields  # Return extracted data as a dictionary for external use
