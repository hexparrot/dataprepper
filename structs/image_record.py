import os
import json
import logging
from structs.base_record import BaseRecord
from xform.exif_parser import ExifParser  # Directly use ExifParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class ImageRecord(BaseRecord):
    """
    Handles directory-based EXIF metadata extraction from images.
    Calls ExifParser directly and saves structured JSON output.
    """

    def __init__(self, input_dir: str, output_dir: str):
        """
        :param input_dir: Directory containing image files.
        :param output_dir: Directory where JSON files will be saved.
        """
        super().__init__()
        self.input_dir = os.path.abspath(input_dir)
        self.output_root = os.path.abspath(output_dir)
        os.makedirs(self.output_root, exist_ok=True)

    def parse(self, file_path):
        """
        Calls ExifParser to extract metadata and structures the data.
        :param file_path: Path to the image file.
        """
        if not os.path.isfile(file_path):
            logging.warning(f"File not found: {file_path}")
            return None

        try:
            # Read image binary
            with open(file_path, "rb") as f:
                image_data = f.read()

            # Directly call ExifParser
            parser = ExifParser()
            exif_data = parser._extract_records(image_data)  # Get structured EXIF data

            if not exif_data:
                logging.warning(f"No valid EXIF data found in {file_path}")
                return None

            # Set structured fields
            self.set_field(
                "author", "unspecified"
            )  # EXIF does not usually contain author info
            self.set_field("timestamp", exif_data.get("timestamp", ""))
            self.set_field("detail", exif_data.get("detail", ""))
            self.set_field("filepath", file_path)

            # Include all raw EXIF data as well
            for key, value in exif_data.items():
                self.set_field(key, value)

            logging.info(f"Structured EXIF data for {file_path} stored successfully.")

        except Exception as e:
            logging.error(f"Failed to process {file_path}: {e}")

    def process_directory(self):
        """
        Traverses directory and converts all image files to structured JSON.
        """
        logging.info(f"Processing image files in directory: {self.input_dir}")

        file_count = 0
        for root, _, files in os.walk(self.input_dir):
            for file in files:
                if file.lower().endswith((".jpg", ".jpeg", ".png", ".tiff")):
                    file_count += 1
                    image_path = os.path.join(root, file)
                    json_path = os.path.join(
                        self.output_root, os.path.splitext(file)[0] + ".json"
                    )
                    self.process_image(image_path, json_path)

        if file_count == 0:
            logging.warning(f"No image files found in {self.input_dir}")
        else:
            logging.info(f"Successfully processed {file_count} image files.")

    def process_image(self, image_path, json_path):
        """
        Extracts EXIF data from a single image and saves as JSON.
        """
        self.parse(image_path)

        if self.is_valid:
            try:
                os.makedirs(os.path.dirname(json_path), exist_ok=True)
                with open(json_path, "w", encoding="utf-8") as json_file:
                    json.dump(self._fields, json_file, indent=4)
                logging.info(f"Saved JSON output: {json_path}")
            except Exception as e:
                logging.error(f"Failed to save JSON file {json_path}: {e}")
        else:
            logging.warning(f"Skipping invalid record for {image_path}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python structs/image_record.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    image_record = ImageRecord(input_dir, output_dir)
    image_record.process_directory()
