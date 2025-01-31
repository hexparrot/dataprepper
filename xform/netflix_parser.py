import sys
import json
import csv
import re
from datetime import datetime
from xform.base_parser import BaseParser


class NetflixParser(BaseParser):
    """
    Parser for Netflix Viewing History CSV exports.
    Extracts profile name, start time, title, duration, device type, and country.
    """

    def _extract_records(self, csv_content: str) -> list[dict]:
        """
        Extract viewing history records from Netflix CSV.
        :param csv_content: Raw CSV content as a string.
        :return: List of dictionaries with structured data.
        """
        records = []
        reader = csv.DictReader(csv_content.splitlines())

        for row in reader:
            try:
                profile_name = row.get("Profile Name").strip()
                raw_start_time = row.get("Start Time").strip()
                title = row.get("Title").strip()
                duration = row.get("Duration").strip()
                device = row.get("Device Type").strip()
                country = row.get("Country").strip()

                # Convert Start Time to ISO 8601 format
                iso_timestamp = self._format_timestamp(raw_start_time)

                # Construct record
                record = {
                    "profile": profile_name,
                    "title": title,
                    "timestamp": iso_timestamp,
                    "duration": duration,
                    "device": device,
                    "country": country,
                    "message": f"Watched {title} on Netflix",
                    "author": profile_name,  # Person who watched
                    "product": "Netflix",
                }

                # Ensure required fields are present
                if not title or not iso_timestamp:
                    print("[DEBUG] Skipping entry due to missing data", file=sys.stderr)
                    continue

                records.append(record)

            except Exception as e:
                # print(f"[ERROR] Error parsing entry: {e}", file=sys.stderr)
                continue

        return records


def _format_timestamp(self, raw_timestamp: str) -> str:
    """
    Convert Netflix timestamp into ISO 8601 format.
    If the timestamp is already formatted, return it directly.
    """
    try:
        # If timestamp matches ISO 8601 pattern, return as is
        if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", raw_timestamp):
            return raw_timestamp  # Already valid

        # Otherwise, attempt to parse and convert
        dt_obj = datetime.strptime(raw_timestamp, "%Y-%m-%d %H:%M:%S")
        return dt_obj.strftime("%Y-%m-%dT%H:%M:%S")  # Standardize format
    except ValueError:
        print(f"[DEBUG] Failed to parse timestamp: {raw_timestamp}", file=sys.stderr)
        return "1970-01-01T00:00:00"  # Fallback for errors


def main():
    """Reads Netflix CSV from stdin and outputs structured JSON."""
    csv_content = sys.stdin.read()
    parser = NetflixParser()
    parsed_data = parser.parse(csv_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
