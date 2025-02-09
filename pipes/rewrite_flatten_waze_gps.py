#!/usr/bin/env python3
import sys
import logging
from datetime import datetime, timezone
from basepipe import BaseJSONPipe

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class WazeGPSPipe(BaseJSONPipe):
    """
    Processes Waze GPS pings from JSON input and outputs structured records.

    - Extracts GPS timestamps and lat/long coordinates.
    - Converts timestamps into ISO 8601 format.
    - Handles older formats with missing timestamps by using the trip date (if valid).
    - Splits latitude and longitude into distinct fields.
    - Skips records with invalid timestamps.
    - Reduces excessive logging by summarizing skipped records.
    """

    def process_entry(self, entry):
        """
        Processes an individual JSON record containing GPS coordinates.

        :param entry: A JSON dictionary with 'Date' and 'Coordinates'.
        :return: List of transformed JSON records (empty list if no valid data).
        """
        trip_date = entry.get("Date", "").strip()
        coordinates_data = entry.get("Coordinates", "").strip()

        if not coordinates_data:
            return []  # Ensure we return an empty list, NOT None

        gps_entries = coordinates_data.split("|")  # Split multiple GPS points
        transformed_records = []
        skipped_entries = []  # Store skipped entries for logging

        # **Detect if timestamps exist in the data**
        has_timestamps = any(":" in gps or "+" in gps for gps in gps_entries)

        for gps_entry in gps_entries:
            try:
                if has_timestamps:
                    # **Newer format: Has timestamp-latlong pairs**
                    ts_part, latlong_part = gps_entry.split("(")
                    latlong = latlong_part.rstrip(")")  # Remove closing `)`

                    # Convert timestamp to ISO 8601 format
                    iso_timestamp = self._convert_to_iso8601(ts_part.strip(), gps_entry)

                else:
                    # **Older format: No timestamps, only lat/long**
                    iso_timestamp = self._convert_to_iso8601(trip_date, gps_entry)
                    latlong = gps_entry.strip()  # The whole entry is just lat/long

                # **Skip this record if the timestamp is invalid**
                if not iso_timestamp:
                    skipped_entries.append(gps_entry)  # Store skipped entry
                    continue

                # Split lat/long into separate fields
                lon, lat = map(str.strip, latlong.split())

                transformed_records.append(
                    {
                        "author": "User",
                        "detail": f"GPS ping via Waze for trip on {trip_date}",
                        "timestamp": iso_timestamp,
                        "latitude": lat,
                        "longitude": lon,
                        "product": "Waze",
                        "metadata": {
                            "processedBy": "LyftGPSPingPipe_v1",
                            "processingTimestamp": datetime.now(
                                timezone.utc
                            ).isoformat(),
                        },
                    }
                )

                # **For old format, only process the first lat/long pair**
                if not has_timestamps:
                    break  # Stop after processing one entry

            except ValueError:
                skipped_entries.append(gps_entry)  # Store malformed entry

        # **Log skipped entries in one message instead of multiple**
        if skipped_entries:
            logging.warning(
                f"Skipping {len(skipped_entries)} timestamp-less GPS records."
            )

        return transformed_records  # Always return a list (empty if no valid data)

    def _convert_to_iso8601(self, raw_timestamp, raw_entry):
        """
        Converts a timestamp into ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).

        :param raw_timestamp: A timestamp string (potentially with timezone offsets).
        :param raw_entry: The full raw entry (used for logging).
        :return: A properly formatted ISO 8601 string or None if invalid.
        """
        raw_timestamp = raw_timestamp.strip()

        if not raw_timestamp:
            return None  # **Return None to indicate an invalid timestamp**

        try:
            # Handle timestamps with offsets (e.g., "2015-02-05 00:58:35+00")
            dt_obj = datetime.strptime(
                raw_timestamp.split("+")[0].strip(), "%Y-%m-%d %H:%M:%S"
            )
            return dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO 8601 format

        except ValueError:
            return None  # **Return None to indicate an invalid timestamp**

    def run(self):
        """
        Reads JSON from stdin, processes each entry, and writes structured JSON to stdout.
        Filters out empty records before writing.
        """
        entries = self.read_input()
        processed_entries = [
            result
            for entry in entries
            for result in self.process_entry(entry)
            if result
        ]

        self.write_output(processed_entries)


if __name__ == "__main__":
    parser = WazeGPSPipe()
    parser.run()
