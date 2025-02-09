#!/usr/bin/env python3
import json
import sys
from datetime import datetime, timezone
from basepipe import BaseJSONPipe


class LocationPingPipe(BaseJSONPipe):
    """Base class for processing location ping JSON data."""

    def convert_to_iso8601(self, timestamp_str):
        """Converts EXIF DateTimeOriginal and Niantic Timestamp formats to ISO 8601."""
        if not timestamp_str:
            return None

        timestamp_str = timestamp_str.strip().replace(
            " UTC", ""
        )  # Remove UTC suffix before parsing

        try:
            # EXIF format: YYYY:MM:DD HH:MM:SS
            if len(timestamp_str) == 19 and timestamp_str[4] == ":":
                return datetime.strptime(timestamp_str, "%Y:%m:%d %H:%M:%S").isoformat()

            # Niantic format: YYYY-MM-DD HH:MM:SS.SSS
            return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f").isoformat()

        except ValueError:
            self.log(f"Invalid timestamp format: {timestamp_str}")
            return None

    def convert_to_human_readable_time(self, timestamp_iso):
        """Converts ISO 8601 timestamp to human-readable format."""
        if not timestamp_iso:
            return "Unknown Time"
        try:
            dt = datetime.fromisoformat(timestamp_iso.replace("Z", ""))
            return dt.strftime("%B %d, %Y at %-I:%M %p UTC")
        except ValueError:
            self.log(f"Invalid ISO timestamp format: {timestamp_iso}")
            return "Unknown Time"

    def process_entry(self, entry):
        raise NotImplementedError("Subclasses must implement process_entry.")


class ExifLocationPingPipe(LocationPingPipe):
    """Processes EXIF-based location ping JSON data."""

    def dms_to_decimal(self, dms, ref):
        """Converts degrees, minutes, seconds format to decimal degrees."""
        degrees, minutes, seconds = dms
        decimal = degrees + (minutes / 60.0) + (seconds / 3600.0)
        if ref in ["S", "W"]:
            decimal = -decimal
        return round(decimal, 6)

    def process_entry(self, entry):
        try:
            timestamp_str = entry.get("EXIF DateTimeOriginal") or entry.get(
                "Image DateTime"
            )
            timestamp_iso = self.convert_to_iso8601(timestamp_str)
            timestamp_human = self.convert_to_human_readable_time(timestamp_iso)

            latitude = self.dms_to_decimal(
                eval(entry.get("GPS GPSLatitude", "[0,0,0]")),
                entry.get("GPS GPSLatitudeRef", "N"),
            )
            longitude = self.dms_to_decimal(
                eval(entry.get("GPS GPSLongitude", "[0,0,0]")),
                entry.get("GPS GPSLongitudeRef", "E"),
            )

            if latitude == 0 and longitude == 0:
                self.log("Skipping record with invalid location (0,0)")
                return None

            direction_raw = entry.get("GPS GPSImgDirection", "0")
            direction = (
                round(eval(direction_raw))
                if "/" in direction_raw
                else int(direction_raw)
            )
            cardinal_directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
            cardinal = cardinal_directions[round(direction / 45) % 8]

            lens_make = entry.get("EXIF LensMake", "Unknown LensMake")
            lens_model = entry.get("EXIF LensModel", "Unknown LensModel")
            orientation = entry.get("Image Orientation", "Unknown Orientation")
            detail = f"Picture taken at ({latitude}, {longitude}) at {timestamp_human} facing {cardinal} with {lens_make} {lens_model}, Orientation: {orientation}."

            return {
                "timestamp": timestamp_iso,
                "latitude": latitude,
                "longitude": longitude,
                "author": "User",
                "detail": detail,
                "metadata": {
                    "processedBy": "ExifLocationPingPipe_v1",
                    "processingTimestamp": datetime.now(timezone.utc).isoformat(),
                },
            }
        except Exception as e:
            self.log(f"Error processing entry: {e}")
            return None


class PokemonGoLocationPingPipe(LocationPingPipe):
    """Processes Pokémon GO Pokéstop spin location data."""

    def process_entry(self, entry):
        try:
            timestamp_str = entry.get("Timestamp")
            timestamp_iso = (
                self.convert_to_iso8601(timestamp_str) if timestamp_str else None
            )
            timestamp_human = self.convert_to_human_readable_time(timestamp_iso)

            latitude = float(entry.get("Player_Latitude", 0))
            longitude = float(entry.get("Player_Longitude", 0))

            if latitude == 0 and longitude == 0:
                self.log("Skipping record with invalid location (0,0)")
                return None

            detail = f"Pokemon GO interaction at ({latitude}, {longitude}) on {timestamp_human}."

            return {
                "timestamp": timestamp_iso,
                "latitude": latitude,
                "longitude": longitude,
                "author": "User",
                "detail": detail,
                "metadata": {
                    "processedBy": "PokemonGoLocationPingPipe_v1",
                    "processingTimestamp": datetime.now(timezone.utc).isoformat(),
                },
            }
        except Exception as e:
            self.log(f"Error processing entry: {e}")
            return None


class LyftGPSPingPipe(BaseJSONPipe):
    """Processes Lyft ride records into three separate GPS events."""

    def convert_timestamp(self, ts):
        """Converts a timestamp string to ISO 8601 format."""
        return (
            datetime.strptime(ts.replace(" UTC", ""), "%Y-%m-%d %H:%M:%S").isoformat()
            if ts
            else None
        )

    def convert_to_human_readable_time(self, timestamp_iso):
        """Converts ISO 8601 timestamp to human-readable format."""
        if not timestamp_iso:
            return "Unknown Time"
        try:
            dt = datetime.fromisoformat(timestamp_iso.replace("Z", ""))
            return dt.strftime("%B %d, %Y at %-I:%M %p UTC")
        except ValueError:
            self.log(f"Invalid ISO timestamp format: {timestamp_iso}")
            return "Unknown Time"

    def extract_city_state(self, address):
        """Attempts to extract the city and state or city and country from an address."""
        if not address:
            return "Unknown Location"
        parts = address.split(", ")
        if len(parts) >= 2:
            return ", ".join(
                parts[-2:]
            )  # Get last two parts (e.g., City, State or City, Country)
        return address  # Fallback to full address if parsing fails

    def process_entry(self, entry):
        """Processes a single Lyft ride record into structured GPS events."""
        events = []

        # Create event for ride request
        if entry.get("requested_timestamp"):
            timestamp_iso = self.convert_timestamp(entry["requested_timestamp"])
            events.append(
                {
                    "timestamp": timestamp_iso,
                    "latitude": entry["requested_lat"],
                    "longitude": entry["requested_lng"],
                    "detail": f"Rideshare interaction on {self.convert_to_human_readable_time(timestamp_iso)}",
                    "author": "User",
                }
            )

        # Create event for pickup
        if entry.get("pickup_timestamp"):
            timestamp_iso = self.convert_timestamp(entry["pickup_timestamp"])
            location = self.extract_city_state(entry.get("pickup_address"))
            events.append(
                {
                    "timestamp": timestamp_iso,
                    "latitude": entry["pickup_lat"],
                    "longitude": entry["pickup_lng"],
                    "detail": f"Rideshare interaction on {self.convert_to_human_readable_time(timestamp_iso)} in {location}",
                    "author": "User",
                }
            )

        # Create event for dropoff
        if entry.get("dropoff_timestamp"):
            timestamp_iso = self.convert_timestamp(entry["dropoff_timestamp"])
            location = self.extract_city_state(entry.get("destination_address"))
            events.append(
                {
                    "timestamp": timestamp_iso,
                    "latitude": entry["dropoff_lat"],
                    "longitude": entry["dropoff_lng"],
                    "detail": f"Rideshare interaction on {self.convert_to_human_readable_time(timestamp_iso)} in {location}",
                    "author": "User",
                }
            )

        return events


PARSERS = {
    "exif": ExifLocationPingPipe,
    "pogo": PokemonGoLocationPingPipe,
    "lyft": LyftGPSPingPipe,
}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: purpose_gps_ping.py <exif|pogo>\n")
        sys.exit(1)

    parser_type = sys.argv[1].lower()
    parser_class = PARSERS.get(parser_type)

    if not parser_class:
        sys.stderr.write(f"Invalid parser type: {parser_type}\n")
        sys.exit(1)

    parser_class().run()
