#!/usr/bin/env python3
import sys
import csv
import json
from datetime import datetime
from xform.base_parser import BaseParser


class NetflixGamesParser(BaseParser):
    """
    Parser for game play activity logs from CSV exports.
    Extracts structured fields including profile name, start time, duration, game title, platform, and device type.
    """

    def _extract_records(self, csv_content: str) -> list[dict]:
        """
        Extracts game play session records from Netflix CSV.
        """
        records = []
        reader = csv.DictReader(csv_content.splitlines())

        for row in reader:
            try:
                # Ensure all fields are treated as strings before calling .strip()
                profile_name = str(row.get("Profile Name", "Unknown Profile")).strip()
                start_time = str(row.get("Start Time", "1970-01-01T00:00:00")).strip()
                duration = str(row.get("Duration", "00:00:00")).strip()
                game_title = str(row.get("Game Title", "Unknown Game")).strip()
                game_version = str(row.get("Game Version", "Unknown Version")).strip()
                platform = str(row.get("Platform", "Unknown Platform")).strip()
                device_type = str(row.get("Device Type", "Unknown Device")).strip()
                country = str(row.get("Country", "Unknown Country")).strip()
                esn = str(row.get("Esn", "Unknown ESN")).strip()
                ip = str(row.get("Ip", "0.0.0.0")).strip()

                # Format timestamp correctly
                timestamp = self._format_timestamp(start_time)

                records.append(
                    {
                        "profile": profile_name,
                        "timestamp": timestamp,
                        "duration": duration,
                        "game_title": game_title,
                        "game_version": game_version,
                        "platform": platform,
                        "device_type": device_type,
                        "country": country,
                        "esn": esn,
                        "ip": ip,
                        "author": profile_name,
                        "message": f"Playing {game_title} on {platform}",
                        "product": "Netflix Games",
                    }
                )

            except Exception as e:
                print(f"[ERROR] Error parsing entry: {e} (Row: {row})", file=sys.stderr)
                continue

        return records

    def _format_timestamp(self, raw_timestamp: str) -> str:
        """
        Convert raw timestamp to ISO 8601 format.
        If already in ISO format, return as is.
        """
        try:
            # If it's already ISO 8601 format, return it directly
            if "T" in raw_timestamp:
                return raw_timestamp

            # Otherwise, parse and convert space-separated format
            return datetime.strptime(raw_timestamp, "%Y-%m-%d %H:%M:%S").isoformat()

        except ValueError:
            # print(f"[DEBUG] Failed to parse timestamp: {raw_timestamp}", file=sys.stderr)
            return "1970-01-01T00:00:00"  # Fallback value


def main():
    """Reads CSV from stdin and outputs structured JSON."""
    csv_content = sys.stdin.read()
    parser = GamePlayParser()
    parsed_data = parser.parse(csv_content)
    print(json.dumps(parsed_data, indent=4))


if __name__ == "__main__":
    main()
