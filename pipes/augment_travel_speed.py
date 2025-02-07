#!/usr/bin/env python3
import sys
import json
import logging
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2
from basepipe import BaseJSONPipe


class AugmentTravelSpeedPipe(BaseJSONPipe):
    """Pipe that calculates and appends transit speed (MPH) between GPS records."""

    def __init__(self, verbose=True):
        super().__init__(verbose)
        self.last_record = None  # Stores the last GPS record for speed calculation

    @staticmethod
    def _haversine_distance(lat1, lon1, lat2, lon2):
        """
        Computes the great-circle distance (in miles) between two lat/lon points.
        Uses the Haversine formula.
        """
        R = 3958.8  # Radius of Earth in miles
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        return R * c  # Distance in miles

    def process_entry(self, entry):
        """
        Processes an entry, calculates transit speed if applicable.
        """
        try:
            lat = float(entry["latitude"])
            lon = float(entry["longitude"])
            timestamp = datetime.fromisoformat(entry["timestamp"])

            if self.last_record:
                last_lat = float(self.last_record["latitude"])
                last_lon = float(self.last_record["longitude"])
                last_timestamp = datetime.fromisoformat(self.last_record["timestamp"])

                # Compute distance and time difference
                distance = self._haversine_distance(last_lat, last_lon, lat, lon)
                time_diff_hours = (timestamp - last_timestamp).total_seconds() / 3600

                # Calculate speed (MPH) and add to record
                speed_mph = (
                    round(distance / time_diff_hours, 2) if time_diff_hours > 0 else 0
                )
                entry["speed_mph"] = speed_mph

            else:
                entry["speed_mph"] = None  # No speed for first entry

            self.last_record = (
                entry  # Store current entry as last_record for next iteration
            )
            return entry

        except (ValueError, KeyError) as e:
            logging.error(f"Error processing entry: {entry}, {e}")
            return entry  # Return unmodified if there's an error


if __name__ == "__main__":
    parser = AugmentTravelSpeedPipe()
    parser.run()
