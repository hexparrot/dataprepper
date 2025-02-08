#!/usr/bin/env python3

import json
import sys
from datetime import datetime, timezone
from basepipe import BaseJSONPipe


class MediaHistoryPipe(BaseJSONPipe):
    """Base class for processing media watch/listening history."""

    def convert_to_iso8601(self, timestamp_str):
        """Converts a timestamp string to ISO 8601 format, handling different timestamp formats."""
        try:
            if "T" in timestamp_str:
                return (
                    datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S").isoformat()
                    + "Z"
                )
            return (
                datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").isoformat() + "Z"
            )
        except ValueError:
            try:
                return (
                    datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M").isoformat() + "Z"
                )
            except ValueError:
                self.log(f"Invalid timestamp format: {timestamp_str}")
                return None
        """Converts a timestamp string to ISO 8601 format, handling different timestamp formats."""
        try:
            # Try parsing full datetime format first
            return (
                datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").isoformat() + "Z"
            )
        except ValueError:
            try:
                # If full datetime parsing fails, try parsing without seconds
                return (
                    datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M").isoformat() + "Z"
                )
            except ValueError:
                self.log(f"Invalid timestamp format: {timestamp_str}")
                return None
        """Converts a timestamp string to ISO 8601 format."""
        try:
            return (
                datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S").isoformat() + "Z"
            )
        except ValueError:
            self.log(f"Invalid timestamp format: {timestamp_str}")
            return None

    def convert_to_human_readable_time(self, timestamp_iso):
        """Converts ISO 8601 timestamp to human-readable format."""
        try:
            dt = datetime.strptime(timestamp_iso, "%Y-%m-%dT%H:%M:%SZ")
            return dt.strftime("%B %d, %Y at %-I:%M %p UTC")
        except ValueError:
            return timestamp_iso


class NetflixWatchHistoryPipe(MediaHistoryPipe):
    """Processes Netflix watch history JSON data."""

    def process_entry(self, entry):
        try:
            title = entry.get("Title")
            if not title:
                return None

            timestamp_str = entry.get("Start Time")
            timestamp_iso = (
                self.convert_to_iso8601(timestamp_str) if timestamp_str else None
            )
            timestamp_human = self.convert_to_human_readable_time(timestamp_iso)

            duration_str = entry.get("Duration")
            duration_seconds = (
                int(duration_str.split(":")[0]) * 3600
                + int(duration_str.split(":")[1]) * 60
                + int(duration_str.split(":")[2])
                if duration_str
                else None
            )

            author = entry.get("author", "User")
            detail = (
                f"I played '{title}' on Netflix on {timestamp_human}."
                if timestamp_human
                else None
            )

            return {
                k: v
                for k, v in {
                    "timestamp": timestamp_iso
                    or datetime.now(timezone.utc).isoformat(),
                    "title": title,
                    "durationSeconds": duration_seconds,
                    "author": author,
                    "detail": detail,
                    "metadata": {
                        "processedBy": "NetflixWatchHistoryPipe_v1",
                        "processingTimestamp": datetime.now(timezone.utc).isoformat(),
                    },
                }.items()
                if v is not None
            }
        except Exception as e:
            self.log(f"Error processing entry: {e}")
            return None


class YouTubeWatchHistoryPipe(MediaHistoryPipe):
    """Processes YouTube watch history JSON data."""

    def process_entry(self, entry):
        try:
            title = entry.get("title")
            if not title:
                return None

            timestamp_str = entry.get("timestamp")
            timestamp_iso = (
                self.convert_to_iso8601(timestamp_str) if timestamp_str else None
            )
            timestamp_human = self.convert_to_human_readable_time(timestamp_iso)

            author = entry.get("author", "User")
            detail = (
                f"I watched '{title}' on YouTube on {timestamp_human}."
                if timestamp_human
                else None
            )

            return {
                k: v
                for k, v in {
                    "timestamp": timestamp_iso
                    or datetime.now(timezone.utc).isoformat(),
                    "title": title,
                    "url": entry.get("url"),
                    "author": author,
                    "detail": detail,
                    "metadata": {
                        "processedBy": "YouTubeWatchHistoryPipe_v1",
                        "processingTimestamp": datetime.now(timezone.utc).isoformat(),
                    },
                }.items()
                if v is not None
            }
        except Exception as e:
            self.log(f"Error processing entry: {e}")
            return None


class SpotifyListeningHistoryPipe(MediaHistoryPipe):
    """Processes Spotify listening history JSON data."""

    def process_entry(self, entry):
        try:
            track_name = entry.get("trackName")
            if not track_name:
                return None

            timestamp_str = entry.get("endTime")
            timestamp_iso = (
                self.convert_to_iso8601(timestamp_str) if timestamp_str else None
            )
            timestamp_human = self.convert_to_human_readable_time(timestamp_iso)

            duration_seconds = entry.get("msPlayed", 0) // 1000
            author = entry.get("author", "User")
            detail = (
                f"I listened to '{track_name}' by {entry.get('artistName', 'Unknown')} on Spotify on {timestamp_human}."
                if timestamp_human
                else None
            )

            return {
                k: v
                for k, v in {
                    "timestamp": timestamp_iso
                    or datetime.now(timezone.utc).isoformat(),
                    "trackName": track_name,
                    "artistName": entry.get("artistName"),
                    "durationSeconds": duration_seconds,
                    "author": author,
                    "detail": detail,
                    "metadata": {
                        "processedBy": "SpotifyListeningHistoryPipe_v1",
                        "processingTimestamp": datetime.now(timezone.utc).isoformat(),
                    },
                }.items()
                if v is not None
            }
        except Exception as e:
            self.log(f"Error processing entry: {e}")
            return None


PARSERS = {
    "netflix": NetflixWatchHistoryPipe,
    "youtube": YouTubeWatchHistoryPipe,
    "spotify": SpotifyListeningHistoryPipe,
}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: python script.py <parser_name>\n")
        sys.exit(1)

    parser_name = sys.argv[1].lower()
    parser_class = PARSERS.get(parser_name)

    if not parser_class:
        sys.stderr.write(f"Invalid parser name: {parser_name}\n")
        sys.exit(1)

    parser_class().run()
