#!/usr/bin/env python3
import sys
import json
import numpy as np
from datetime import datetime
from collections import defaultdict
from scipy.spatial.distance import pdist
from scipy.cluster.hierarchy import fcluster, linkage


def parse_datetime(entry):
    """Convert DateTime string back to datetime object if valid."""
    if isinstance(entry.get("DateTime"), str):
        try:
            entry["DateTime"] = datetime.strptime(
                entry["DateTime"], "%Y-%m-%d %H:%M:%S"
            )
        except ValueError:
            entry["DateTime"] = None  # If invalid, keep as None
    return entry


def analyze_camera_usage(metadata):
    """Analyze camera usage and enumerate spans of time each device was active."""
    camera_timeline = defaultdict(list)

    for entry in metadata:
        if entry["DateTime"]:
            year_month = entry["DateTime"].strftime("%Y-%m")
            camera_timeline[f"{entry['Make']} {entry['Model']}"].append(year_month)

    serial_list = {}
    last_seen = {}
    for device, dates in camera_timeline.items():
        unique_months = sorted(set(dates))
        serial_list[device] = {month: dates.count(month) for month in unique_months}
        last_seen[device] = unique_months[-1]

    return serial_list, last_seen


def analyze_gps_coverage(metadata):
    """Analyze GPS availability across devices."""
    gps_data = defaultdict(lambda: {"total": 0, "gps_available": 0})

    for entry in metadata:
        if entry["DateTime"]:
            key = f"{entry['Make']} {entry['Model']}"
            gps_data[key]["total"] += 1
            if entry["GPS Latitude"] is not None and entry["GPS Longitude"] is not None:
                gps_data[key]["gps_available"] += 1

    return {
        k: v["gps_available"] / v["total"] if v["total"] else 0
        for k, v in gps_data.items()
    }


def cluster_gps_locations(metadata, distance_threshold_km=5):
    """Cluster photos based on geographic proximity."""
    gps_points = []
    gps_entries = []

    for entry in metadata:
        if entry["GPS Latitude"] is not None and entry["GPS Longitude"] is not None:
            gps_points.append((entry["GPS Latitude"], entry["GPS Longitude"]))
            gps_entries.append(entry)

    if not gps_points:
        return []

    gps_points = np.array(gps_points)
    dist_matrix = pdist(gps_points, metric="euclidean") * 111  # Keep condensed form
    clusters = fcluster(
        linkage(dist_matrix, method="ward"), distance_threshold_km, criterion="distance"
    )

    clustered_results = defaultdict(list)
    for idx, cluster_id in enumerate(clusters):
        clustered_results[str(cluster_id)].append(
            gps_entries[idx]
        )  # Convert int32 to string

    return clustered_results


if __name__ == "__main__":
    metadata = json.load(sys.stdin)  # Read from stdin

    # Convert DateTime strings to datetime objects
    metadata = [parse_datetime(entry) for entry in metadata]

    serial_list, last_seen = analyze_camera_usage(metadata)
    gps_coverage = analyze_gps_coverage(metadata)
    location_clusters = cluster_gps_locations(metadata)

    output = {
        "Camera Usage": serial_list,
        "Last Seen Per Device": last_seen,
        "GPS Coverage": gps_coverage,
        "Location Clusters": location_clusters,
    }

    print(json.dumps(output, indent=4, default=str))
