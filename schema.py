print("Loading data_sources.json...")

import json

with open("data_sources.json", "r") as f:
    data_sources = json.load(f)

print("data_sources.json loaded successfully.")

from ariadne import QueryType, make_executable_schema, gql
from ariadne.asgi import GraphQL
from pymongo import MongoClient
import os
import logging
from datetime import datetime

print("Importing modules done.")

# MongoDB Connection
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/admin"

print("Connecting to MongoDB...")
client = MongoClient(MONGO_URI)
print("MongoDB connection established.")

query = QueryType()

# Load Schema
print("Defining GraphQL schema...")
type_defs = gql(
    """
type Query {
  allTriplets(database: String!): [Triplet]
  countTriplets(database: String!): Int
  availableDatabases: [String]
}

type Triplet {
  timestamp: String
  latitude: Float
  longitude: Float
}

scalar JSON
"""
)

print("GraphQL schema defined.")


@query.field("availableDatabases")
def resolve_available_databases(_, info):
    return list(data_sources.keys())


@query.field("allTriplets")
def resolve_all_triplets(_, info, database):
    """Returns all triplets with timestamp, latitude, and longitude for the specified database."""
    if database not in data_sources:
        raise ValueError(f"Database '{database}' not found in configuration.")

    db = client[database]
    merged_data = []

    # Handle Pokemongo (Multiple Collections, One Triplet per Record)
    if database == "pokemongo":
        for collection, fields in data_sources[database].items():
            logging.info(f"Fetching triplets from {database}.{collection}")

            # Fetch records with dynamically matching fields and sort by timestamp
            records = (
                db[collection]
                .find(
                    {},
                    {
                        field: 1 for field in fields.values()
                    },  # Dynamically select fields
                )
                .sort("timestamp", 1)
            )  # Sort by 'timestamp' in ascending order (1 for ascending)

            for record in records:
                # Ensure all required fields are present in the record
                if all(field in record for field in fields.values()):
                    timestamp_value = record[fields["timestamp"]]
                    if isinstance(timestamp_value, datetime):
                        timestamp_value = timestamp_value.isoformat()

                    merged_data.append(
                        {
                            "timestamp": str(timestamp_value),
                            "latitude": record[fields["latitude"]],
                            "longitude": record[fields["longitude"]],
                        }
                    )

    # Handle Lyft (Single Collection, 3 Triplets per Record)
    elif database == "lyft":
        collection = "passenger_rides"
        logging.info(f"Fetching triplets from {database}.{collection}")

        records = (
            db[collection]
            .find(
                {},
                {
                    "requested_timestamp": 1,
                    "requested_lat": 1,
                    "requested_lng": 1,
                    "pickup_timestamp": 1,
                    "pickup_lat": 1,
                    "pickup_lng": 1,
                    "dropoff_timestamp": 1,
                    "dropoff_lat": 1,
                    "dropoff_lng": 1,
                },
            )
            .sort("requested_timestamp", 1)
        )  # Sort by 'requested_timestamp'

        for record in records:
            # For requested triplet
            if (
                "requested_timestamp" in record
                and "requested_lat" in record
                and "requested_lng" in record
            ):
                requested_timestamp = record["requested_timestamp"]
                requested_lat = record["requested_lat"]
                requested_lng = record["requested_lng"]
                merged_data.append(
                    {
                        "timestamp": requested_timestamp.isoformat()
                        if isinstance(requested_timestamp, datetime)
                        else str(requested_timestamp),
                        "latitude": requested_lat,
                        "longitude": requested_lng,
                    }
                )

            # For pickup triplet
            if (
                "pickup_timestamp" in record
                and "pickup_lat" in record
                and "pickup_lng" in record
            ):
                pickup_timestamp = record["pickup_timestamp"]
                pickup_lat = record["pickup_lat"]
                pickup_lng = record["pickup_lng"]
                merged_data.append(
                    {
                        "timestamp": pickup_timestamp.isoformat()
                        if isinstance(pickup_timestamp, datetime)
                        else str(pickup_timestamp),
                        "latitude": pickup_lat,
                        "longitude": pickup_lng,
                    }
                )

            # For dropoff triplet
            if (
                "dropoff_timestamp" in record
                and "dropoff_lat" in record
                and "dropoff_lng" in record
            ):
                dropoff_timestamp = record["dropoff_timestamp"]
                dropoff_lat = record["dropoff_lat"]
                dropoff_lng = record["dropoff_lng"]
                merged_data.append(
                    {
                        "timestamp": dropoff_timestamp.isoformat()
                        if isinstance(dropoff_timestamp, datetime)
                        else str(dropoff_timestamp),
                        "latitude": dropoff_lat,
                        "longitude": dropoff_lng,
                    }
                )

    # Handle Waze database (single collection, multiple triplets per record)
    elif database == "waze":
        collection = "location_details"
        logging.info(f"Fetching triplets from {database}.{collection}")

        records = (
            db[collection]
            .find(
                {},
                {
                    "Date": 1,  # The date field
                    "Coordinates": 1,  # The coordinates field with multiple points
                },
            )
            .sort("Date", 1)
        )  # Sort by 'Date' field

        # Process each record
        for record in records:
            logging.debug(f"Processing record: {record}")  # Log each record

            # Extract Date and Coordinates
            date = record.get("Date")
            coordinates = record.get("Coordinates", "")

            # Debug: Check if coordinates exist
            if not coordinates:
                logging.warning(f"Record missing 'Coordinates' field: {record}")
                continue

            # Split the coordinates by pipe (`|`) to get each timestamp-location pair
            for coordinate_pair in coordinates.split("|"):
                try:
                    # Each coordinate_pair should be in the format "timestamp(lat, lng)"
                    logging.debug(
                        f"Processing coordinate pair: {coordinate_pair}"
                    )  # Log the pair being processed

                    timestamp_str, lat_lng_str = coordinate_pair.split("(")
                    lat_lng_str = lat_lng_str.rstrip(
                        ")"
                    )  # Remove the closing parenthesis

                    # Split latitude and longitude
                    lng, lat = lat_lng_str.split()

                    # Parse timestamp and coordinates
                    timestamp = timestamp_str.strip()
                    latitude = float(lat)
                    longitude = float(lng)

                    # Check if timestamp is empty
                    if not timestamp:
                        logging.warning(
                            f"Missing timestamp for coordinate pair: {coordinate_pair}"
                        )
                        continue

                    # Create a triplet for each parsed coordinate
                    merged_data.append(
                        {
                            "timestamp": timestamp,
                            "latitude": latitude,
                            "longitude": longitude,
                        }
                    )

                except Exception as e:
                    logging.error(
                        f"Error processing coordinate pair: {coordinate_pair}, Error: {e}"
                    )

    # Return sorted data by timestamp
    return sorted(merged_data, key=lambda x: x["timestamp"])


# @query.field("allTriplets")
# def resolve_all_triplets(_, info, database):
#    # Check if the database exists in the configuration
#    if database not in data_sources:
#        raise ValueError(f"Database '{database}' not found in configuration.")
#
#    db = client[database]
#    merged_data = []
#
#    # Handle Pokemongo (Multiple Collections, One Triplet per Record)
#    if database == "pokemongo":
#        for collection, fields in data_sources[database].items():
#            logging.info(f"Fetching triplets from {database}.{collection}")
#
#            # Fetch records with dynamically matching fields
#            records = db[collection].find(
#                {},
#                {field: 1 for field in fields.values()}  # Dynamically select fields
#            )
#
#            for record in records:
#                # Ensure all required fields are present in the record
#                if all(field in record for field in fields.values()):
#                    timestamp_value = record[fields["timestamp"]]
#                    if isinstance(timestamp_value, datetime):
#                        timestamp_value = timestamp_value.isoformat()
#
#                    merged_data.append({
#                        "timestamp": str(timestamp_value),
#                        "latitude": record[fields["latitude"]],
#                        "longitude": record[fields["longitude"]],
#                    })
#
#    # Handle Lyft (Single Collection, 3 Triplets per Record)
#    elif database == "lyft":
#        collection = "passenger_rides"
#        logging.info(f"Fetching triplets from {database}.{collection}")
#
#        records = db[collection].find(
#            {},
#            {
#                "requested_timestamp": 1,
#                "requested_lat": 1,
#                "requested_lng": 1,
#                "pickup_timestamp": 1,
#                "pickup_lat": 1,
#                "pickup_lng": 1,
#                "dropoff_timestamp": 1,
#                "dropoff_lat": 1,
#                "dropoff_lng": 1,
#            }
#        )
#
#        for record in records:
#            # For requested triplet
#            if "requested_timestamp" in record and "requested_lat" in record and "requested_lng" in record:
#                requested_timestamp = record["requested_timestamp"]
#                requested_lat = record["requested_lat"]
#                requested_lng = record["requested_lng"]
#                merged_data.append({
#                    "timestamp": requested_timestamp.isoformat() if isinstance(requested_timestamp, datetime) else str(requested_timestamp),
#                    "latitude": requested_lat,
#                    "longitude": requested_lng
#                })
#
#            # For pickup triplet
#            if "pickup_timestamp" in record and "pickup_lat" in record and "pickup_lng" in record:
#                pickup_timestamp = record["pickup_timestamp"]
#                pickup_lat = record["pickup_lat"]
#                pickup_lng = record["pickup_lng"]
#                merged_data.append({
#                    "timestamp": pickup_timestamp.isoformat() if isinstance(pickup_timestamp, datetime) else str(pickup_timestamp),
#                    "latitude": pickup_lat,
#                    "longitude": pickup_lng
#                })
#
#            # For dropoff triplet
#            if "dropoff_timestamp" in record and "dropoff_lat" in record and "dropoff_lng" in record:
#                dropoff_timestamp = record["dropoff_timestamp"]
#                dropoff_lat = record["dropoff_lat"]
#                dropoff_lng = record["dropoff_lng"]
#                merged_data.append({
#                    "timestamp": dropoff_timestamp.isoformat() if isinstance(dropoff_timestamp, datetime) else str(dropoff_timestamp),
#                    "latitude": dropoff_lat,
#                    "longitude": dropoff_lng
#                })
#
#    elif database == "waze":
#        collection = "location_details"
#        logging.info(f"Fetching triplets from {database}.{collection}")
#
#        records = db[collection].find(
#            {},
#            {
#                "Date": 1,  # The date field
#                "Coordinates": 1,  # The coordinates field with multiple points
#            }
#        )
#
#        # Check if records are found and print the count
#        record_count = db[collection].count_documents({})
#        logging.debug(f"Found {record_count} records in 'location_details' collection.")
#
#        if record_count == 0:
#            logging.warning(f"No records found in the '{collection}' collection.")
#
#        # Process each record
#        for record in records:
#            logging.debug(f"Processing record: {record}")  # Log each record
#
#            # Extract Date and Coordinates
#            date = record.get("Date")
#            coordinates = record.get("Coordinates", "")
#
#            # Debug: Check if coordinates exist
#            if not coordinates:
#                logging.warning(f"Record missing 'Coordinates' field: {record}")
#                continue
#
#            # Split the coordinates by pipe (`|`) to get each timestamp-location pair
#            for coordinate_pair in coordinates.split("|"):
#                try:
#                    # Each coordinate_pair should be in the format "timestamp(lat, lng)"
#                    logging.debug(f"Processing coordinate pair: {coordinate_pair}")  # Log the pair being processed
#
#                    timestamp_str, lat_lng_str = coordinate_pair.split("(")
#                    lat_lng_str = lat_lng_str.rstrip(")")  # Remove the closing parenthesis
#
#                    # Split latitude and longitude
#                    lat, lng = lat_lng_str.split()
#
#                    # Parse timestamp and coordinates
#                    timestamp = timestamp_str.strip()
#                    latitude = float(lat)
#                    longitude = float(lng)
#
#                    # Check if timestamp is empty
#                    if not timestamp:
#                        logging.warning(f"Missing timestamp for coordinate pair: {coordinate_pair}")
#                        continue
#
#                    # Create a triplet for each parsed coordinate
#                    merged_data.append({
#                        "timestamp": timestamp,
#                        "latitude": latitude,
#                        "longitude": longitude
#                    })
#
#                except Exception as e:
#                    logging.error(f"Error processing coordinate pair: {coordinate_pair}, Error: {e}")
#
#
#
#    else:
#        # Your existing logic for pokemongo or other databases
#        pass
#
#    # Return sorted data by timestamp
#    return sorted(merged_data, key=lambda x: x["timestamp"])


@query.field("countTriplets")
def resolve_count_triplets(_, info, database):
    """Counts the total number of triplets with timestamp, latitude, and longitude in the specified database."""
    if database not in data_sources:
        raise ValueError(f"Database '{database}' not found in configuration.")

    db = client[database]
    total_count = 0

    # Handle Pokemongo and other databases (multiple collections)
    if database == "pokemongo":
        for collection, fields in data_sources[database].items():
            logging.info(
                f"Counting triplets in {database}.{collection} with fields: {fields}"
            )

            count_query = {
                fields["timestamp"]: {"$exists": True},
                fields["latitude"]: {"$exists": True},
                fields["longitude"]: {"$exists": True},
            }

            logging.info(f"Running count query: {count_query}")
            count = db[collection].count_documents(
                count_query
            )  # Use count_documents here
            logging.info(f"Count result for {collection}: {count}")

            total_count += count

    # Handle Lyft database (single collection, 3 triplets per record)
    elif database == "lyft":
        collection = "passenger_rides"
        logging.info(f"Counting triplets in {database}.{collection}")

        count_query = {
            "requested_timestamp": {"$exists": True},
            "requested_lat": {"$exists": True},
            "requested_lng": {"$exists": True},
            "pickup_timestamp": {"$exists": True},
            "pickup_lat": {"$exists": True},
            "pickup_lng": {"$exists": True},
            "dropoff_timestamp": {"$exists": True},
            "dropoff_lat": {"$exists": True},
            "dropoff_lng": {"$exists": True},
        }

        logging.info(f"Running count query: {count_query}")
        count = db[collection].count_documents(count_query)  # Use count_documents here
        logging.info(f"Count result for {collection}: {count}")

        # Each valid ride record will produce 3 triplets (requested, pickup, dropoff)
        total_count += count * 3

    # Handle Waze database (single collection, multiple triplets per record)
    elif database == "waze":
        collection = "location_details"
        logging.info(f"Counting triplets in {database}.{collection}")

        # Count records where the Coordinates field exists
        count_query = {
            "Coordinates": {"$exists": True},  # Ensure Coordinates field exists
        }

        logging.info(f"Running count query: {count_query}")
        count = db[collection].count_documents(
            count_query
        )  # Count records with Coordinates
        logging.info(f"Count result for {collection}: {count}")

        # For each record, count the number of coordinate pairs in the Coordinates field
        for record in db[collection].find(count_query):
            coordinates = record.get("Coordinates", "")
            if coordinates:
                # Split the Coordinates field by pipe (`|`) and count the number of pairs
                triplet_count = len(coordinates.split("|"))
                logging.info(
                    f"Record has {triplet_count} triplets (based on {coordinates})"
                )
                total_count += triplet_count
            else:
                logging.warning(
                    f"Missing 'Coordinates' field in record: {record['_id']}"
                )

    logging.info(f"Total triplet count from {database}: {total_count}")
    return total_count


print("Resolvers defined.")

schema = make_executable_schema(type_defs, query)
print("Executable schema created.")

app = GraphQL(schema)
print("GraphQL App Ready.")
