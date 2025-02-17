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
    # Check if the database exists in the configuration
    if database not in data_sources:
        raise ValueError(f"Database '{database}' not found in configuration.")

    db = client[database]
    merged_data = []

    # Fetch records from the passenger_rides collection (same collection)
    collection = "passenger_rides"
    fields = data_sources[database].get(collection)

    # Fetch records with dynamically matching fields
    records = db[collection].find(
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

    for record in records:
        # Extract requested triplet
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

        # Extract pickup triplet
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

        # Extract dropoff triplet
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

    # Return sorted data by timestamp
    return sorted(merged_data, key=lambda x: x["timestamp"])


@query.field("allTriplets")
def resolve_all_triplets(_, info, database):
    # Check if the database exists in the configuration
    if database not in data_sources:
        raise ValueError(f"Database '{database}' not found in configuration.")

    db = client[database]
    merged_data = []

    # Fetch records from the passenger_rides collection (same collection)
    collection = "passenger_rides"
    fields = data_sources[database].get(collection)

    # Fetch records with dynamically matching fields
    records = db[collection].find(
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

    for record in records:
        # Extract requested triplet
        if (
            "requested_timestamp" in record
            and "requested_lat" in record
            and "requested_lng" in record
        ):
            requested_timestamp = record["requested_timestamp"]
            requested_lat = record.get("requested_lat")
            requested_lng = record.get("requested_lng")

            # Validate lat/lng (ensure they are valid floats)
            if (
                requested_lat
                and isinstance(requested_lat, (int, float))
                and requested_lng
                and isinstance(requested_lng, (int, float))
            ):
                merged_data.append(
                    {
                        "timestamp": requested_timestamp.isoformat()
                        if isinstance(requested_timestamp, datetime)
                        else str(requested_timestamp),
                        "latitude": requested_lat if requested_lat != "" else None,
                        "longitude": requested_lng if requested_lng != "" else None,
                    }
                )

        # Extract pickup triplet
        if (
            "pickup_timestamp" in record
            and "pickup_lat" in record
            and "pickup_lng" in record
        ):
            pickup_timestamp = record["pickup_timestamp"]
            pickup_lat = record.get("pickup_lat")
            pickup_lng = record.get("pickup_lng")

            # Validate lat/lng (ensure they are valid floats)
            if (
                pickup_lat
                and isinstance(pickup_lat, (int, float))
                and pickup_lng
                and isinstance(pickup_lng, (int, float))
            ):
                merged_data.append(
                    {
                        "timestamp": pickup_timestamp.isoformat()
                        if isinstance(pickup_timestamp, datetime)
                        else str(pickup_timestamp),
                        "latitude": pickup_lat if pickup_lat != "" else None,
                        "longitude": pickup_lng if pickup_lng != "" else None,
                    }
                )

        # Extract dropoff triplet
        if (
            "dropoff_timestamp" in record
            and "dropoff_lat" in record
            and "dropoff_lng" in record
        ):
            dropoff_timestamp = record["dropoff_timestamp"]
            dropoff_lat = record.get("dropoff_lat")
            dropoff_lng = record.get("dropoff_lng")

            # Validate lat/lng (ensure they are valid floats)
            if (
                dropoff_lat
                and isinstance(dropoff_lat, (int, float))
                and dropoff_lng
                and isinstance(dropoff_lng, (int, float))
            ):
                merged_data.append(
                    {
                        "timestamp": dropoff_timestamp.isoformat()
                        if isinstance(dropoff_timestamp, datetime)
                        else str(dropoff_timestamp),
                        "latitude": dropoff_lat if dropoff_lat != "" else None,
                        "longitude": dropoff_lng if dropoff_lng != "" else None,
                    }
                )

    # Return sorted data by timestamp
    return sorted(merged_data, key=lambda x: x["timestamp"])


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
            count = db[collection].count_documents(count_query)
            logging.info(f"Count result for {collection}: {count}")

            total_count += count

    # Handle Lyft database (single collection, 3 triplets per record)
    elif database == "lyft":
        collection = "passenger_rides"
        logging.info(f"Counting triplets in {database}.{collection}.")

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
        count = db[collection].count_documents(count_query)
        logging.info(f"Count result for {collection}: {count}")

        # Each valid ride record will produce 3 triplets (requested, pickup, dropoff)
        total_count += count * 3

    logging.info(f"Total triplet count from {database}: {total_count}")
    return total_count


print("Resolvers defined.")

schema = make_executable_schema(type_defs, query)
print("Executable schema created.")

app = GraphQL(schema)
print("GraphQL App Ready.")
