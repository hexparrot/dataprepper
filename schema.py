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
"""
)

print("GraphQL schema defined.")


@query.field("availableDatabases")
def resolve_available_databases(_, info):
    return list(data_sources.keys())


@query.field("allTriplets")
def resolve_all_triplets(_, info, database):
    if database not in data_sources:
        raise ValueError(f"Database '{database}' not found in configuration.")

    db = client[database]
    merged_data = []

    for collection, fields in data_sources[database].items():
        logging.info(f"Fetching triplets from {database}.{collection}")
        records = db[collection].find(
            {},
            {
                fields["timestamp"]: 1,
                fields["latitude"]: 1,
                fields["longitude"]: 1,
                "_id": 0,
            },
        )

        for record in records:
            if (
                fields["timestamp"] in record
                and fields["latitude"] in record
                and fields["longitude"] in record
            ):
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

    return sorted(merged_data, key=lambda x: x["timestamp"])


@query.field("countTriplets")
def resolve_count_triplets(_, info, database):
    """Counts the total number of records with timestamp, latitude, and longitude in the specified database."""
    if database not in data_sources:
        raise ValueError(f"Database '{database}' not found in configuration.")

    db = client[database]
    total_count = 0

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

    logging.info(f"Total triplet count from {database}: {total_count}")
    return total_count


print("Resolvers defined.")

schema = make_executable_schema(type_defs, query)
print("Executable schema created.")

app = GraphQL(schema)
print("GraphQL App Ready.")
