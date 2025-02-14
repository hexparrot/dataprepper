#!/usr/bin/env python3
import os
import json
import pymongo
from datetime import datetime

# MongoDB Connection
MONGO_URI = "mongodb://admin:password@localhost:27017/admin"
DB_NAME = "pokemongo"
DATA_DIR = "userdata/purposed/niantic"


def get_mongo_client():
    """Establishes and returns a MongoDB client."""
    return pymongo.MongoClient(MONGO_URI)


def convert_timestamp(doc):
    """Converts 'timestamp' field to datetime if present."""
    if isinstance(doc, dict) and "timestamp" in doc:
        try:
            doc["timestamp"] = datetime.fromisoformat(
                doc["timestamp"].replace("Z", "+00:00")
            )
        except ValueError:
            pass
    return doc


def ingest_json_files():
    """Iterates through JSON files in the standard directory and ingests them into MongoDB."""
    client = get_mongo_client()
    db = client[DB_NAME]

    if not os.path.exists(DATA_DIR):
        print(f"Data directory '{DATA_DIR}' does not exist.")
        return

    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".json"):
            collection_name = os.path.splitext(filename)[0]
            collection = db[collection_name]
            file_path = os.path.join(DATA_DIR, filename)

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # Skip empty or invalid JSON files
                if not data:
                    print(f"Skipping empty or invalid JSON file: {filename}")
                    continue

                if isinstance(data, list):
                    data = [convert_timestamp(doc) for doc in data]
                    for doc in data:
                        collection.update_one(doc, {"$set": doc}, upsert=True)
                elif isinstance(data, dict):
                    data = convert_timestamp(data)
                    collection.update_one(data, {"$set": data}, upsert=True)
                else:
                    print(f"Skipping unexpected JSON format in {filename}")

                print(
                    f"Ingested '{filename}' into collection '{collection_name}' with converted timestamps."
                )

            except json.JSONDecodeError:
                print(f"Skipping malformed JSON file: {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")

    client.close()


if __name__ == "__main__":
    ingest_json_files()
