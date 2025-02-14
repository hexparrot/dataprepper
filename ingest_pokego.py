import os
import json
import pymongo
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# MongoDB Connection
MONGO_URI = "mongodb://admin:uberleet@localhost:27017/admin"
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


def document_exists(collection, doc):
    """Checks if a document already exists in the collection."""
    return collection.count_documents(doc, limit=1) > 0


def ingest_json_files():
    """Iterates through JSON files in the standard directory and ingests them into MongoDB."""
    client = get_mongo_client()
    db = client[DB_NAME]

    if not os.path.exists(DATA_DIR):
        logging.warning(f"Data directory '{DATA_DIR}' does not exist.")
        return

    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".json"):
            collection_name = os.path.splitext(filename)[0]
            collection = db[collection_name]
            file_path = os.path.join(DATA_DIR, filename)

            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if not data:
                        logging.info(f"Skipping empty JSON file: {filename}")
                        continue

                    data = (
                        [convert_timestamp(doc) for doc in data]
                        if isinstance(data, list)
                        else [convert_timestamp(data)]
                    )

                    if collection.estimated_document_count() == 0:
                        collection.insert_many(data)
                        logging.info(
                            f"Inserted {len(data)} documents into new collection '{collection_name}'."
                        )
                    else:
                        duplicate_count = 0
                        for doc in data:
                            if not document_exists(collection, doc):
                                collection.insert_one(doc)
                            else:
                                duplicate_count += 1
                        logging.info(
                            f"Skipped {duplicate_count} duplicate documents in '{collection_name}'."
                        )
            except Exception as e:
                logging.error(f"Error processing {filename}: {e}")

    client.close()


if __name__ == "__main__":
    ingest_json_files()
