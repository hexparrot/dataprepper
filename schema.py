from ariadne import QueryType, make_executable_schema, gql
from ariadne.asgi import GraphQL
from pymongo import MongoClient
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

# MongoDB Connection using environment variables
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/admin"
DB_NAME = "pokemongo"


def get_mongo_client():
    """Establishes and returns a MongoDB client."""
    return MongoClient(MONGO_URI)


client = get_mongo_client()
db = client[DB_NAME]
query = QueryType()

# Define GraphQL Schema (Adding Triplet & allTriplets Query)
type_defs = gql(
    """
type Query {
    getApp_Sessions(limit: Int): [App_Sessions]
    getDeploy_Pokemon(limit: Int): [Deploy_Pokemon]
    getFitnessData(limit: Int): [FitnessData]
    getFriendList(limit: Int): [FriendList]
    getGameplayLocationHistory(limit: Int): [GameplayLocationHistory]
    getGym_battle(limit: Int): [Gym_battle]
    getInAppPurchases(limit: Int): [InAppPurchases]
    getIncense_encounter(limit: Int): [Incense_encounter]
    getJoin_Raid_lobby(limit: Int): [Join_Raid_lobby]
    getLure_encounter(limit: Int): [Lure_encounter]
    getMap_Pokemon_encounter(limit: Int): [Map_Pokemon_encounter]
    getPokestop_spin(limit: Int): [Pokestop_spin]
    getSfida_capture(limit: Int): [Sfida_capture]
    getSupportInteractions(limit: Int): [SupportInteractions]
    getUser_Attribution_Installs(limit: Int): [User_Attribution_Installs]
    getUser_Attribution_Sessions(limit: Int): [User_Attribution_Sessions]

    allTriplets: [Triplet]  # New Query to Fetch All Timestamp-Geo Records
    countTriplets: Int  # New Query to Count Records
}

type Triplet {
    timestamp: String
    latitude: Float
    longitude: Float
}

type App_Sessions {
    timestamp: String
    Install_time: String
    Event_name: String
    Cost_currency: String
    Channel: String
    Install_app_store: String
}

type Deploy_Pokemon {
    timestamp: String
    latitude: Float
    longitude: Float
}

type FitnessData {
    Steps_walked: Int
    Distance_travelled_meters: Int
    Calories_burned: Int
    Exercise_duration_minutes: Int
    Wheelchair_distance_meters: Int
    timestamp: String
}

type FriendList {
    Friends_codename: String
    Date_of_first_Niantic_friendship_start: String
    Nickname: String
    Games_they_are_Friends_in: String
    timestamp: String
}

type GameplayLocationHistory {
    timestamp: String
    latitude: Float
    longitude: Float
}

type Gym_battle {
    timestamp: String
}

type InAppPurchases {
    Type_of_activity: String
    Vendor: String
    Item_purchased: String
    Number_of_items: String
    Change_in_pokecoins: Int
    Money_spent_on_purchase: String
    Currency: String
    timestamp: String
}

type Pokestop_spin {
    Fort_Latitude: Float
    Fort_Longitude: Float
    timestamp: String
    latitude: Float
    longitude: Float
}

type SupportInteractions {
    Ticket_number_and_title: String
    Message_content: String
    timestamp: String
}

type Incense_encounter {
    timestamp: String
}

type Join_Raid_lobby {
    Gym_Latitude: Float
    Gym_Longitude: Float
    timestamp: String
    latitude: Float
    longitude: Float
}

type Lure_encounter {
    Gym_Latitude: Float
    Gym_Longitude: Float
    timestamp: String
    latitude: Float
    longitude: Float
}

type Map_Pokemon_encounter {
    timestamp: String
    latitude: Float
    longitude: Float
}

type Sfida_capture {
    Gym_Latitude: String
    Gym_Longitude: String
    timestamp: String
    latitude: Float
    longitude: Float
}

type User_Attribution_Installs {
    Device_IP_Address: String
    Type_of_Activity: String
    timestamp: String
}

type User_Attribution_Sessions {
    Device_IP_Address: String
    Type_of_Activity: String
    timestamp: String
}

"""
)


@query.field("allTriplets")
def resolve_all_triplets(_, info):
    """Fetches all records with timestamp, latitude, and longitude from all collections."""
    merged_data = []

    # Collections that may have timestamp, latitude, longitude
    collections = [
        "Deploy_Pokemon",
        "GameplayLocationHistory",
        "Pokestop_spin",
        "Join_Raid_lobby",
        "Lure_encounter",
        "Map_Pokemon_encounter",
        "Sfida_capture",
    ]

    for collection in collections:
        logging.info(f"Fetching triplets from {collection}")
        records = db[collection].find(
            {}, {"timestamp": 1, "latitude": 1, "longitude": 1, "_id": 0}
        )

        for record in records:
            if "timestamp" in record and "latitude" in record and "longitude" in record:
                # Ensure timestamp is converted to a string
                timestamp_value = record["timestamp"]

                # Convert datetime to ISO string if necessary
                if isinstance(timestamp_value, datetime):
                    timestamp_value = timestamp_value.isoformat()

                merged_data.append(
                    {
                        "timestamp": str(timestamp_value),  # Ensure it's a string
                        "latitude": record["latitude"],
                        "longitude": record["longitude"],
                    }
                )

    # Sort by timestamp (ensure string format)
    sorted_data = sorted(merged_data, key=lambda x: x["timestamp"])

    logging.info(f"Total triplets returned: {len(sorted_data)}")
    return sorted_data


@query.field("countTriplets")
def resolve_count_triplets(_, info):
    """Counts the total number of records with timestamp, latitude, and longitude."""
    total_count = 0

    collections = [
        "Deploy_Pokemon",
        "GameplayLocationHistory",
        "Pokestop_spin",
        "Join_Raid_lobby",
        "Lure_encounter",
        "Map_Pokemon_encounter",
        "Sfida_capture",
    ]

    for collection in collections:
        logging.info(f"Counting triplets in {collection}")

        count = db[collection].count_documents(
            {
                "timestamp": {"$exists": True},
                "latitude": {"$exists": True},
                "longitude": {"$exists": True},
            }
        )

        total_count += count

    logging.info(f"Total triplet count: {total_count}")
    return total_count


# Register Query Resolvers
schema = make_executable_schema(type_defs, query)
app = GraphQL(schema)

logging.info("GraphQL schema with allTriplets is ready.")
