from ariadne import QueryType, make_executable_schema, gql
from ariadne.asgi import GraphQL
from pymongo import MongoClient
import os
import logging

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

# Define fixed GraphQL schema
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


# Resolver functions
def create_resolver(collection_name):
    """Creates a resolver for fetching data from MongoDB collections."""

    def resolve(_, info, limit=None):
        logging.info(f"Fetching data from collection: {collection_name}")
        results = list(db[collection_name].find({}, {"_id": 0}))
        logging.info(f"Returned {len(results)} records from {collection_name}")
        return results[:limit] if limit else results

    return resolve


# Bind resolvers for all collections
for collection in [
    "App_Sessions",
    "Deploy_Pokemon",
    "FitnessData",
    "FriendList",
    "GameplayLocationHistory",
    "Gym_battle",
    "InAppPurchases",
    "Incense_encounter",
    "Join_Raid_lobby",
    "Lure_encounter",
    "Map_Pokemon_encounter",
    "Pokestop_spin",
    "Sfida_capture",
    "SupportInteractions",
    "User_Attribution_Installs",
    "User_Attribution_Sessions",
]:
    query.set_field(f"get{collection}", create_resolver(collection))

schema = make_executable_schema(type_defs, query)
app = GraphQL(schema)
logging.info("GraphQL schema is ready.")
