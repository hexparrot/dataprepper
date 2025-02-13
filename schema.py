from datetime import datetime
from pymongo import MongoClient
from ariadne import QueryType, make_executable_schema, gql, ScalarType

# Connect to MongoDB
client = MongoClient(
    "mongodb://admin:password@localhost:27017/pokemongo?authSource=admin"
)
db = client["pokemongo"]

# Define DateTime scalar manually
datetime_scalar = ScalarType("DateTime")


@datetime_scalar.serializer
def serialize_datetime(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    return None


# Define GraphQL Schema
type_defs = gql(
    """
    scalar DateTime

    type AppSession {
        attributed_touch_time: DateTime
        install_time: DateTime
        event_time: DateTime
        event_name: String
        cost_currency: String
        channel: String
        install_app_store: String
        ad_ID: String
        region: String
        country_code: String
        state: String
        city: String
        postal_code: Int
        IP: String
        operator: String
        language: String
        android_ID: String
        advertising_ID: String
        IDFA: String
        IDFV: String
        amazon_fire_ID: String
        device_type: String
        device_category: String
        platform: String
        OS_version: Int
        app_version: String
        SDK_version: String
        app_name: String
        OAID: String
        conversion_type: String
        device_model: String
        ATT: String
    }

    type Query {
        getAppSessions: [AppSession]
    }
"""
)

query = QueryType()


# Helper function to parse timestamps safely
def parse_datetime(value):
    if isinstance(value, str):
        value = value.strip()  # Remove whitespace
        if value == "":  # Handle empty strings
            return None
        if value.endswith(" UTC"):
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S UTC")
            except ValueError:
                return None
    return value


# Helper function to parse integers safely
def parse_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


# Resolver function to fetch all records from app_sessions
@query.field("getAppSessions")
def resolve_get_app_sessions(_, info):
    collection = db["app_sessions"]
    results = list(collection.find({}, {"_id": 0}))  # Exclude MongoDB's _id field

    for record in results:
        record["attributed_touch_time"] = parse_datetime(
            record.get("Attributed_touch_time", None)
        )
        record["install_time"] = parse_datetime(record.get("Install_time", None))
        record["event_time"] = parse_datetime(record.get("Event_time", None))
        record["event_name"] = record.get("Event_name", None)
        record["cost_currency"] = record.get("Cost_currency", None)
        record["channel"] = record.get("Channel", None)
        record["install_app_store"] = record.get("Install_app_store", None)
        record["ad_ID"] = record.get("Ad_ID", None)
        record["region"] = record.get("Region", None)
        record["country_code"] = record.get("Country_code", None)
        record["state"] = record.get("State", None)
        record["city"] = record.get("City", None)
        record["postal_code"] = parse_int(record.get("Postal_code", None))
        record["IP"] = record.get("IP", None)
        record["operator"] = record.get("Operator", None)
        record["language"] = record.get("Language", None)

        # Fix missing fields (Correct case-sensitive MongoDB field names)
        record["android_ID"] = record.get("Android_ID", None)
        record["advertising_ID"] = record.get("Advertising_ID", None)
        record["IDFA"] = record.get("IDFA", None)
        record["IDFV"] = record.get("IDFV", None)
        record["amazon_fire_ID"] = record.get("Amazon_Fire_ID", None)
        record["device_type"] = record.get("Device_type", None)
        record["device_category"] = record.get("Device_category", None)
        record["platform"] = record.get("Platform", None)
        record["OS_version"] = parse_int(record.get("OS_version", None))
        record["app_version"] = record.get("App_version", None)
        record["SDK_version"] = record.get("SDK_version", None)
        record["app_name"] = record.get("App_name", None)
        record["OAID"] = record.get("OAID", None)
        record["conversion_type"] = record.get("Conversion_type", None)
        record["device_model"] = record.get("Device_model", None)
        record["ATT"] = record.get("ATT", None)

    return results


# Create executable schema
schema = make_executable_schema(type_defs, query, datetime_scalar)
