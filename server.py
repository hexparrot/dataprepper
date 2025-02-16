import uvicorn
from schema import app  # Import the GraphQL app

if __name__ == "__main__":
    print("Starting server via uvicorn...")
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
