#!/usr/bin/env python3

import logging
import sys
from fastapi import FastAPI, Request
from ariadne.asgi import GraphQL
from schema import schema

# Configure logging to explicitly output to stdout
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

app = FastAPI()


# Middleware to log every request
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logging.info(f"📥 Received request: {request.method} {request.url}")
    response = await call_next(request)
    return response


# Mount GraphQL
graphql_app = GraphQL(schema, debug=True)
app.add_route("/graphql", graphql_app)

logging.info("✅ GraphQL endpoint ready at /graphql")

if __name__ == "__main__":
    logging.info("🚀 Starting FastAPI server...")
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
