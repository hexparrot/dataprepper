#!/usr/bin/env python3

from fastapi import FastAPI
from ariadne.asgi import GraphQL
from schema import schema

app = FastAPI()

# GraphQL endpoint
app.add_route("/graphql", GraphQL(schema, debug=True))

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
