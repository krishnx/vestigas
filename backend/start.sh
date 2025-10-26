#!/bin/bash
# Start the Uvicorn ASGI server for the FastAPI application.
# We explicitly set the host to 0.0.0.0 and port to 80 for Docker compatibility.
exec uvicorn backend.main:app --host 0.0.0.0 --port 80