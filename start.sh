#!/bin/bash

# Install Playwright browsers if not already installed
if [ ! -d "/opt/render/.cache/ms-playwright" ]; then
    echo "Installing Playwright browsers..."
    playwright install
    playwright install-deps
fi

# Start the FastAPI application
echo "Starting Swedish Business Scraper..."
uvicorn scraper:app --host 0.0.0.0 --port $PORT
