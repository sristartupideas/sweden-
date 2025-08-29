#!/bin/bash

echo "Starting deployment process..."

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Install Playwright browsers
echo "Installing Playwright browsers..."
playwright install

# Install Playwright system dependencies
echo "Installing Playwright system dependencies..."
playwright install-deps

# Verify Playwright installation
echo "Verifying Playwright installation..."
if [ -d "/opt/render/.cache/ms-playwright" ]; then
    echo "Playwright browsers found in /opt/render/.cache/ms-playwright"
elif [ -d "/root/.cache/ms-playwright" ]; then
    echo "Playwright browsers found in /root/.cache/ms-playwright"
else
    echo "WARNING: Playwright browsers not found in expected locations"
    echo "Attempting to reinstall..."
    playwright install --force
fi

echo "Deployment setup complete!"
