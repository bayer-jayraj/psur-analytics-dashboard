#!/bin/bash

echo "=== Starting Azure Web App Setup ==="

# Set non-interactive mode for apt-get
export DEBIAN_FRONTEND=noninteractive

echo "Updating package lists..."
apt-get update -y

echo "Installing required packages..."
apt-get install -y curl gnupg2 software-properties-common apt-transport-https

echo "Adding Microsoft repository..."
# Download and install the Microsoft signing key
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg

# Add the Microsoft repository
echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/20.04/prod focal main" > /etc/apt/sources.list.d/mssql-release.list

echo "Updating package lists with Microsoft repository..."
apt-get update -y

echo "Installing ODBC Driver 17 for SQL Server..."
# Install ODBC Driver 17 (more reliable than 13)
ACCEPT_EULA=Y apt-get install -y msodbcsql17

echo "Installing unixODBC development libraries..."
apt-get install -y unixodbc-dev

echo "Verifying ODBC installation..."
odbcinst -q -d

echo "=== ODBC Driver installation completed ==="

echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "=== Starting Streamlit application ==="
python -m streamlit run app.py --server.port=${PORT:-8000} --server.address=0.0.0.0 --server.headless=true --server.enableCORS=false --server.enableXsrfProtection=false
