#!/bin/bash

# -----------------------------
# Command Centre Deployment Script
# -----------------------------

set -e  # stop on error

APP_DIR="/opt/command_centre"
REPO_URL="https://gitlab.com/your-team/command-centre-automation.git"  # <-- update this
VENV_DIR="$APP_DIR/venv"
PYTHON=$(which python3)

echo "=== Starting Command Centre setup ==="

# Clone or update repo
if [ ! -d "$APP_DIR" ]; then
    echo "Cloning repository..."
    sudo git clone $REPO_URL $APP_DIR
else
    echo "Repository exists, pulling latest changes..."
    cd $APP_DIR
    sudo git pull
fi

cd $APP_DIR

# Create folders
mkdir -p uploads data
sudo chown -R $USER:$USER $APP_DIR

# Create virtual environment if not exists
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    $PYTHON -m venv $VENV_DIR
fi

# Activate venv and install dependencies
echo "Installing dependencies..."
source $VENV_DIR/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Run Flask (test mode)
echo "Starting Flask for testing..."
nohup $VENV_DIR/bin/python app.py > flask.log 2>&1 &

echo "=== Flask is running in background on port 5000 ==="
echo "Check logs with: tail -f $APP_DIR/flask.log"
