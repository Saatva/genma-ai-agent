#!/bin/bash

# Setup script for Data Catalog Generator
# This script creates a virtual environment and installs dependencies

set -e  # Exit on error

echo "=================================="
echo "Data Catalog Generator - Setup"
echo "=================================="
echo ""

# Check if virtual environment already exists
if [ -d "venv" ]; then
    echo "✓ Virtual environment already exists"
else
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "✓ Virtual environment created"
fi

echo ""
echo "Activating virtual environment..."
source venv/bin/activate

echo "✓ Virtual environment activated"
echo ""

echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo ""
echo "=================================="
echo "✓ Setup complete!"
echo "=================================="
echo ""
echo "To use the catalog generator:"
echo ""
echo "  1. Activate the virtual environment:"
echo "     source venv/bin/activate"
echo ""
echo "  2. Configure your environment:"
echo "     cp .env.example .env"
echo "     # Then edit .env with your credentials"
echo ""
echo "  3. Run the generator:"
echo "     python run.py"
echo ""
echo "  4. When done, deactivate the environment:"
echo "     deactivate"
echo ""
