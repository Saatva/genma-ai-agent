#!/usr/bin/env python3
"""
Quick start script for running the data catalog generator
"""

import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.main import main

if __name__ == '__main__':
    main()
