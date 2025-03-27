#!/usr/bin/env python3
"""
Simple Flask server to serve the neighborhood report.
"""

import os
from pathlib import Path
from flask import Flask, send_file

# Get the directory containing this script
SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
UI_DIR = SCRIPT_DIR / "ui"
NEIGHBORHOOD_FILE = UI_DIR / "neighborhood_report_updated.html"

app = Flask(__name__)

@app.route('/')
def serve_neighborhood():
    """Serve the neighborhood report HTML file"""
    if NEIGHBORHOOD_FILE.exists():
        print(f"Serving neighborhood file: {NEIGHBORHOOD_FILE}")
        return send_file(str(NEIGHBORHOOD_FILE))
    else:
        print(f"Neighborhood file not found: {NEIGHBORHOOD_FILE}")
        return "Neighborhood report not found", 404

if __name__ == "__main__":
    if not NEIGHBORHOOD_FILE.exists():
        print(f"ERROR: Neighborhood file not found: {NEIGHBORHOOD_FILE}")
        print(f"Files in {UI_DIR}:")
        for file in os.listdir(UI_DIR):
            print(f"  - {file}")
        exit(1)
        
    print(f"Starting server for: {NEIGHBORHOOD_FILE}")
    app.run(host='0.0.0.0', port=8888, debug=False) 