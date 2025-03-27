#!/usr/bin/env python3
"""
Simple HTTP server to serve static files from the UI directory.
"""
import os
import http.server
import socketserver
from pathlib import Path

# Get the directory containing this script
SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
UI_DIR = SCRIPT_DIR / "ui"

# Change directory to the UI directory
os.chdir(UI_DIR)
print(f"Serving files from: {UI_DIR}")

# Set up a simple HTTP server
PORT = 8080
Handler = http.server.SimpleHTTPRequestHandler

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"Server started at http://127.0.0.1:{PORT}")
    print("Hit Ctrl+C to stop the server")
    httpd.serve_forever() 