from flask import Flask, jsonify
import subprocess
import os
import time
from datetime import datetime

app = Flask(__name__)
LAST_RUN_FILE = "last_rental_run.txt"

@app.route('/api/check-last-run')
def check_last_run():
    try:
        with open(LAST_RUN_FILE, 'r') as f:
            last_run = float(f.read().strip())
            # Check if it's been more than 30 days
            should_run = (time.time() - last_run) >= (30 * 24 * 60 * 60)
            days_since_last_run = int((time.time() - last_run) / (24 * 60 * 60))
            return jsonify({
                "shouldRun": should_run,
                "lastRun": datetime.fromtimestamp(last_run).strftime('%Y-%m-%d %H:%M:%S'),
                "daysSinceLastRun": days_since_last_run
            })
    except FileNotFoundError:
        return jsonify({
            "shouldRun": True,
            "lastRun": None,
            "daysSinceLastRun": None
        })

@app.route('/api/trigger-propbot', methods=['POST'])
def trigger_propbot():
    try:
        # Run the PropBot workflow
        result = subprocess.run(['python3', 'propbot/main.py', '--scrape', '--analyze', '--report'],
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            # Update the last run timestamp
            with open(LAST_RUN_FILE, 'w') as f:
                f.write(str(time.time()))
            
            return jsonify({
                "success": True,
                "message": "PropBot workflow completed successfully"
            })
        else:
            return jsonify({
                "success": False,
                "message": f"PropBot workflow failed: {result.stderr}"
            }), 500
    
    except Exception as e:
        return jsonify({
            "success": False,
            "message": f"Error running PropBot workflow: {str(e)}"
        }), 500

if __name__ == '__main__':
    app.run(port=5000) 