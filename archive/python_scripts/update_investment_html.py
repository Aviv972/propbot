#!/usr/bin/env python3
import re
from datetime import datetime
import os

def update_html_file(file_path):
    # Create a backup of the original file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"{file_path}.bak_{timestamp}"
    os.system(f"cp {file_path} {backup_file}")
    print(f"Created backup of HTML file as {backup_file}")
    
    # Read the HTML file
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Update the neighborhoods count from 18 to 31
    content = re.sub(
        r'<div class="stat-title">Neighborhoods Analyzed</div>\s*<div class="stat-value">18</div>',
        '<div class="stat-title">Neighborhoods Analyzed</div>\n                            <div class="stat-value">31</div>',
        content
    )
    
    # Update the link to the new neighborhood report
    content = re.sub(
        r'<a href="neighborhood_report.html" class="btn btn-primary">View Detailed Neighborhood Analysis</a>',
        '<a href="neighborhood_report_updated.html" class="btn btn-primary">View Detailed Neighborhood Analysis</a>',
        content
    )
    
    # Write the updated content back to the file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"Updated HTML file with new neighborhood count and link")

if __name__ == "__main__":
    update_html_file("investment_summary.html") 