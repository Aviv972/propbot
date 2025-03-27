#!/usr/bin/env python3
import pandas as pd
import re
import os
from datetime import datetime
import numpy as np
import json
import time

# Input/output configuration
csv_file = "investment_summary_with_metrics_fixed.csv"
html_file = "investment_dashboard.html"
last_run_file = "last_rental_run.txt"

print(f"Generating improved investment dashboard using data from {csv_file}")

# Load the CSV data
df = pd.read_csv(csv_file)
print(f"Loaded {len(df)} properties")

# Add timestamp if not exists (for demo, using current time for all properties)
if 'Added Date' not in df.columns:
    df['Added Date'] = int(time.time())

# Sort properties by Added Date (newest first)
df = df.sort_values('Added Date', ascending=False)

# Calculate summary statistics
stats = {
    "total_properties": len(df),
    "neighborhoods": len(df['Neighborhood'].dropna().unique()),
    "avg_gross_yield": df['Gross Yield (%)'].mean(),
    "avg_cap_rate": df['Cap Rate (%)'].mean(),
    "avg_price_sqm": df['Price per sqm (€)'].mean(),
    "median_price": df['Price (€)'].median(),
    "high_potential": sum(df['Cap Rate (%)'] >= 6.0),
    "medium_potential": sum((df['Cap Rate (%)'] >= 4.5) & (df['Cap Rate (%)'] < 6.0)),
    "low_potential": sum(df['Cap Rate (%)'] < 4.5),
    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M")
}

# Get top 5 neighborhoods by property count
top_neighborhoods = df['Neighborhood'].value_counts().head(5).to_dict()

# Create property JSON for JavaScript use
properties_json = []
for _, row in df.iterrows():
    # Determine classification based on cap rate
    classification = "Unknown"
    if pd.notna(row['Cap Rate (%)']):
        if row['Cap Rate (%)'] >= 6.0:
            classification = "High Potential"
        elif row['Cap Rate (%)'] >= 4.5:
            classification = "Medium Potential"
        else:
            classification = "Low Potential"
    
    # Create property object
    prop = {
        "url": row['Property URL'],
        "price": float(row['Price (€)']) if pd.notna(row['Price (€)']) else None,
        "neighborhood": row['Neighborhood'] if pd.notna(row['Neighborhood']) else "Unknown",
        "size": float(row['Size (sqm)']) if pd.notna(row['Size (sqm)']) else None,
        "monthly_rent": float(row['Monthly Rent (€)']) if pd.notna(row['Monthly Rent (€)']) else None,
        "annual_rent": float(row['Annual Rent (€)']) if pd.notna(row['Annual Rent (€)']) else None,
        "recurring_expenses": float(row['Total Recurring Expenses (€)']) if pd.notna(row['Total Recurring Expenses (€)']) else None,
        "noi": float(row['NOI (€)']) if pd.notna(row['NOI (€)']) else None,
        "cap_rate": float(row['Cap Rate (%)']) if pd.notna(row['Cap Rate (%)']) else None,
        "gross_yield": float(row['Gross Yield (%)']) if pd.notna(row['Gross Yield (%)']) else None,
        "price_sqm": float(row['Price per sqm (€)']) if pd.notna(row['Price per sqm (€)']) else None,
        "classification": classification,
        "added_date": int(row['Added Date'])
    }
    properties_json.append(prop)

# Check if it's been a month since last rental data update
def check_last_rental_run():
    try:
        with open(last_run_file, 'r') as f:
            last_run = float(f.read().strip())
            # Check if it's been more than 30 days
            return (time.time() - last_run) >= (30 * 24 * 60 * 60)
    except FileNotFoundError:
        return True

# Create the HTML dashboard
html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PropBot Investment Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
    <style>
        :root {{
            --primary-color: #1e88e5;
            --primary-dark: #1565c0;
            --secondary-color: #26a69a;
            --light-bg: #f5f7fa;
            --dark-text: #333;
            --light-text: #fff;
            --border-color: #ddd;
            --success-color: #4caf50;
            --warning-color: #ff9800;
            --danger-color: #f44336;
            --card-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            --transition: all 0.3s ease;
        }}
        
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Roboto', sans-serif;
            background-color: var(--light-bg);
            color: var(--dark-text);
            line-height: 1.6;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        header {{
            text-align: center;
            margin-bottom: 30px;
            padding: 20px 0;
            border-bottom: 1px solid var(--border-color);
            background-color: white;
            box-shadow: var(--card-shadow);
            border-radius: 8px;
        }}
        
        header h1 {{
            color: var(--primary-color);
            font-size: 2.5rem;
            margin-bottom: 10px;
        }}
        
        header p {{
            color: #666;
            font-size: 1.1rem;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .stat-card {{
            background-color: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: var(--card-shadow);
            text-align: center;
            transition: var(--transition);
        }}
        
        .stat-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 6px 12px rgba(0, 0, 0, 0.15);
        }}
        
        .stat-card .stat-icon {{
            font-size: 2rem;
            margin-bottom: 15px;
            color: var(--primary-color);
        }}
        
        .stat-card h3 {{
            font-size: 0.9rem;
            color: #666;
            font-weight: 500;
            margin-bottom: 10px;
            text-transform: uppercase;
        }}
        
        .stat-card .stat-value {{
            font-size: 1.8rem;
            font-weight: 700;
            color: var(--dark-text);
        }}
        
        .dashboard-section {{
            background-color: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 30px;
            box-shadow: var(--card-shadow);
        }}
        
        .section-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 1px solid var(--border-color);
        }}
        
        .section-header h2 {{
            font-size: 1.5rem;
            color: var(--primary-color);
        }}
        
        .filters {{
            margin-bottom: 20px;
            display: flex;
            flex-wrap: wrap;
            gap: 15px;
            align-items: center;
        }}
        
        .filter-group {{
            display: flex;
            flex-direction: column;
            gap: 5px;
        }}
        
        .filter-group label {{
            font-size: 0.9rem;
            color: #666;
        }}
        
        .filter-group select,
        .filter-group input {{
            padding: 8px 12px;
            border: 1px solid var(--border-color);
            border-radius: 4px;
            font-size: 1rem;
            min-width: 150px;
        }}
        
        .btn {{
            padding: 10px 16px;
            border: none;
            border-radius: 4px;
            background-color: var(--primary-color);
            color: var(--light-text);
            font-size: 0.9rem;
            cursor: pointer;
            transition: var(--transition);
            text-decoration: none;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
        }}
        
        .btn:hover {{
            background-color: var(--primary-dark);
        }}
        
        .btn-outline {{
            background-color: transparent;
            border: 1px solid var(--primary-color);
            color: var(--primary-color);
        }}
        
        .btn-outline:hover {{
            background-color: var(--primary-color);
            color: var(--light-text);
        }}
        
        .btn-secondary {{
            background-color: var(--secondary-color);
        }}
        
        .btn-secondary:hover {{
            background-color: #00897b;
        }}
        
        /* Property table styles */
        .table-responsive {{
            overflow-x: auto;
            margin-bottom: 20px;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            table-layout: fixed;
        }}
        
        th, td {{
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }}
        
        th {{
            background-color: var(--primary-color);
            color: var(--light-text);
            position: sticky;
            top: 0;
            font-weight: 500;
            white-space: nowrap;
        }}
        
        tbody tr {{
            transition: var(--transition);
        }}
        
        tbody tr:hover {{
            background-color: rgba(0, 0, 0, 0.02);
        }}
        
        .property-link {{
            color: var(--primary-color);
            text-decoration: none;
            font-weight: 500;
        }}
        
        .property-link:hover {{
            text-decoration: underline;
        }}
        
        .badge {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        .badge-success {{
            background-color: rgba(76, 175, 80, 0.15);
            color: var(--success-color);
        }}
        
        .badge-warning {{
            background-color: rgba(255, 152, 0, 0.15);
            color: var(--warning-color);
        }}
        
        .badge-danger {{
            background-color: rgba(244, 67, 54, 0.15);
            color: var(--danger-color);
        }}
        
        /* Property cards for mobile view */
        .property-cards {{
            display: none;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 20px;
        }}
        
        .property-card {{
            background-color: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: var(--card-shadow);
            transition: var(--transition);
        }}
        
        .property-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 10px 20px rgba(0, 0, 0, 0.1);
        }}
        
        .property-card-header {{
            padding: 15px;
            background-color: var(--primary-color);
            color: var(--light-text);
        }}
        
        .property-card-body {{
            padding: 15px;
        }}
        
        .property-card-row {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 10px;
            padding-bottom: 10px;
            border-bottom: 1px solid var(--border-color);
        }}
        
        .property-card-row:last-child {{
            border-bottom: none;
            margin-bottom: 0;
            padding-bottom: 0;
        }}
        
        .property-card-label {{
            font-size: 0.9rem;
            color: #666;
        }}
        
        .property-card-value {{
            font-weight: 500;
        }}
        
        .property-card-footer {{
            padding: 15px;
            background-color: #f9f9f9;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        /* Visualization section */
        .visualization-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }}
        
        .chart-container {{
            background-color: white;
            border-radius: 8px;
            box-shadow: var(--card-shadow);
            padding: 20px;
            height: 300px;
        }}
        
        .neighborhoods-list {{
            list-style: none;
            margin-top: 15px;
        }}
        
        .neighborhoods-list li {{
            display: flex;
            justify-content: space-between;
            padding: 10px 0;
            border-bottom: 1px solid var(--border-color);
        }}
        
        .neighborhoods-list li:last-child {{
            border-bottom: none;
        }}
        
        .neighborhoods-list .neighborhood-name {{
            font-weight: 500;
        }}
        
        .neighborhoods-list .neighborhood-count {{
            background-color: var(--primary-color);
            color: white;
            border-radius: 20px;
            padding: 2px 10px;
            font-size: 0.8rem;
        }}
        
        /* Pagination */
        .pagination {{
            display: flex;
            justify-content: center;
            align-items: center;
            margin-top: 20px;
            gap: 8px;
            flex-wrap: wrap;
        }}
        
        .pagination-button {{
            padding: 8px 16px;
            border: 1px solid var(--border-color);
            background-color: white;
            cursor: pointer;
            transition: var(--transition);
            border-radius: 4px;
            font-size: 14px;
            min-width: 40px;
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        
        .pagination-button:hover:not(:disabled) {{
            background-color: var(--primary-color);
            color: white;
            border-color: var(--primary-color);
        }}
        
        .pagination-button.active {{
            background-color: var(--primary-color);
            color: white;
            border-color: var(--primary-color);
        }}
        
        .pagination-button:disabled {{
            opacity: 0.5;
            cursor: not-allowed;
        }}
        
        .pagination-info {{
            font-size: 14px;
            color: #666;
            margin: 0 10px;
        }}
        
        /* Action buttons styles */
        .action-buttons {{
            margin: 20px 0;
            display: flex;
            gap: 10px;
            align-items: center;
        }}
        
        .action-button {{
            padding: 12px 24px;
            border: none;
            border-radius: 4px;
            background-color: var(--primary-color);
            color: white;
            font-size: 1rem;
            cursor: pointer;
            transition: var(--transition);
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }}
        
        .action-button:hover {{
            background-color: var(--primary-dark);
        }}
        
        .action-button:disabled {{
            opacity: 0.7;
            cursor: not-allowed;
        }}
        
        .action-button i {{
            font-size: 1.2rem;
        }}
        
        .status-message {{
            font-size: 0.9rem;
            color: #666;
            margin-left: 10px;
        }}
        
        /* Responsive Design */
        @media (max-width: 1024px) {{
            .stats-grid {{
                grid-template-columns: repeat(3, 1fr);
            }}
            
            .visualization-grid {{
                grid-template-columns: 1fr;
            }}
        }}
        
        @media (max-width: 768px) {{
            .stats-grid {{
                grid-template-columns: repeat(2, 1fr);
            }}
            
            .table-responsive {{
                display: none;
            }}
            
            .property-cards {{
                display: grid;
            }}
            
            .filters {{
                flex-direction: column;
                align-items: stretch;
            }}
        }}
        
        @media (max-width: 480px) {{
            .stats-grid {{
                grid-template-columns: 1fr;
            }}
            
            .property-cards {{
                grid-template-columns: 1fr;
            }}
            
            .container {{
                padding: 10px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>PropBot Investment Dashboard</h1>
            <p>Real-time property investment analysis and metrics</p>
        </header>
        
        <!-- Action Buttons Section -->
        <div class="dashboard-section">
            <div class="action-buttons">
                <button id="runPropbotBtn" class="action-button">
                    <i class="fas fa-sync-alt"></i> Update Property Data
                </button>
                <span id="statusMessage" class="status-message"></span>
            </div>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-icon"><i class="fas fa-building"></i></div>
                <h3>Total Properties</h3>
                <div class="stat-value">{stats['total_properties']}</div>
            </div>
            <div class="stat-card">
                <div class="stat-icon"><i class="fas fa-map-marker-alt"></i></div>
                <h3>Neighborhoods</h3>
                <div class="stat-value">{stats['neighborhoods']}</div>
            </div>
            <div class="stat-card">
                <div class="stat-icon"><i class="fas fa-chart-line"></i></div>
                <h3>Avg Gross Yield</h3>
                <div class="stat-value">{stats['avg_gross_yield']:.2f}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-icon"><i class="fas fa-percentage"></i></div>
                <h3>Avg Cap Rate</h3>
                <div class="stat-value">{stats['avg_cap_rate']:.2f}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-icon"><i class="fas fa-euro-sign"></i></div>
                <h3>Avg Price/sqm</h3>
                <div class="stat-value">€{stats['avg_price_sqm']:.2f}</div>
            </div>
            <div class="stat-card">
                <div class="stat-icon"><i class="fas fa-clock"></i></div>
                <h3>Last Updated</h3>
                <div class="stat-value" style="font-size: 1.2rem;">{stats['last_updated']}</div>
            </div>
        </div>
        
        <div class="dashboard-section">
            <div class="section-header">
                <h2>Investment Opportunities</h2>
                <a href="neighborhood_report_updated.html" class="btn btn-outline">
                    <i class="fas fa-map"></i> View Neighborhood Analysis
                </a>
            </div>
            
            <div class="filters">
                <div class="filter-group">
                    <label for="neighborhood-filter">Neighborhood</label>
                    <select id="neighborhood-filter">
                        <option value="">All Neighborhoods</option>
                    </select>
                </div>
                <div class="filter-group">
                    <label for="classification-filter">Classification</label>
                    <select id="classification-filter">
                        <option value="">All Classifications</option>
                        <option value="High Potential">High Potential</option>
                        <option value="Medium Potential">Medium Potential</option>
                        <option value="Low Potential">Low Potential</option>
                    </select>
                </div>
                <div class="filter-group">
                    <label for="min-price">Min Price</label>
                    <input type="number" id="min-price" placeholder="Min €">
                </div>
                <div class="filter-group">
                    <label for="max-price">Max Price</label>
                    <input type="number" id="max-price" placeholder="Max €">
                </div>
                <div class="filter-group">
                    <label for="min-cap-rate">Min Cap Rate</label>
                    <input type="number" id="min-cap-rate" placeholder="Min %" step="0.1">
                </div>
                <button id="apply-filters" class="btn">
                    <i class="fas fa-filter"></i> Apply Filters
                </button>
                <button id="reset-filters" class="btn btn-outline">
                    <i class="fas fa-undo"></i> Reset
                </button>
            </div>
            
            <div id="filter-summary" style="margin-bottom: 15px; font-size: 0.9rem; color: #666;"></div>
            
            <!-- Table View (desktop) -->
            <div class="table-responsive">
                <table id="properties-table">
                    <thead>
                        <tr>
                            <th>Property</th>
                            <th>Price (€)</th>
                            <th>Neighborhood</th>
                            <th>Size (sqm)</th>
                            <th>Monthly Rent (€)</th>
                            <th>Annual Rent (€)</th>
                            <th>NOI (€)</th>
                            <th>Cap Rate (%)</th>
                            <th>Gross Yield (%)</th>
                            <th>Price/sqm (€)</th>
                            <th>Classification</th>
                        </tr>
                    </thead>
                    <tbody id="properties-table-body">
                        <!-- Table rows will be populated by JavaScript -->
                    </tbody>
                </table>
            </div>
            
            <!-- Card View (mobile) -->
            <div class="property-cards" id="property-cards-container">
                <!-- Property cards will be populated by JavaScript -->
            </div>
            
            <div class="pagination" id="pagination">
                <!-- Pagination will be populated by JavaScript -->
            </div>
        </div>
        
        <div class="visualization-grid">
            <div class="dashboard-section">
                <div class="section-header">
                    <h2>Investment Classification</h2>
                </div>
                <div class="chart-container">
                    <canvas id="classification-chart"></canvas>
                </div>
            </div>
            
            <div class="dashboard-section">
                <div class="section-header">
                    <h2>Top Neighborhoods</h2>
                </div>
                <ul class="neighborhoods-list">
                    <!-- Top neighborhoods will be populated by JavaScript -->
                </ul>
            </div>
        </div>
        
        <footer>
            <p>PropBot Investment Analysis Dashboard &copy; 2025 | Powered by PropBot Analysis Engine</p>
        </footer>
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script>
        // Property data
        const properties = {json.dumps(properties_json)};
        
        // Top neighborhoods
        const topNeighborhoods = {json.dumps(top_neighborhoods)};
        
        // Stats
        const stats = {json.dumps(stats)};
        
        // Populate neighborhoods dropdown
        const neighborhoodFilter = document.getElementById('neighborhood-filter');
        const uniqueNeighborhoods = [...new Set(properties.map(p => p.neighborhood))].sort();
        uniqueNeighborhoods.forEach(neighborhood => {{
            const option = document.createElement('option');
            option.value = neighborhood;
            option.textContent = neighborhood;
            neighborhoodFilter.appendChild(option);
        }});
        
        // Utility functions for formatting
        function formatCurrency(value) {{
            return value !== null ? `€${{value.toLocaleString('en-US', {{minimumFractionDigits: 2, maximumFractionDigits: 2}})}}` : '-';
        }}
        
        function formatPercentage(value) {{
            return value !== null ? `${{value.toFixed(2)}}%` : '-';
        }}
        
        function formatNumber(value) {{
            return value !== null ? value.toLocaleString('en-US', {{minimumFractionDigits: 2, maximumFractionDigits: 2}}) : '-';
        }}
        
        function getClassificationBadge(classification) {{
            if (classification === 'High Potential') {{
                return '<span class="badge badge-success">High Potential</span>';
            }} else if (classification === 'Medium Potential') {{
                return '<span class="badge badge-warning">Medium Potential</span>';
            }} else if (classification === 'Low Potential') {{
                return '<span class="badge badge-danger">Low Potential</span>';
            }} else {{
                return '<span class="badge">Unknown</span>';
            }}
        }}
        
        // Property display functions
        function renderPropertiesTable(properties) {{
            const tableBody = document.getElementById('properties-table-body');
            tableBody.innerHTML = '';
            
            properties.forEach(property => {{
                const tr = document.createElement('tr');
                
                tr.innerHTML = `
                    <td><a href="${{property.url}}" class="property-link" target="_blank">View Property</a></td>
                    <td>${{formatCurrency(property.price)}}</td>
                    <td>${{property.neighborhood}}</td>
                    <td>${{formatNumber(property.size)}} sqm</td>
                    <td>${{formatCurrency(property.monthly_rent)}}</td>
                    <td>${{formatCurrency(property.annual_rent)}}</td>
                    <td>${{formatCurrency(property.noi)}}</td>
                    <td>${{formatPercentage(property.cap_rate)}}</td>
                    <td>${{formatPercentage(property.gross_yield)}}</td>
                    <td>${{formatCurrency(property.price_sqm)}}</td>
                    <td>${{getClassificationBadge(property.classification)}}</td>
                `;
                
                tableBody.appendChild(tr);
            }});
        }}
        
        function renderPropertyCards(properties) {{
            const cardsContainer = document.getElementById('property-cards-container');
            cardsContainer.innerHTML = '';
            
            properties.forEach(property => {{
                const card = document.createElement('div');
                card.className = 'property-card';
                
                card.innerHTML = `
                    <div class="property-card-header">
                        <h3>€${{property.price ? property.price.toLocaleString() : '-'}}</h3>
                    </div>
                    <div class="property-card-body">
                        <div class="property-card-row">
                            <span class="property-card-label">Neighborhood:</span>
                            <span class="property-card-value">${{property.neighborhood}}</span>
                        </div>
                        <div class="property-card-row">
                            <span class="property-card-label">Size:</span>
                            <span class="property-card-value">${{formatNumber(property.size)}} sqm</span>
                        </div>
                        <div class="property-card-row">
                            <span class="property-card-label">Monthly Rent:</span>
                            <span class="property-card-value">${{formatCurrency(property.monthly_rent)}}</span>
                        </div>
                        <div class="property-card-row">
                            <span class="property-card-label">Cap Rate:</span>
                            <span class="property-card-value">${{formatPercentage(property.cap_rate)}}</span>
                        </div>
                        <div class="property-card-row">
                            <span class="property-card-label">Gross Yield:</span>
                            <span class="property-card-value">${{formatPercentage(property.gross_yield)}}</span>
                        </div>
                        <div class="property-card-row">
                            <span class="property-card-label">Classification:</span>
                            <span class="property-card-value">${{getClassificationBadge(property.classification)}}</span>
                        </div>
                    </div>
                    <div class="property-card-footer">
                        <a href="${{property.url}}" class="btn btn-outline" target="_blank">View Property</a>
                        <span>${{formatCurrency(property.price_sqm)}}/sqm</span>
                    </div>
                `;
                
                cardsContainer.appendChild(card);
            }});
        }}
        
        // Filtering logic
        function applyFilters() {{
            const neighborhoodValue = document.getElementById('neighborhood-filter').value;
            const classificationValue = document.getElementById('classification-filter').value;
            const minPriceValue = document.getElementById('min-price').value;
            const maxPriceValue = document.getElementById('max-price').value;
            const minCapRateValue = document.getElementById('min-cap-rate').value;
            
            let filtered = [...properties];
            
            if (neighborhoodValue) {{
                filtered = filtered.filter(p => p.neighborhood === neighborhoodValue);
            }}
            
            if (classificationValue) {{
                filtered = filtered.filter(p => p.classification === classificationValue);
            }}
            
            if (minPriceValue) {{
                filtered = filtered.filter(p => p.price >= parseFloat(minPriceValue));
            }}
            
            if (maxPriceValue) {{
                filtered = filtered.filter(p => p.price <= parseFloat(maxPriceValue));
            }}
            
            if (minCapRateValue) {{
                filtered = filtered.filter(p => p.cap_rate >= parseFloat(minCapRateValue));
            }}
            
            // Update filter summary
            let summary = `Showing ${{filtered.length}} of ${{properties.length}} properties`;
            const filters = [];
            
            if (neighborhoodValue) filters.push(`Neighborhood: ${{neighborhoodValue}}`);
            if (classificationValue) filters.push(`Classification: ${{classificationValue}}`);
            if (minPriceValue) filters.push(`Min Price: €${{parseFloat(minPriceValue).toLocaleString()}}`);
            if (maxPriceValue) filters.push(`Max Price: €${{parseFloat(maxPriceValue).toLocaleString()}}`);
            if (minCapRateValue) filters.push(`Min Cap Rate: ${{minCapRateValue}}%`);
            
            if (filters.length > 0) {{
                summary += ` (Filters: ${{filters.join(', ')}})`;
            }}
            
            document.getElementById('filter-summary').textContent = summary;
            
            // Update visualizations
            renderPropertiesTable(filtered);
            renderPropertyCards(filtered);
            setupPagination(filtered);
        }}
        
        document.getElementById('apply-filters').addEventListener('click', applyFilters);
        
        document.getElementById('reset-filters').addEventListener('click', function() {{
            document.getElementById('neighborhood-filter').value = '';
            document.getElementById('classification-filter').value = '';
            document.getElementById('min-price').value = '';
            document.getElementById('max-price').value = '';
            document.getElementById('min-cap-rate').value = '';
            
            applyFilters();
        }});
        
        // Sort properties by added_date (newest first)
        properties.sort((a, b) => b.added_date - a.added_date);
        
        // Updated pagination function
        function setupPagination(properties, pageSize = 10) {{
            const pageCount = Math.ceil(properties.length / pageSize);
            const paginationContainer = document.getElementById('pagination');
            paginationContainer.innerHTML = '';
            
            if (pageCount <= 1) return;
            
            let currentPage = 1;
            
            function updatePagination() {{
                paginationContainer.innerHTML = '';
                
                // Previous button
                const prevButton = document.createElement('button');
                prevButton.className = 'pagination-button';
                prevButton.innerHTML = '<i class="fas fa-chevron-left"></i>';
                prevButton.disabled = currentPage === 1;
                prevButton.addEventListener('click', () => goToPage(currentPage - 1));
                paginationContainer.appendChild(prevButton);
                
                // Page numbers
                let startPage = Math.max(1, currentPage - 2);
                let endPage = Math.min(pageCount, startPage + 4);
                startPage = Math.max(1, endPage - 4);
                
                if (startPage > 1) {{
                    paginationContainer.appendChild(createPageButton(1));
                    if (startPage > 2) {{
                        const ellipsis = document.createElement('span');
                        ellipsis.className = 'pagination-info';
                        ellipsis.textContent = '...';
                        paginationContainer.appendChild(ellipsis);
                    }}
                }}
                
                for (let i = startPage; i <= endPage; i++) {{
                    paginationContainer.appendChild(createPageButton(i));
                }}
                
                if (endPage < pageCount) {{
                    if (endPage < pageCount - 1) {{
                        const ellipsis = document.createElement('span');
                        ellipsis.className = 'pagination-info';
                        ellipsis.textContent = '...';
                        paginationContainer.appendChild(ellipsis);
                    }}
                    paginationContainer.appendChild(createPageButton(pageCount));
                }}
                
                // Next button
                const nextButton = document.createElement('button');
                nextButton.className = 'pagination-button';
                nextButton.innerHTML = '<i class="fas fa-chevron-right"></i>';
                nextButton.disabled = currentPage === pageCount;
                nextButton.addEventListener('click', () => goToPage(currentPage + 1));
                paginationContainer.appendChild(nextButton);
                
                // Page info
                const pageInfo = document.createElement('span');
                pageInfo.className = 'pagination-info';
                const startItem = (currentPage - 1) * pageSize + 1;
                const endItem = Math.min(currentPage * pageSize, properties.length);
                pageInfo.textContent = 'Showing ' + startItem + '-' + endItem + ' of ' + properties.length + ' properties';
                paginationContainer.appendChild(pageInfo);
            }}
            
            function createPageButton(pageNum) {{
                const button = document.createElement('button');
                button.className = 'pagination-button' + (pageNum === currentPage ? ' active' : '');
                button.textContent = pageNum;
                button.addEventListener('click', () => goToPage(pageNum));
                return button;
            }}
            
            function goToPage(page) {{
                currentPage = page;
                const start = (page - 1) * pageSize;
                const end = start + pageSize;
                const paginatedProperties = properties.slice(start, end);
                
                renderPropertiesTable(paginatedProperties);
                renderPropertyCards(paginatedProperties);
                updatePagination();
            }}
            
            // Initial render
            updatePagination();
            goToPage(1);
        }}
        
        // Classification chart
        function initClassificationChart() {{
            const ctx = document.getElementById('classification-chart').getContext('2d');
            
            const highCount = properties.filter(p => p.classification === 'High Potential').length;
            const mediumCount = properties.filter(p => p.classification === 'Medium Potential').length;
            const lowCount = properties.filter(p => p.classification === 'Low Potential').length;
            
            new Chart(ctx, {{
                type: 'doughnut',
                data: {{
                    labels: ['High Potential', 'Medium Potential', 'Low Potential'],
                    datasets: [{{
                        data: [highCount, mediumCount, lowCount],
                        backgroundColor: [
                            'rgba(76, 175, 80, 0.7)',   // Green
                            'rgba(255, 152, 0, 0.7)',   // Orange
                            'rgba(244, 67, 54, 0.7)'    // Red
                        ],
                        borderColor: [
                            'rgba(76, 175, 80, 1)',
                            'rgba(255, 152, 0, 1)',
                            'rgba(244, 67, 54, 1)'
                        ],
                        borderWidth: 1
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{
                            position: 'bottom'
                        }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    const label = context.label || '';
                                    const value = context.raw || 0;
                                    const percentage = (value / properties.length * 100).toFixed(1);
                                    return `${{label}}: ${{value}} (${{percentage}}%)`;
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}
        
        // Top neighborhoods list
        function renderTopNeighborhoods() {{
            const list = document.querySelector('.neighborhoods-list');
            
            Object.entries(topNeighborhoods).forEach(([neighborhood, count]) => {{
                const li = document.createElement('li');
                li.innerHTML = `
                    <span class="neighborhood-name">${{neighborhood}}</span>
                    <span class="neighborhood-count">${{count}}</span>
                `;
                list.appendChild(li);
            }});
        }}
        
        // PropBot workflow trigger
        document.getElementById('runPropbotBtn').addEventListener('click', function() {
            const button = this;
            const statusMessage = document.getElementById('statusMessage');
            
            // Check if we've run this recently using localStorage
            const lastRun = localStorage.getItem('lastPropbotRun');
            const now = Date.now();
            
            if (lastRun) {
                const daysSinceLastRun = Math.floor((now - parseInt(lastRun)) / (1000 * 60 * 60 * 24));
                if (daysSinceLastRun < 30) {
                    statusMessage.textContent = 'Rental data was updated ' + daysSinceLastRun + ' days ago. No update needed yet.';
                    return;
                }
            }
            
            button.disabled = true;
            button.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Updating...';
            statusMessage.textContent = 'Starting PropBot workflow...';
            
            // Open new window to run the script
            const updateWindow = window.open('run_propbot.html', '_blank', 'width=800,height=600');
            
            // Store the current time in localStorage
            localStorage.setItem('lastPropbotRun', now.toString());
            
            // Reset the button after some time
            setTimeout(() => {
                button.disabled = false;
                button.innerHTML = '<i class="fas fa-sync-alt"></i> Update Property Data';
                statusMessage.textContent = 'Update initiated. Check the new window for progress.';
            }, 2000);
        });
        
        // Initialize everything
        document.addEventListener('DOMContentLoaded', function() {{
            renderPropertiesTable(properties);
            renderPropertyCards(properties);
            setupPagination(properties);
            initClassificationChart();
            renderTopNeighborhoods();
        }});
    </script>
</body>
</html>
'''

# Write the HTML file
with open(html_file, 'w', encoding='utf-8') as f:
    f.write(html_content)
print(f"Successfully generated improved investment dashboard: {html_file}")

# Create or update the last run file with current timestamp
def update_last_run_timestamp():
    with open(last_run_file, 'w') as f:
        f.write(str(time.time()))

print("Dashboard generation complete. Open investment_dashboard.html in your browser to view it.")

# Include a wrapper script for the button
with open('run_propbot.py', 'w') as f:
    f.write('''#!/usr/bin/env python3
import subprocess
import os
import time
import json
from datetime import datetime

LAST_RUN_FILE = "last_rental_run.txt"

def check_last_run():
    try:
        with open(LAST_RUN_FILE, 'r') as f:
            last_run = float(f.read().strip())
            # Check if it's been more than 30 days
            should_run = (time.time() - last_run) >= (30 * 24 * 60 * 60)
            days_since_last_run = int((time.time() - last_run) / (24 * 60 * 60))
            return {
                "shouldRun": should_run,
                "lastRun": datetime.fromtimestamp(last_run).strftime('%Y-%m-%d %H:%M:%S'),
                "daysSinceLastRun": days_since_last_run
            }
    except FileNotFoundError:
        return {
            "shouldRun": True,
            "lastRun": None,
            "daysSinceLastRun": None
        }

def update_last_run():
    with open(LAST_RUN_FILE, 'w') as f:
        f.write(str(time.time()))

def run_propbot_workflow():
    result = subprocess.run(['python3', 'propbot/main.py', '--scrape', '--analyze', '--report'],
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        # Update the last run timestamp
        update_last_run()
        return {"success": True, "message": "PropBot workflow completed successfully"}
    else:
        return {"success": False, "message": f"PropBot workflow failed: {result.stderr}"}

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--check":
        # Just check last run
        print(json.dumps(check_last_run()))
    else:
        # Run the workflow
        print(json.dumps(run_propbot_workflow()))
''')

os.chmod('run_propbot.py', 0o755)  # Make executable 