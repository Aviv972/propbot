import json
import random
import webbrowser
import time
from datetime import datetime

def log_message(message):
    """Log a message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def open_google_maps(address):
    """Open Google Maps with the given address."""
    base_url = "https://www.google.com/maps/search/?api=1&query="
    query = address.replace(" ", "+") + "+Lisboa+Portugal"
    url = base_url + query
    webbrowser.open(url)
    return url

def validate_location_match(report_file="rental_income_report.json", sample_size=5):
    """
    Sample properties with their comparables and open Google Maps for validation.
    """
    log_message(f"Loading income report from {report_file}")
    
    try:
        with open(report_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Filter to properties that have at least one comparable
        properties_with_comparables = [p for p in data if p.get("comparable_count", 0) > 0]
        log_message(f"Found {len(properties_with_comparables)} properties with comparables")
        
        if len(properties_with_comparables) == 0:
            log_message("No properties with comparables found to validate")
            return
        
        # Sample random properties to check
        sample_size = min(sample_size, len(properties_with_comparables))
        samples = random.sample(properties_with_comparables, sample_size)
        
        log_message(f"Selected {sample_size} random properties for validation")
        
        for i, prop in enumerate(samples):
            print("\n" + "="*80)
            print(f"Property {i+1}/{sample_size}:")
            print(f"Title: {prop['property_title']}")
            print(f"URL: {prop['property_url']}")
            print(f"Location: {prop['property_location']}")
            print(f"Room type: {prop['property_room_type']}, Size: {prop['property_size']} m²")
            print(f"Estimated monthly rent: €{prop['estimated_monthly_rent']:.2f} (based on {prop['comparable_count']} comparables)")
            
            # Open main property in Google Maps
            print("\nOpening main property in Google Maps...")
            main_url = open_google_maps(prop['property_location'])
            print(f"Google Maps URL: {main_url}")
            
            print("\nComparable properties:")
            for j, comp in enumerate(prop['comparables']):
                print(f"\n{j+1}. {comp['location']}")
                print(f"   Room type: {comp['room_type']}, Size: {comp['size_sqm']} m²")
                print(f"   Rent: €{comp['rent_price']:.2f}")
                print(f"   URL: {comp['url']}")
                
                # Wait a moment before opening the next map
                if i > 0 or j > 0:
                    time.sleep(2)  # Avoid opening too many tabs at once
                
                print(f"   Opening comparable in Google Maps...")
                comp_url = open_google_maps(comp['location'])
                print(f"   Google Maps URL: {comp_url}")
            
            # Ask user to continue
            input("\nPress Enter after verifying these locations to continue to the next property...")
    
    except Exception as e:
        log_message(f"Error validating location matching: {str(e)}")

def generate_validation_report(report_file="rental_income_report.json", output_file="location_validation_report.html"):
    """
    Generate an HTML report with links to Google Maps for manual validation.
    This avoids opening many browser tabs at once.
    """
    log_message(f"Generating validation report from {report_file}")
    
    try:
        with open(report_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Filter to properties that have at least one comparable
        properties_with_comparables = [p for p in data if p.get("comparable_count", 0) > 0]
        log_message(f"Found {len(properties_with_comparables)} properties with comparables")
        
        if len(properties_with_comparables) == 0:
            log_message("No properties with comparables found to validate")
            return
        
        # Sample random properties to check
        sample_size = min(10, len(properties_with_comparables))
        samples = random.sample(properties_with_comparables, sample_size)
        
        log_message(f"Selected {sample_size} random properties for validation report")
        
        # Generate HTML report
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Location Matching Validation</title>
            <style>
                body { font-family: Arial, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; }
                .property { border: 1px solid #ccc; margin: 20px 0; padding: 15px; border-radius: 5px; }
                .property-header { background-color: #f5f5f5; padding: 10px; margin-bottom: 15px; }
                .comparable { margin: 15px 0; padding: 10px; border-left: 3px solid #2196F3; background-color: #f9f9f9; }
                .map-link { display: inline-block; margin: 5px 0; padding: 5px 10px; background-color: #4CAF50; color: white; text-decoration: none; border-radius: 3px; }
                .map-link:hover { background-color: #45a049; }
                .details { margin: 10px 0; }
                .property-links { display: flex; gap: 10px; }
            </style>
        </head>
        <body>
            <h1>Location Matching Validation Report</h1>
            <p>This report contains random samples of properties and their comparable rentals for location matching validation.</p>
        """
        
        for i, prop in enumerate(samples):
            html += f"""
            <div class="property">
                <div class="property-header">
                    <h2>Property {i+1}/{sample_size}: {prop['property_title']}</h2>
                    <div class="details">
                        <strong>Location:</strong> {prop['property_location']}<br>
                        <strong>Room type:</strong> {prop['property_room_type']}, <strong>Size:</strong> {prop['property_size']} m²<br>
                        <strong>Estimated monthly rent:</strong> €{prop['estimated_monthly_rent']:.2f} (based on {prop['comparable_count']} comparables)
                    </div>
                    <div class="property-links">
                        <a href="{prop['property_url']}" target="_blank" class="map-link">View Property</a>
                        <a href="https://www.google.com/maps/search/?api=1&query={prop['property_location'].replace(' ', '+')}+Lisboa+Portugal" target="_blank" class="map-link">View on Google Maps</a>
                    </div>
                </div>
                
                <h3>Comparable properties:</h3>
            """
            
            for j, comp in enumerate(prop['comparables']):
                html += f"""
                <div class="comparable">
                    <div class="details">
                        <strong>{j+1}. {comp['location']}</strong><br>
                        <strong>Room type:</strong> {comp['room_type']}, <strong>Size:</strong> {comp['size_sqm']} m²<br>
                        <strong>Rent:</strong> €{comp['rent_price']:.2f}
                    </div>
                    <div class="property-links">
                        <a href="{comp['url']}" target="_blank" class="map-link">View Rental</a>
                        <a href="https://www.google.com/maps/search/?api=1&query={comp['location'].replace(' ', '+')}+Lisboa+Portugal" target="_blank" class="map-link">View on Google Maps</a>
                    </div>
                </div>
                """
            
            html += "</div>"
        
        html += """
        </body>
        </html>
        """
        
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html)
            
        log_message(f"Validation report saved to {output_file}")
        
    except Exception as e:
        log_message(f"Error generating validation report: {str(e)}")

if __name__ == "__main__":
    log_message("Location matching validation tool")
    print("Select a validation method:")
    print("1. Interactive validation (opens browser tabs)")
    print("2. Generate HTML report (recommended)")
    
    choice = input("Enter your choice (1 or 2): ")
    
    if choice == "1":
        sample_size = input("How many properties to check? (default: 5): ")
        try:
            sample_size = int(sample_size)
        except:
            sample_size = 5
        
        validate_location_match(sample_size=sample_size)
    elif choice == "2":
        generate_validation_report()
        log_message("Report generated. Open location_validation_report.html in your browser to validate the matches.")
    else:
        log_message("Invalid choice. Exiting.") 