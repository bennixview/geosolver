import pandas as pd
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from tqdm import tqdm
from math import isnan

def load_data(list_a_file, list_b_file):
    """Load data from CSV files."""
    list_a = pd.read_csv(list_a_file)
    list_b = pd.read_csv(list_b_file)
    return list_a, list_b

def construct_full_address(row):
    """Construct the full address from city, street, house number, and zip code."""
    return f"{row['street']} {row['house number']}, {row['zip code']} {row['city']}"

def geocode_address(geolocator, address):
    """Geocode an address using Nominatim."""
    try:
        location = geolocator.geocode(address)
        if location:
            return location.latitude, location.longitude
    except Exception as e:
        print(f"Error geocoding {address}: {e}")
    return None, None

def preprocess_and_geocode(list_a, list_b):
    """Add latitude and longitude to the data."""
    geolocator = Nominatim(user_agent="location-matcher")

    # Construct full address for List A
    list_a["Full Address"] = list_a.apply(construct_full_address, axis=1)
    
    # Geocode List A
    tqdm.pandas(desc="Geocoding List A")
    list_a["Coordinates"] = list_a["Full Address"].progress_apply(lambda x: geocode_address(geolocator, x))
    list_a[["Latitude", "Longitude"]] = pd.DataFrame(list_a["Coordinates"].tolist(), index=list_a.index)
    
    # Construct simplified address for List B
    list_b["Simplified Address"] = list_b["City"] + ", " + list_b["Zip Code"].astype(str)

    # Geocode List B
    tqdm.pandas(desc="Geocoding List B")
    list_b["Coordinates"] = list_b["Simplified Address"].progress_apply(lambda x: geocode_address(geolocator, x))
    list_b[["Latitude", "Longitude"]] = pd.DataFrame(list_b["Coordinates"].tolist(), index=list_b.index)

    return list_a, list_b

def calculate_distances(stock_locations, customer_locations):
    """Calculate distances and find the closest stock location for each customer."""
    results = []
    for _, customer in tqdm(customer_locations.iterrows(), desc="Matching Customers", total=len(customer_locations)):
        coord_customer = (customer["Latitude"], customer["Longitude"])
        closest_stock = None
        min_distance = float("inf")
        
        for _, stock in stock_locations.iterrows():
            coord_stock = (stock["Latitude"], stock["Longitude"])
            if not (isnan(coord_customer[0]) or isnan(coord_customer[1]) or isnan(coord_stock[0]) or isnan(coord_stock[1])):
                distance = geodesic(coord_customer, coord_stock).kilometers
                if distance < min_distance:
                    min_distance = distance
                    closest_stock = stock
            else:
                print(f"Ungültige Koordinaten gefunden: {coord_customer}, {coord_stock}")
        
        
        if closest_stock is not None:
            results.append({
                "Customer Location": f"{customer['City']} {customer['Zip Code']}",
                "Closest Stock Location": closest_stock["Full Address"],
                "Distance (km)": min_distance
            })
    return pd.DataFrame(results)

def save_results(results, output_file):
    """Save the results to a CSV file."""
    results.to_csv(output_file, index=False)

def main():
    # Input and Output Files
    stock_file = "list_a.csv"
    customer_file = "list_b.csv"
    output_file = "matched_customers_to_stocks.csv"

    # Step 1: Load Data
    stock_locations, customer_locations = load_data(stock_file, customer_file)

    # Step 2: Preprocess and Geocode
    stock_locations, customer_locations = preprocess_and_geocode(stock_locations, customer_locations)

    # Step 3: Calculate Distances and Find Closest Stock for Each Customer
    results = calculate_distances(stock_locations, customer_locations)

    # Step 4: Save Results
    save_results(results, output_file)
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    main()
