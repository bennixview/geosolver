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

def calculate_distances(list_a, list_b):
    """Calculate distances and find the closest matches."""
    results = []
    for _, row_a in tqdm(list_a.iterrows(), desc="Matching Locations", total=len(list_a)):
        coord_a = (row_a["Latitude"], row_a["Longitude"])
        closest_match = None
        min_distance = float("inf")
        
        for _, row_b in list_b.iterrows():
            coord_b = (row_b["Latitude"], row_b["Longitude"])
            if not (isnan(coord_a[0]) or isnan(coord_a[1]) or isnan(coord_b[0]) or isnan(coord_b[1])):
                distance = geodesic(coord_a, coord_b).kilometers
                if distance < min_distance:
                    min_distance = distance
                    closest_match = row_b
            else:
                print(f"Ungültige Koordinaten gefunden: {coord_a}, {coord_b}")
                
        if closest_match is not None:
            results.append({
                "Address from List A": row_a["Full Address"],
                "Location from List B": f"{closest_match['City']} {closest_match['Zip Code']}",
                "Distance (km)": min_distance
            })
    return pd.DataFrame(results)

def save_results(results, output_file):
    """Save the results to a CSV file."""
    results.to_csv(output_file, index=False)

def main():
    # Input and Output Files
    list_a_file = "list_a.csv"
    list_b_file = "list_b.csv"
    output_file = "matched_locations.csv"

    # Step 1: Load Data
    list_a, list_b = load_data(list_a_file, list_b_file)

    # Step 2: Preprocess and Geocode
    list_a, list_b = preprocess_and_geocode(list_a, list_b)

    # Step 3: Calculate Distances and Find Matches
    results = calculate_distances(list_a, list_b)

    # Step 4: Save Results
    save_results(results, output_file)
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    main()
