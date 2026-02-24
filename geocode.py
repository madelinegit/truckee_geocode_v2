import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

INPUT_FILE = "data/properties_raw.csv"
OUTPUT_FILE = "data/properties_geocoded_clean.csv"
FAIL_FILE = "data/geocode_failures.csv"

# Truckee bounding box
MIN_LAT = 39.28
MAX_LAT = 39.42
MIN_LNG = -120.30
MAX_LNG = -120.05

SLEEP_TIME = 0.2


def geocode_address(address):
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": address,
        "key": API_KEY,
        "region": "us",
        "components": "country:US"
    }

    response = requests.get(url, params=params)
    data = response.json()

    if data["status"] != "OK":
        return None, None, f"API_{data['status']}"

    result = data["results"][0]

    location_type = result["geometry"]["location_type"]
    if location_type == "APPROXIMATE":
        return None, None, "LOW_PRECISION"

    lat = result["geometry"]["location"]["lat"]
    lng = result["geometry"]["location"]["lng"]

    if not (MIN_LAT <= lat <= MAX_LAT and MIN_LNG <= lng <= MAX_LNG):
        return None, None, "OUTSIDE_TRUCKEE"

    return lat, lng, None


def main():
    df = pd.read_csv(INPUT_FILE)

    # Remove old coordinate columns completely
    df = df.drop(columns=[col for col in df.columns if col.lower() in ["latitude", "longitude"]], errors="ignore")

    df["Latitude"] = None
    df["Longitude"] = None

    failures = []

    for index, row in df.iterrows():
        address = row["Unit Address"]

        print(f"Geocoding: {address}")

        lat, lng, error = geocode_address(address)

        if lat and lng:
            df.at[index, "Latitude"] = lat
            df.at[index, "Longitude"] = lng
            df.to_csv(OUTPUT_FILE, index=False)
            print("Saved.")
        else:
            failures.append({
                "Unit Address": address,
                "Error": error
            })
            print(f"Failed: {error}")

        time.sleep(SLEEP_TIME)

    if failures:
        pd.DataFrame(failures).to_csv(FAIL_FILE, index=False)

    print("Done.")


if __name__ == "__main__":
    main()