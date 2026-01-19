import csv
import os
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/weatherdb")
client = MongoClient(MONGO_URI)
db= client.get_database()
col = db ["observations"]

CSV_PATH = "./data/GlobalWeatherRepository.csv"

def safe_float(x):
    try:
        return float(x)
    except:
        return None
    
def main():
    inserted = 0
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            city = row.get("location_name")
            date = (row.get("last_updated"))[:10]

            if not city or len(date) != 10:
                continue

            doc={
                "city": city,
                "date": date,
                "temperature": safe_float(row.get("temperature_celsius")),
                "humidity": safe_float(row.get("humidity")),
                "precipitation": safe_float(row.get("precip_mm")),
                "source": "kaggle-global-weather-repository"
            }
            col.insert_one(doc)
            inserted += 1

    print (f"Inserted: {inserted}")

if __name__ == "__main__":
    main()