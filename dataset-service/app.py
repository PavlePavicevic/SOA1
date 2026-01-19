import os
from datetime import datetime
from flask import Flask, request, jsonify
from pymongo import MongoClient
from bson import ObjectId

app= Flask(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/weatherdb")
client = MongoClient(MONGO_URI)
db = client.get_database()
col = db["observations"]

col.create_index([("city", 1), ("date", 1)])

def to_json(doc):
    doc["_id"]= str(doc["_id"])
    return doc

def parse_date(s: str):
    return datetime.strptime(s, "%Y-%m-%d").date().isoformat()


@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/observations")
def create_observation():
    data = request.get_json(force=True)

    for key in ["city", "date"]:
        if key not in data:
            return {"error": f"Nepostojeće polje: {key}"}, 400

    doc= {
        "city": data["city"],
        "date": parse_date(data["date"]),
        "temperature": data.get("temperature"),
        "humidity": data.get("humidity"),
        "precipitation": data.get("precipitation"),
        "source": data.get("source", "kaggle-global-weather-repository"),
    }

    res = col.insert_one(doc)
    doc["_id"]= str(res.inserted_id)
    return jsonify(doc), 201


@app.get("/observations/<id>")
def get_observation(id):
    doc= col.find_one({"_id": ObjectId(id)})
    if not doc:
        return {"Greška": "Nije pronađen"}, 404
    return jsonify(to_json(doc))

@app.put("/observations/<id>")
def update_observation(id):
    data = request.get_json(force=True)

    update={}
    for field in ["city", "date", "temperature", "humidity", "precipitation", "source"]:
        if field in data:
            update[field] = parse_date(data[field]) if field == "date" else data[field]

    if not update:
        return {"Greška": "Nema polja za ažuriranje"}, 400

    res = col.update_one({"_id": ObjectId(id)}, {"$set":update})
    if res.matched_count == 0:
        return {"error" : "Nije pronađen"}, 404

    doc = col.find_one({"_id": ObjectId(id)})
    return jsonify(to_json(doc))

@app.delete("/observations/<id>")
def delete_observation(id):
    res = col.delete_one({"_id":ObjectId(id)})
    if res.deleted_count == 0:
        return {"Greška": "Nije pronađen"}, 404
    return {"deleted": True}


@app.get("/observations")
def search_observations():
    """
    GET /observations?city=Belgrade&from=2025-01-01to=2025-01-31&limit=200
    """
    city = request.args.get("city")
    date_from = request.args.get("from")
    date_to = request.args.get("to")
    limit = int(request.args.get("limit", "200"))

    q= {}
    if city:
        q["city"]= city

    if date_from or date_to:
        q["date"]= {}
        if date_from:
            q["date"]["$gte"] = parse_date(date_from)
        if date_to:
            q["date"]["$lte"] = parse_date(date_to)

    docs = list(col.find(q).sort("date", 1).limit(limit))
    return jsonify([to_json(d) for d in docs])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
