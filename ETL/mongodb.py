from pymongo import MongoClient
from faker import Faker
from datetime import datetime
import random

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["fintech_mongo"]
collection = db["transactions"]

# Use Faker for fake data
fake = Faker()

# Clear existing data (optional)
collection.delete_many({})

# Generate and insert data
data = []
for _ in range(100):
    doc = {
        "customer_name": fake.name(),
        "email": fake.email(),
        "account_number": fake.bban(),
        "transaction_type": random.choice(["Deposit", "Withdrawal", "Transfer"]),
        "amount": round(random.uniform(10, 5000), 2),
        "currency": random.choice(["USD", "ZWL", "EUR"]),
        "timestamp": datetime.utcnow(),
        "branch": {
            "name": random.choice(["Harare CBD", "Borrowdale", "Avondale"]),
            "city": "Harare",
            "country": "Zimbabwe"
        }
    }
    data.append(doc)

collection.insert_many(data)

print("✅ 100 fake financial documents inserted into MongoDB.")
