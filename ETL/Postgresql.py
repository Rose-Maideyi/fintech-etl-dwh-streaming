import psycopg2
import pandas as pd

# PostgreSQL connection settings
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="fintech",   # Make sure this DB exists
    user="postgres",
    password="rose"
)
cur = conn.cursor()

# Load CSV file
df = pd.read_csv(r"C:\Users\rosep\Downloads\customers (2).csv")  # Make sure this file exists in the same folder

# Ensure required columns exist
required_columns = ["full_name", "email", "phone", "created_at"]
for col in required_columns:
    if col not in df.columns:
        raise ValueError(f"Missing required column: {col}")

# Convert 'created_at' to datetime
df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

# Create table (if not exists)
cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id SERIAL PRIMARY KEY,
        full_name VARCHAR(100),
        email VARCHAR(150),
        phone VARCHAR(50),
        created_at TIMESTAMP
    );
""")
conn.commit()

# Insert each row
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO customers (full_name, email, phone, created_at)
        VALUES (%s, %s, %s, %s);
    """, (
        row["full_name"],
        row["email"],
        row["phone"],
        row["created_at"]
    ))

conn.commit()
cur.close()
conn.close()

print("✅ CSV data loaded successfully into PostgreSQL.")
