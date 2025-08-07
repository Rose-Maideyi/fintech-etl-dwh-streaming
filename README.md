 fintech-etl-dwh-streaming



 🧩 Overview



This project showcases a complete data engineering solution for financial services, comprising:



ETL Pipelines from MongoDB and PostgreSQL to a PostgreSQL Data Warehouse (Star Schema

Data Warehouse Design optimized for reporting and analytics

Real-Time Data Streaming using Python, Apache Kafka, Spark, and InfluxDB with Grafana



---



 🏗️ Architecture Summary


 1. ETL Pipeline (Batch)



Sources:



&nbsp;  MongoDB: Transactional data

&nbsp; PostgreSQL: Customer data



Destination:



&nbsp; PostgreSQL Data Warehouse



Key Features:



&nbsp;  Data transformation and cleaning

&nbsp;  Star schema alignment

&nbsp;  YAML-based configuration

&nbsp;  Scheduling via APScheduler



2. Data Warehouse



Schema: Star schema

Fact Tables: `fact\_transactions`, `fact\_account\_balances`

Dimension Tables: `dim\_customer`, `dim\_account`, `dim\_date`, `dim\_branch`, `dim\_transaction\_type`

Security: Role-based access

Quality Checks: Negative balances, missing keys, orphan records



 3. Real-Time Streaming Pipeline



Tools: Python, Flume, Kafka, Spark, InfluxDB, Grafana

Features:



&nbsp;  Live financial transaction simulation

&nbsp;  Real-time analytics with InfluxDB + Grafana dashboards



---



 ⚙️ Components \& Setup



 ✅ ETL Pipeline



Configuration

Create a `config.yaml`:



```yaml

mongo:

&nbsp; host: localhost

&nbsp; port: 27017

&nbsp; database: your\_mongo\_db

&nbsp; collection: your\_transaction\_collection



postgresql:

&nbsp; host: localhost

&nbsp; port: 5432

&nbsp; database: your\_source\_db

&nbsp; user: your\_pg\_user

&nbsp; password: your\_pg\_password



warehouse:

&nbsp; host: localhost

&nbsp; port: 5432

&nbsp; database: your\_warehouse\_db

&nbsp; user: your\_dw\_user

&nbsp; password: your\_dw\_password

```



Run ETL Once:



```bash

python etl\_pipeline.py

```



\*\*Or Schedule Every Hour\*\*:



```python

run\_scheduled\_etl(minutes=60)

```





✅ Data Warehouse Setup



Create Database:

sql

CREATE DATABASE financial\_dw;

\\c financial\_dw;





Run SQL Schema:



```bash

psql -U your\_user -f financial\_dw\_star\_schema.sql

```



Sample Query: Customer Portfolio



```sql

SELECT 

&nbsp;   c.customer\_id,

&nbsp;   c.last\_name || ', ' || c.first\_name as customer\_name,

&nbsp;   COUNT(DISTINCT a.account\_id) as account\_count,

&nbsp;   SUM(fab.balance) as total\_balance

FROM dim\_customer c

JOIN dim\_account a ON c.customer\_id = a.customer\_id

JOIN fact\_account\_balances fab ON a.account\_id = fab.account\_id

WHERE fab.date\_id = (SELECT date\_id FROM dim\_date WHERE full\_date = CURRENT\_DATE)

GROUP BY c.customer\_id, customer\_name

ORDER BY total\_balance DESC

LIMIT 100;

```



---



✅ Real-Time Streaming Pipeline



Data Generator Script (Python)



```python

 generator.py

import json, time, random, os

from datetime import datetime



def generate\_transaction():

&nbsp;   return {

&nbsp;       "transaction\_id": f"T{random.randint(100000, 999999)}",

&nbsp;       "account\_id": f"A{random.randint(100000, 999999)}",

&nbsp;       "transaction\_type": random.choice(\["deposit", "withdrawal", "payment"]),

&nbsp;       "amount": round(random.uniform(100.0, 5000.0), 2),

&nbsp;       "currency": "USD",

&nbsp;       "transaction\_date": datetime.utcnow().isoformat() + "Z",

&nbsp;       "branch": "Main Street",

&nbsp;       "customer\_age": random.randint(18, 80),

&nbsp;       "transaction\_status": "completed"

&nbsp;   }



while True:

&nbsp;   with open("transactions.json", "a") as f:

&nbsp;       f.write(json.dumps(generate\_transaction()) + "\\n")

&nbsp;   time.sleep(1)

```



Kafka Setup:



```bash

kafka-topics.sh --create --topic finance --bootstrap-server localhost:9092

```



Spark Consumer Script\*\*:



```python

spark\_consumer.py

from kafka import KafkaConsumer

from influxdb\_client import InfluxDBClient, Point

from influxdb\_client.client.write\_api import SYNCHRONOUS

import json



consumer = KafkaConsumer('finance', bootstrap\_servers='localhost:9092',

&nbsp;                        value\_deserializer=lambda x: json.loads(x.decode('utf-8')))

client = InfluxDBClient(url="https://your-url", token="your-token", org="your-org")

write\_api = client.write\_api(write\_options=SYNCHRONOUS)



for msg in consumer:

&nbsp;   data = msg.value

&nbsp;   point = Point("transactions").tag("type", data\["transaction\_type"]) \\

&nbsp;                                .field("amount", float(data\["amount"])) \\

&nbsp;                                .time(data\["transaction\_date"])

&nbsp;   write\_api.write(bucket="finance", org="your-org", record=point)

```



Grafana Setup:



 Connect InfluxDB as a data source

 Visualize metrics like:



&nbsp; Total transaction volume

&nbsp;  Status breakdown

&nbsp;  Age distribution



---



🔐 Security \& Governance



 Roles: `finance\_analyst`, `finance\_manager`, `finance\_admin`

Access control via PostgreSQL GRANTs

 Masked columns (e.g., SSNs)
 Data validation: Nulls, negative values, key references



---



 📦 Requirements



Python 3.7+

 PostgreSQL 12+

 MongoDB

 Kafka, Spark, Flume (for real-time)
 Python packages:



&nbsp; ```bash

&nbsp; pip install pymongo psycopg2 sqlalchemy pandas PyYAML APScheduler influxdb-client kafka-python

&nbsp; 






🛠️ Future Enhancements



Introduce incremental ETL loading

 Integrate a CI/CD pipeline for deployments

 Add support for S3 or BigQuery as alternate warehouses
 Extend real-time alerts via Grafana or Prometheus



---





