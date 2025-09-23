import requests
import pandas as pd
from datetime import datetime
import psycopg2

# PostgreSQL connection details
DB_HOST = "postgres"
DB_NAME = "crypto_db"
DB_USER = "airflow"
DB_PASS = "airflow"

def extract(**context):
    """Fetch Bitcoin prices from Coindesk API"""
    url = "https://api.coindesk.com/v1/bpi/currentprice.json"
    response = requests.get(url)
    data = response.json()
    return data  # Sent to XCom

def transform(**context):
    """Convert API JSON into structured records"""
    data = context['ti'].xcom_pull(task_ids='extract_task')

    time_updated = data['time']['updated']
    usd_rate = data['bpi']['USD']['rate_float']
    gbp_rate = data['bpi']['GBP']['rate_float']
    eur_rate = data['bpi']['EUR']['rate_float']

    df = pd.DataFrame([{
        "time": time_updated,
        "USD": usd_rate,
        "GBP": gbp_rate,
        "EUR": eur_rate
    }])

    return df.to_dict(orient="records")  # Save as JSON for XCom

def load(**context):
    """Load transformed records into PostgreSQL"""
    records = context['ti'].xcom_pull(task_ids='transform_task')
    df = pd.DataFrame(records)

    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cur = conn.cursor()

    # Create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crypto_rates (
            id SERIAL PRIMARY KEY,
            time TIMESTAMP,
            usd NUMERIC,
            gbp NUMERIC,
            eur NUMERIC
        )
    """)

    # Insert rows
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO crypto_rates (time, usd, gbp, eur)
            VALUES (%s, %s, %s, %s)
        """, (row['time'], row['USD'], row['GBP'], row['EUR']))

    conn.commit()
    cur.close()
    conn.close()
    print("Data loaded into PostgreSQL")
