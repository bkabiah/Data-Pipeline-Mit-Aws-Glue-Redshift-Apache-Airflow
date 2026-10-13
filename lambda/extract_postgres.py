import psycopg2
import json
import boto3
import os
from datetime import datetime

s3 = boto3.client('s3')
BUCKET = os.environ.get('S3_BUCKET', 'ecom-raw-data-bucket-[dein-name]')

def lambda_handler(event, context):
    # Verbindung zu PostgreSQL (Nutze AWS Secrets Manager in Produktion!)
    conn = psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT']
    )
    cur = conn.cursor()
    
    # Prüfe ob Full oder Incremental Load (basierend auf Event)
    load_type = event.get('load_type', 'incremental')
    
    if load_type == 'full':
        cur.execute("SELECT customer_id, name, email, created_at FROM customers;")
    else:
        cur.execute("SELECT customer_id, name, email, created_at FROM customers WHERE created_at >= CURRENT_DATE - INTERVAL '1 day';")
        
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    
    # In JSON umwandeln
    data = [dict(zip(columns, row)) for row in rows]
    
    # Nach S3 schreiben
    key = f"postgres_customers/{datetime.now().strftime('%Y/%m/%d')}/data.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data, default=str))
    
    cur.close()
    conn.close()
    
    return {'statusCode': 200, 'body': json.dumps({'records_processed': len(data)})}
