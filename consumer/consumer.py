import json
import time
from datetime import datetime
from kafka import KafkaConsumer
import boto3

BUCKET_NAME = "banking-streaming-pipeline-raw"
BATCH_SIZE = 10
BATCH_INTERVAL_SECONDS = 30

s3 = boto3.client('s3')

consumer = KafkaConsumer(
    'bank-transactions',
    bootstrap_servers='localhost:9092',
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='s3-writer-group'
)

def flush_batch(batch):
    if not batch:
        return
    now = datetime.utcnow()
    key = f"raw/year={now.year}/month={now.month:02d}/day={now.day:02d}/transactions_{now.strftime('%Y%m%dT%H%M%S')}.json"
    body = "\n".join(json.dumps(record) for record in batch)
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=body.encode('utf-8'))
    print(f"Flushed {len(batch)} records to s3://{BUCKET_NAME}/{key}")

if __name__ == "__main__":
    print("Starting consumer... reading from topic 'bank-transactions'")
    batch = []
    last_flush = time.time()

    try:
        for message in consumer:
            batch.append(message.value)
            print(f"Consumed from partition {message.partition}: {message.value['transaction_id']}")

            if len(batch) >= BATCH_SIZE or (time.time() - last_flush) >= BATCH_INTERVAL_SECONDS:
                flush_batch(batch)
                batch = []
                last_flush = time.time()
    except KeyboardInterrupt:
        print("\nStopping consumer, flushing remaining batch...")
        flush_batch(batch)
