import json
import boto3
from collections import Counter
from datetime import datetime

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('fraud-alerts')

PROCESSED_BUCKET = "banking-streaming-pipeline-raw"
FRAUD_AMOUNT_THRESHOLD = 10000
VELOCITY_THRESHOLD = 3

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        print(f"Processing s3://{bucket}/{key}")

        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        transactions = [json.loads(line) for line in content.strip().split('\n') if line]

        account_counts = Counter(txn['account_id'] for txn in transactions)

        flagged = []
        for txn in transactions:
            reasons = []
            if txn['amount'] > FRAUD_AMOUNT_THRESHOLD:
                reasons.append(f"amount_exceeds_{FRAUD_AMOUNT_THRESHOLD}")
            if account_counts[txn['account_id']] >= VELOCITY_THRESHOLD:
                reasons.append(f"velocity_{account_counts[txn['account_id']]}_txns_in_batch")

            if reasons:
                txn['flag_reasons'] = reasons
                flagged.append(txn)
                table.put_item(Item=txn)

        now = datetime.utcnow()
        processed_key = key.replace('raw/', 'processed/')
        body = "\n".join(json.dumps(txn) for txn in transactions)
        s3.put_object(Bucket=PROCESSED_BUCKET, Key=processed_key, Body=body.encode('utf-8'))

        print(f"Processed {len(transactions)} transactions, flagged {len(flagged)}, wrote to s3://{PROCESSED_BUCKET}/{processed_key}")

    return {"statusCode": 200, "body": f"Processed {len(event['Records'])} file(s)"}
