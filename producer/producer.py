import json
import time
import random
import uuid
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TRANSACTION_TYPES = ['deposit', 'withdrawal', 'transfer', 'card_payment']
MERCHANTS = ['Amazon', 'Starbucks', 'Walmart', 'Shell', 'Netflix', 'Uber', 'Local ATM', '7-eleven', 'Lazada', 'Shoppee']

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"ACC{random.randint(1000, 1050)}",
        "amount": round(random.uniform(5, 15000), 2),
        "type": random.choice(TRANSACTION_TYPES),
        "merchant": random.choice(MERCHANTS),
        "location": fake.city(),
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print("Starting producer... sending to topic 'bank-transactions'")
    while True:
        txn = generate_transaction()
        producer.send('bank-transactions', key=txn['account_id'], value=txn)
        print(f"Sent to partition key={txn['account_id']}: {txn}")
        time.sleep(random.uniform(0.5, 2))
