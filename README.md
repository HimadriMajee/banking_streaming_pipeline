# Real-Time Banking Transaction Pipeline

A self-initiated data engineering project simulating a bank's real-time transaction stream using Apache Kafka and AWS services (S3, Lambda, DynamoDB, Athena).

## Status: In Progress

## Architecture
[Python Producer: Fake Transactions] <br>
        ↓ <br>
   [Apache Kafka / Amazon MSK]  →  [Kafka Consumer → S3 raw zone] <br>
        ↓ <br>
[Stream Processor: Kafka Streams / Flink / Kinesis Data Analytics] <br>
   → real-time fraud rules, running aggregates <br>
        ↓ <br>
   [S3 processed zone] → [AWS Glue Crawler + ETL Jobs] <br>
        ↓ <br>
   [Athena / Redshift Spectrum] → [QuickSight Dashboard] <br>
        ↓ <br>
[Orchestration: Airflow (MWAA) or Step Functions]<br>
[IaC: Terraform]   [CI/CD: GitHub Actions]   [Monitoring: CloudWatch]<br>

## Tech Stack
- Apache Kafka (Docker)
- Python (producer/consumer, boto3)
- AWS S3, Lambda, DynamoDB, Athena
- Streamlit (dashboard)

## How to Run
(instructions coming soon)
