# NetPulse – Real-Time Telecom Network Monitoring & Alerting System

A production-style real-time Data Engineering platform designed to monitor telecom network infrastructure, detect anomalies in real time, and provide operational visibility through streaming analytics and live dashboards.

## Tech Stack

- Python
- Apache Kafka
- Apache Spark Structured Streaming
- PostgreSQL
- Apache Airflow
- Grafana
- Docker

## Architecture

                     NetPulse
       Real-Time Telecom Monitoring Platform

           Python Event Simulator
                    |
                    v
              Apache Kafka
                    |
        +-----------+-----------+
        |                       |
        v                       v
   Raw Ingestion          Alert Engine
   Spark Streaming      Spark Streaming
        |                       |
        +-----------+-----------+
                    |
                    v
               PostgreSQL
               /         \
              /           \
             v             v
        Apache Airflow   Grafana

## Features

- Real-time telemetry processing
- Rule-based anomaly detection
- Automated alerting
- Historical data storage
- Health monitoring
- Daily aggregation and reporting
- Live Grafana dashboards

## Project Metrics

| Metric | Value |
|---|---|
| Simulated Devices | 26 |
| Docker Containers | 12 |
| Kafka Topics | 1 |
| Spark Jobs | 2 |
| PostgreSQL Tables | 5 |
| Alert Rules | 6 |
| Airflow DAGs | 2 |
| Grafana Panels | 15 |
