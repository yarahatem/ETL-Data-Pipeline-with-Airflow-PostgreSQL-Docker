# ETL-Data-Pipeline-with-Airflow-PostgreSQL-Docker
Developed a containerized ETL pipeline that extracts cryptocurrency prices from an API, transforms the data, and loads it into PostgreSQL. Automated workflows with Apache Airflow and deployed the entire system using Docker for reproducibility and scalability.

## Build the Docker Image
```bash
docker build -t my-airflow:latest .
docker-compose up --build
