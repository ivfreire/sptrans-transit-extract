# SPTrans Transit Extract

An ETL pipeline for extracting, transforming, and loading real-time bus position data from the SPTrans (São Paulo Public Transportation System) open API. This project provides both a data processing pipeline and a FastAPI web service for accessing bus positioning information.

## Overview

This application extracts georeferenced bus information from the SPTrans Olho Vivo API, processes the data, and makes it available through multiple channels:
- **Google Cloud Pub/Sub**: For real-time streaming data
- **Google BigQuery**: For data warehousing and analytics
- **FastAPI endpoint**: For web-based access to the data

## Features

- 🚌 **Real-time bus positioning**: Extracts current positions of all São Paulo public buses
- 🔄 **Data transformation**: Normalizes and enriches raw API data
- ☁️ **Cloud integration**: Publishes to Google Cloud Pub/Sub and BigQuery
- 🌐 **Web API**: FastAPI endpoint for accessing bus position data
- 🐳 **Containerized**: Ready-to-deploy Docker container
- 📊 **Structured data**: Exports data with standardized schema

## Architecture

```
SPTrans Olho Vivo API → Data Processing → Multiple Outputs
                            ↓
                    [Extract, Transform, Load]
                            ↓
        ┌─────────────────┬─────────────────┬─────────────────┐
        ↓                 ↓                 ↓
   Pub/Sub Topic     BigQuery Table    FastAPI Endpoint
   (Real-time)       (Data Warehouse)   (Web Access)
```

## Data Schema

The processed data includes the following fields:

| Field | Type | Description |
|-------|------|-------------|
| `ingestion_time` | datetime | When the data was processed |
| `route_id` | string | Bus route identifier |
| `trip_code` | string | Specific trip code |
| `direction_id` | integer | Direction of travel (1 or 2) |
| `bus_prefix` | string | Bus vehicle identifier |
| `is_accessible` | boolean | Whether the bus is wheelchair accessible |
| `timestamp` | datetime | When the position was recorded |
| `lat` | float | Latitude coordinate |
| `lon` | float | Longitude coordinate |

## Prerequisites

- Python 3.11+
- Google Cloud Platform account (for Pub/Sub and BigQuery)
- GCP service account with appropriate permissions:
  - Pub/Sub Publisher
  - BigQuery Data Editor

## Installation

### Local Development

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd sptrans-transit-extract
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up Google Cloud credentials**:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
   ```

4. **Run the application**:
   ```bash
   # Run the ETL pipeline directly
   python src/bus.py
   
   # Or start the FastAPI server
   uvicorn main:app --host 0.0.0.0 --port 8000
   ```

### Docker Deployment

1. **Build the Docker image**:
   ```bash
   docker build -t sptrans-transit-extract .
   ```

2. **Run the container**:
   ```bash
   docker run -p 8080:8080 \
     -v /path/to/service-account.json:/app/credentials.json \
     -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
     sptrans-transit-extract
   ```

## API Endpoints

### GET `/positions`

Extracts and processes current bus position data from SPTrans API.

**Response**: Triggers the ETL pipeline and returns the processing result.

**Example**:
```bash
curl http://localhost:8080/positions
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP service account key | Required |
| `GCP_PROJECT_ID` | Google Cloud Project ID | `sptransit` |
| `PUBSUB_TOPIC` | Pub/Sub topic name | `sptrans-transit-positions` |
| `BIGQUERY_DATASET` | BigQuery dataset | `transit` |
| `BIGQUERY_TABLE` | BigQuery table | `bus_position` |

### Data Processing Configuration

The application processes data in configurable shards for efficient Pub/Sub publishing:

- **SHARDS_COUNT**: Number of data shards for Pub/Sub (default: 10)
- **BASE_URL**: SPTrans API base URL
- **EXPORT_COLUMNS**: Specific columns to export to BigQuery

## Google Cloud Setup

### 1. Create Pub/Sub Topic

```bash
gcloud pubsub topics create sptrans-transit-positions
```

### 2. Create BigQuery Dataset and Table

```sql
-- Create dataset
CREATE SCHEMA IF NOT EXISTS `sptransit.transit`;

-- Create table
CREATE TABLE IF NOT EXISTS `sptransit.transit.bus_position` (
  ingestion_time DATETIME,
  route_id STRING,
  trip_code STRING,
  direction_id INTEGER,
  bus_prefix STRING,
  is_accessible BOOLEAN,
  timestamp DATETIME,
  lat FLOAT64,
  lon FLOAT64
);
```

### 3. Service Account Permissions

Ensure your service account has the following IAM roles:
- `roles/pubsub.publisher`
- `roles/bigquery.dataEditor`
- `roles/bigquery.user`

## Usage Examples

### Programmatic Access

```python
from src.bus import main as bus_main

# Run the ETL pipeline
bus_main()
```

### API Access

```python
import requests

# Trigger data extraction via API
response = requests.get('http://localhost:8080/positions')
print(response.status_code)
```

### Scheduled Execution

For regular data extraction, consider using:
- **Cloud Scheduler** (GCP)
- **Cron jobs** (Linux/Unix)
- **GitHub Actions** (CI/CD)

Example cron job for hourly execution:
```bash
0 * * * * curl http://your-app-url/positions
```

## Development

### Project Structure

```
sptrans-transit-extract/
├── main.py              # FastAPI application entry point
├── src/
│   ├── __init__.py
│   └── bus.py           # Core ETL logic
├── requirements.txt     # Python dependencies
├── Dockerfile          # Container configuration
└── README.md           # This file
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Monitoring and Logging

The application includes basic logging and error handling:
- HTTP status code validation
- JSON parsing error handling
- Data validation before cloud uploads
- Pub/Sub publish confirmation

For production deployments, consider adding:
- Structured logging (JSON format)
- Application monitoring (Cloud Monitoring)
- Alert policies for failures
- Data quality checks

## Troubleshooting

### Common Issues

1. **Authentication Error**:
   ```
   Error: Could not automatically determine credentials
   ```
   **Solution**: Ensure `GOOGLE_APPLICATION_CREDENTIALS` is set correctly.

2. **API Request Failed**:
   ```
   Failed to fetch data: 500
   ```
   **Solution**: Check SPTrans API status and network connectivity.

3. **BigQuery Permission Denied**:
   ```
   Permission denied on dataset
   ```
   **Solution**: Verify service account has BigQuery Data Editor role.

### Debug Mode

Enable debug logging by modifying the logging configuration in `src/bus.py`.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- **SPTrans** for providing the open Olho Vivo API
- **Google Cloud Platform** for cloud infrastructure services
- **FastAPI** for the modern web framework

---

For questions or support, please open an issue in the repository.