# Weather Data Processing Platform

A robust weather data processing platform that ingests, stores, and analyzes weather data using Kafka, Cassandra, and Spark. This platform is designed to handle real-time weather data streams, store them efficiently, and perform analytics and machine learning tasks. This project is a combination of multiple topics I learned in COMP SCI 544 (Big Data Systems) at UW-Madison.

## Features

- **Data Ingestion**: Real-time weather data ingestion using Kafka
- **Data Storage**: Efficient storage using Cassandra with configurable replication
- **Data Processing**: Analytics and machine learning using Spark
- **Caching**: LRU cache for frequently accessed queries
- **Configuration**: Flexible configuration system with JSON support
- **Error Handling**: Comprehensive error handling and logging
- **Resource Management**: Proper cleanup of resources

## Prerequisites

- Python 3.7+
- Kafka server
- Cassandra cluster
- Spark installation
- Required Python packages (see `requirements.txt`)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd weather-platform
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Set up your environment:
   - Ensure Kafka is running and accessible
   - Ensure Cassandra cluster is running and accessible
   - Ensure Spark is properly configured

## Configuration

Create a `config.json` file with your specific settings:

```json
{
    "kafka": {
        "bootstrap_servers": ["localhost:9092"],
        "topic": "temperatures",
        "retries": 10,
        "retry_backoff_ms": 500
    },
    "cassandra": {
        "contact_points": ["localhost"],
        "port": 9042,
        "keyspace": "weather",
        "username": null,
        "password": null,
        "replication_factor": 3
    },
    "spark": {
        "app_name": "weather_platform",
        "executor_memory": "512M"
    },
    "cache": {
        "capacity": 10
    }
}
```

## Usage

### Running the Platform

1. **Data Ingestion**:
```bash
python weather_platform.py --config config.json --ingest --max-records 1000
```

2. **Data Processing**:
```bash
python weather_platform.py --config config.json --process
```

3. **Both Ingestion and Processing**:
```bash
python weather_platform.py --config config.json --ingest --process --max-records 1000
```

### Command Line Arguments

- `--config`: Path to configuration JSON file
- `--ingest`: Run the ingestion process
- `--process`: Run the processing pipeline
- `--max-records`: Maximum number of records to ingest (optional)

## Testing

### Unit Tests

Run the test suite:
```bash
python -m pytest tests/
```

### Manual Testing

1. **Test Data Ingestion**:
```bash
python weather_platform.py --config config.json --ingest --max-records 100
```

2. **Verify Data in Cassandra**:
```bash
cqlsh
USE weather;
SELECT * FROM stations LIMIT 10;
```

3. **Test Data Processing**:
```bash
python weather_platform.py --config config.json --process
```

## Architecture

The platform consists of several key components:

1. **Kafka Producer**: Handles real-time data ingestion
2. **Cassandra Database**: Stores historical weather data
3. **Spark Processing**: Performs analytics and machine learning
4. **LRU Cache**: Caches frequently accessed queries
5. **Configuration System**: Manages platform settings

## Error Handling

The platform includes comprehensive error handling:
- Connection retries for Kafka and Cassandra
- Proper resource cleanup
- Detailed logging
- Graceful shutdown

## Logging

Logs are written to the console with the following format:
```
<timestamp> - weather_platform - <level> - <message>
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
