import json
import os
import time
import logging
from datetime import datetime
import socket
from collections import OrderedDict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('weather_platform')

# Define required dependencies with proper error handling
try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    logger.error("Kafka library not found. Install with 'pip install kafka-python'")
    raise

try:
    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra.policies import RetryPolicy, RoundRobinPolicy, DCAwareRoundRobinPolicy
    from cassandra.query import ConsistencyLevel
    from cassandra.auth import PlainTextAuthProvider
except ImportError:
    logger.error("Cassandra library not found. Install with 'pip install cassandra-driver'")
    raise

try:
    from pyspark.sql import SparkSession
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import DecisionTreeRegressor
    from pyspark.ml.evaluation import RegressionEvaluator
except ImportError:
    logger.error("PySpark library not found. Install with 'pip install pyspark'")
    raise

# Generate sample weather module if it doesn't exist
class WeatherGenerator:
    def __init__(self):
        self.stations = ["ST1", "ST2", "ST3", "ST4", "ST5"]
        self.current_date = datetime.now().strftime("%Y-%m-%d")
        
    def get_next_weather(self, delay_sec=0.1):
        """Generate synthetic weather data for testing"""
        import random
        time.sleep(delay_sec)
        
        station_id = random.choice(self.stations)
        temperature = random.uniform(-10, 40)  # Temperature in Celsius
        
        return self.current_date, temperature, station_id

# Create a simple protobuf-like class if protobuf is not available
class Report:
    def __init__(self, date, degrees, station_id):
        self.date = date
        self.degrees = degrees
        self.station_id = station_id
        
    def SerializeToString(self):
        """Simple JSON serialization fallback"""
        return json.dumps({
            "date": self.date,
            "degrees": self.degrees,
            "station_id": self.station_id
        }).encode('utf-8')

# Import actual protobuf if available
try:
    import report_pb2
except ImportError:
    logger.warning("Protobuf report_pb2 module not found. Using JSON fallback.")
    report_pb2 = type('', (), {})
    report_pb2.Report = Report

# Import weather module if available, otherwise use generator
try:
    import weather
except ImportError:
    logger.warning("Weather module not found. Using synthetic data generator.")
    weather = WeatherGenerator()

class LRUCache:
    def __init__(self, capacity):
        self.cache = OrderedDict()
        self.capacity = capacity
        
    def get(self, key):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]
        
    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(first=True)

class WeatherPlatform:
    def __init__(self, config=None):
        """Initialize the WeatherPlatform with optional configuration override"""
        # Default configuration
        self.config = {
            'kafka': {
                'bootstrap_servers': ['localhost:9092'],
                'topic': 'temperatures',
                'retries': 10,
                'retry_backoff_ms': 500
            },
            'cassandra': {
                'contact_points': ['localhost'],
                'port': 9042,
                'keyspace': 'weather',
                'username': None,
                'password': None,
                'replication_factor': 3
            },
            'spark': {
                'app_name': 'weather_platform',
                'executor_memory': '512M'
            },
            'cache': {
                'capacity': 10
            }
        }
        
        # Override with user configuration if provided
        if config:
            self._deep_update(self.config, config)
        
        # Initialize components
        self._init_kafka()
        self._init_cassandra()
        self._init_spark()
        
        # Simple LRU cache for queries
        self.temp_cache = LRUCache(capacity=self.config['cache']['capacity'])
    
    def _deep_update(self, d, u):
        """Deep update dictionary d with values from u"""
        for k, v in u.items():
            if isinstance(v, dict) and k in d and isinstance(d[k], dict):
                self._deep_update(d[k], v)
            else:
                d[k] = v
    
    def _init_kafka(self):
        """Initialize Kafka producer with retries"""
        for attempt in range(5):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                    retries=self.config['kafka']['retries'],
                    retry_backoff_ms=self.config['kafka']['retry_backoff_ms'],
                    acks='all',
                    key_serializer=lambda k: str(k).encode('utf-8'),
                    value_serializer=lambda v: v.SerializeToString()
                )
                logger.info("Kafka producer initialized successfully")
                return
            except KafkaError as e:
                logger.warning(f"Kafka connection attempt {attempt+1} failed: {str(e)}")
                time.sleep(2)
        
        logger.error("Failed to connect to Kafka after multiple attempts")
        raise ConnectionError("Could not connect to Kafka")
        
    def _init_cassandra(self):
        """Initialize Cassandra connection with retries"""
        auth_provider = None
        if self.config['cassandra']['username'] and self.config['cassandra']['password']:
            auth_provider = PlainTextAuthProvider(
                username=self.config['cassandra']['username'],
                password=self.config['cassandra']['password']
            )
        
        # Create a retry policy
        retry_profile = ExecutionProfile(
            load_balancing_policy=RoundRobinPolicy(),
            retry_policy=RetryPolicy(),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            request_timeout=10
        )
        
        profiles = {EXEC_PROFILE_DEFAULT: retry_profile}
        
        for attempt in range(5):
            try:
                self.cluster = Cluster(
                    contact_points=self.config['cassandra']['contact_points'],
                    port=self.config['cassandra']['port'],
                    auth_provider=auth_provider,
                    execution_profiles=profiles
                )
                self.session = self.cluster.connect()
                logger.info("Cassandra connection established")
                self.setup_cassandra()
                return
            except Exception as e:
                logger.warning(f"Cassandra connection attempt {attempt+1} failed: {str(e)}")
                time.sleep(2)
                
        logger.error("Failed to connect to Cassandra after multiple attempts")
        raise ConnectionError("Could not connect to Cassandra")
        
    def _init_spark(self):
        """Initialize Spark session with proper configuration"""
        try:
            self.spark = SparkSession.builder \
                .appName(self.config['spark']['app_name']) \
                .config("spark.executor.memory", self.config['spark']['executor_memory']) \
                .config("spark.cassandra.connection.host", ",".join(self.config['cassandra']['contact_points'])) \
                .config("spark.cassandra.connection.port", self.config['cassandra']['port']) \
                .getOrCreate()
            logger.info("Spark session initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {str(e)}")
            self.spark = None
            
    def setup_cassandra(self):
        """Set up Cassandra keyspace and tables"""
        try:
            # Create keyspace with configurable replication factor
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {self.config['cassandra']['keyspace']}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {self.config['cassandra']['replication_factor']}}}
            """)
            
            self.session.set_keyspace(self.config['cassandra']['keyspace'])
            
            # Create stations table storing both tmax and tmin
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS stations (
                    id text,
                    date date,
                    tmax double,
                    tmin double,
                    PRIMARY KEY (id, date)
                ) WITH CLUSTERING ORDER BY (date ASC)
            """)
            logger.info("Cassandra schema setup complete")
        except Exception as e:
            logger.error(f"Failed to set up Cassandra schema: {str(e)}")
            raise
        
    def ingest_weather_data(self, max_records=None):
        """
        Ingests weather data using Kafka producer and stores in Cassandra
        Args:
            max_records: Maximum number of records to ingest (None for infinite)
        """
        record_count = 0
        
        try:
            logger.info("Starting weather data ingestion")
            
            for date, degrees, station_id in weather.get_next_weather(delay_sec=0.1):
                # Build a protobuf message
                report = report_pb2.Report(
                    date=date,
                    degrees=degrees,
                    station_id=station_id
                )
                
                # Send to Kafka topic
                future = self.producer.send(
                    self.config['kafka']['topic'], 
                    key=station_id, 
                    value=report
                )
                
                # Handle any sending errors
                try:
                    future.get(timeout=10)
                except KafkaError as e:
                    logger.error(f"Failed to send message to Kafka: {str(e)}")
                
                # Insert into Cassandra
                try:
                    # For demonstration, treat `degrees` as tmax and artificially reduce 5.0 for tmin
                    prepared = self.session.prepare("""
                        INSERT INTO stations (id, date, tmax, tmin)
                        VALUES (?, ?, ?, ?)
                    """)
                    prepared.consistency_level = ConsistencyLevel.ONE
                    self.session.execute(prepared, [
                        station_id,
                        date,
                        degrees,
                        degrees - 5.0
                    ])
                    
                    record_count += 1
                    if record_count % 100 == 0:
                        logger.info(f"Ingested {record_count} records")
                    
                    if max_records and record_count >= max_records:
                        logger.info(f"Reached maximum record count: {max_records}")
                        break
                        
                except Exception as e:
                    logger.error(f"Failed to store record in Cassandra: {str(e)}")
                
        except KeyboardInterrupt:
            logger.info("Ingestion interrupted by user")
        except Exception as e:
            logger.error(f"Error during ingestion: {str(e)}")
        finally:
            logger.info(f"Ingestion complete. Total records: {record_count}")
            return record_count
            
    def process_weather_data(self):
        """Processes weather data in Spark and trains a regression model for tmax"""
        if not self.spark:
            logger.error("Spark session not available. Cannot process data.")
            return None, None
            
        try:
            # Load data from Cassandra
            weather_df = self.spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(keyspace=self.config['cassandra']['keyspace'], table="stations") \
                .load()
                
            if weather_df.count() == 0:
                logger.warning("No weather data found for processing")
                return None, None
                
            weather_df.createOrReplaceTempView("weather_data")
            
            # Simple stats
            stats_df = self.spark.sql("""
                SELECT 
                    id,
                    COUNT(*) as record_count,
                    AVG(tmax) as avg_temp,
                    MAX(tmax) as max_temp
                FROM weather_data
                GROUP BY id
                ORDER BY record_count DESC
            """)
            
            # Prepare features: tmax, tmin -> features, with tmax as the label
            assembler = VectorAssembler(
                inputCols=["tmax", "tmin"],
                outputCol="features"
            )
            # Drop any rows with null tmax/tmin
            featured_df = assembler.transform(weather_df.dropna(subset=["tmax", "tmin"]))
            
            if featured_df.count() == 0:
                logger.warning("No valid data for ML training after filtering")
                return stats_df, {}
            
            train_data, test_data = featured_df.randomSplit([0.8, 0.2], seed=41)
            
            evaluator = RegressionEvaluator(
                labelCol="tmax",
                predictionCol="prediction",
                metricName="rmse"
            )
            
            # Train a few models with different tree depths
            rmse_metrics = {}
            for depth in [1, 5, 10, 15, 20]:
                try:
                    dt = DecisionTreeRegressor(
                        maxDepth=depth,
                        labelCol="tmax",
                        featuresCol="features",
                        seed=41
                    )
                    model = dt.fit(train_data)
                    predictions = model.transform(test_data)
                    rmse = evaluator.evaluate(predictions)
                    rmse_metrics[f'depth={depth}'] = rmse
                    
                    # Save the best model
                    if depth == 10:  # Example - could use a better metric
                        model_path = f"models/dt_model_{depth}"
                        model.save(model_path)
                        logger.info(f"Saved model with depth {depth} to {model_path}")
                        
                except Exception as e:
                    logger.error(f"Error training model with depth {depth}: {str(e)}")
            
            return stats_df, rmse_metrics
        except Exception as e:
            logger.error(f"Error processing weather data: {str(e)}")
            return None, None
    
    def query_station_max(self, station_id):
        """Queries the max temperature for a given station, caching results."""
        if not station_id:
            logger.error("Invalid station ID provided")
            return None, False
            
        cache_key = f"max_{station_id}"
        cached_value = self.temp_cache.get(cache_key)
        if cached_value is not None:
            logger.debug(f"Cache hit for {cache_key}")
            return cached_value, True
        
        try:
            # Query Cassandra with consistency level THREE
            prepared = self.session.prepare("""
                SELECT MAX(tmax) AS max_temp
                FROM stations
                WHERE id = ?
            """)
            prepared.consistency_level = ConsistencyLevel.THREE
            
            row = self.session.execute(prepared, [station_id]).one()
            max_temp = row.max_temp if row else None
            
            self.temp_cache.put(cache_key, max_temp)
            logger.debug(f"Cached result for {cache_key}")
            return max_temp, False
        except Exception as e:
            logger.error(f"Error querying station max temp: {str(e)}")
            return None, False
    
    def close(self):
        """Clean up resources"""
        try:
            if hasattr(self, 'producer'):
                self.producer.close()
                logger.info("Kafka producer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {str(e)}")
            
        try:
            if hasattr(self, 'session') and self.session:
                self.session.shutdown()
                logger.info("Cassandra session closed")
        except Exception as e:
            logger.error(f"Error closing Cassandra session: {str(e)}")
            
        try:
            if hasattr(self, 'cluster') and self.cluster:
                self.cluster.shutdown()
                logger.info("Cassandra cluster connection closed")
        except Exception as e:
            logger.error(f"Error closing Cassandra cluster: {str(e)}")
            
        try:
            if hasattr(self, 'spark') and self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")
        except Exception as e:
            logger.error(f"Error stopping Spark session: {str(e)}")

def main():
    # Parse command line arguments
    import argparse
    
    parser = argparse.ArgumentParser(description='Weather Platform Data Processor')
    parser.add_argument('--config', type=str, help='Path to configuration JSON file')
    parser.add_argument('--ingest', action='store_true', help='Run the ingestion process')
    parser.add_argument('--process', action='store_true', help='Run the processing pipeline')
    parser.add_argument('--max-records', type=int, default=None, help='Maximum records to ingest')
    args = parser.parse_args()
    
    # Load configuration if provided
    config = None
    if args.config:
        try:
            with open(args.config, 'r') as f:
                config = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            return
    
    platform = None
    try:
        # Create platform instance
        platform = WeatherPlatform(config)
        
        # Run ingestion if requested
        if args.ingest:
            platform.ingest_weather_data(max_records=args.max_records)
        
        # Run processing if requested
        if args.process:
            stats_df, model_rmse = platform.process_weather_data()
            
            if stats_df:
                print("Station Statistics:")
                stats_df.show()
            
            if model_rmse:
                print("\nModel RMSE by depth:")
                for depth, rmse in model_rmse.items():
                    print(f"{depth}: {rmse}")
    
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        # Clean up resources
        if platform:
            platform.close()

if __name__ == "__main__":
    main()
