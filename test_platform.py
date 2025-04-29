import pytest
import time
from weather_platform import WeatherPlatform

@pytest.fixture
def platform():
    wp = WeatherPlatform()
    yield wp
    wp.session.shutdown()
    wp.cluster.shutdown()

def test_ingest_and_query(platform):
    import threading
    ingestion_thread = threading.Thread(
        target=platform.ingest_weather_data
    )
    ingestion_thread.daemon = True
    ingestion_thread.start()
    
    time.sleep(5)
    
    station_id = "StationA"
    
    max_temp1, cached1 = platform.query_station_max(station_id)
    assert max_temp1 is not None
    assert not cached1
    
    max_temp2, cached2 = platform.query_station_max(station_id)
    assert cached2
    assert max_temp2 == max_temp1

def test_spark_processing(platform):
    stats_df, metrics = platform.process_weather_data()
    
    assert stats_df is not None
    assert stats_df.count() > 0
    
    assert len(metrics) == 5
    for rmse in metrics.values():
        assert rmse >= 0
