"""Run the eight original assertions with equivalent JVM SQL fixture input.

Only input construction changes: upstream createDataFrame(Python tuples) crashes
its Python worker on this Windows runtime. Transformation and assertions are reused.
Set UPSTREAM_DIR to the pinned checkout before running pytest.
"""
import importlib
import os
from pathlib import Path
import sys
import pytest
from pyspark.sql import SparkSession

repo = Path(os.environ['UPSTREAM_DIR']).resolve()
sys.path[:0] = [str(repo / 'tests'), str(repo / 'src'), str(repo / 'src/pipeline')]
original = importlib.import_module('test_silver_pipeline')
for name in dir(original):
    if name.startswith('test_'):
        globals()[name] = getattr(original, name)


@pytest.fixture(scope='session')
def spark():
    session = (SparkSession.builder.master('local[1]').appName('original-assertions-windows')
               .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '1')
               .config('spark.driver.host', '127.0.0.1').getOrCreate())
    session.sparkContext.setLogLevel('ERROR')
    yield session
    session.stop()


@pytest.fixture(scope='session')
def df_silver(spark):
    raw = spark.sql("""SELECT * FROM VALUES
      ('id1','125 000 €','67 mp','2 camere','Floreasca, Sector 2, București','Ap 2 cam','url1'),
      ('id2','250 000 €','120 mp','4 camere','Dorobanți, Sector 1, București','Ap 4 cam','url2'),
      ('id3','N/A','50 mp','1 camera','Titan, Sector 3, București','Ap 1 cam','url3'),
      ('id4','95 000 €','N/A','2 camere','Militari, Sector 6, București','Ap 2 cam','url4'),
      ('id5','180 000 €','90 mp','','','Ap 3 cam','url5')
      AS listings(listing_id, price, area, rooms, location, title, url)""")
    return original.transform_bronze_to_silver(raw)
