# Stream-Processing

pip install uv
uv init -p 3.12
uv add kafka-python pandas pyarrow

kakfa-python is the client we use to connect to kakfa, kakfa protocol because we are going to use redpandas, pyarrow for processing parquet files

create the docker compose file and str the logs using 
docker compose up redpanda -d

to view the logs open another terminal and run (f meaning forward)
docker compose logs redpanda -f

Next Create producer and a consumer 
1. Producer writes to the kakfa stream while the sumer reads from the kakfa stream

To start for the producer
create folder
craete a new file >> producer.ipynb (convert to a py file later)
install jupyter (uv add --dev jupyter) install as a dev dependency



Line API snapshot → cleaned DataFrame
      vehicleId, tripId, lineId, lineName, stationName, direction, bearing, expectedArrival, timestamp, timing
             ↓
Kafka topic: tfl-line-arrivals
             ↓
Flink streaming job:
   - Tumbling windows → active vehicle counts per line/station
   - Session windows → vehicle “streaks” (continuous presence)
   - Delay computations → expectedArrival vs timestamp
             ↓
GCS / BigQuery
             ↓
dbt transformations → analytics-ready tables

after the producer, we pass pour data tp flink. Flink needs two things a jobmanager and and a task manager

Steps 
- include taskmanager and job manager to the docker-compose file 
- before you start, download by running the following to get dockerfile.flink, pyproject.flink.toml, and flink-config.yaml
```
PREFIX="https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/main/07-streaming/workshop"

wget ${PREFIX}/Dockerfile.flink
wget ${PREFIX}/pyproject.flink.toml
wget ${PREFIX}/flink-config.yaml
```

- create a new folder src to map volumes 
- run docker compose up to start the new containers 
- Check the Flink dashboard at http://localhost:8081 - you should see 1 task manager with 15 available task slots.

