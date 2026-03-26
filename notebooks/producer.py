import json
import pandas as pd
import requests 
import dataclasses
from time import time
from dataclasses import dataclass
from kafka import KafkaProducer, KafkaConsumer


# Process to push to redpanda
@dataclass
class tlfArrival:
    id            : str
    operationType          : int
    vehicleId                : str
    naptanId                 : str
    stationName              : str
    lineId                   : str
    lineName                 : str
    platformName             : str
    direction                : str
    bearing                  : str
    tripId                   : str
    baseVersion              : str
    destinationNaptanId      : str
    destinationName          : str
    timestamp                : str
    timeToStation            : int
    currentLocation          : str
    towards                  : str
    expectedArrival          : str
    timeToLive               : str
    modeName                 : str
    timing_source            : str
    timing_insert            : str
    timing_read              : str
    timing_sent              : str
    timing_received          : str

def trip_from_row(row):
    return tlfArrival(
        id=str(row['id']),
        operationType=int(row['operationType']),
        vehicleId=str(row['vehicleId']),
        naptanId=str(row['naptanId']),
        stationName=str(row['stationName']),
        lineId=str(row['lineId']),
        lineName=str(row['lineName']),
        platformName=str(row['platformName']),
        direction=str(row['direction']),
        bearing=str(row['bearing']),
        tripId=str(row['tripId']),
        baseVersion=str(row['baseVersion']),
        destinationNaptanId=str(row['destinationNaptanId']),
        destinationName=str(row['destinationName']),
        timestamp=str(row['timestamp']),
        timeToStation=int(row['timeToStation']),
        currentLocation=str(row['currentLocation']),
        towards=str(row['towards']),
        expectedArrival=str(row['expectedArrival']),
        timeToLive=str(row['timeToLive']),
        modeName=str(row['modeName']),
        timing_source=str(row['timing_source']),
        timing_insert=str(row['timing_insert']),
        timing_read=str(row['timing_read']),
        timing_sent=str(row['timing_sent']),
        timing_received=str(row['timing_received'])

    )


def trip_serializer(trip):
    trip_dict = dataclasses.asdict(trip)
    return json.dumps(trip_dict).encode('utf-8')

def trip_deserializer(data):
    json_str = data.decode('utf-8')
    trip_dict = json.loads(json_str)
    return tlfArrival(**trip_dict)

server = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=trip_serializer
)

topic_name = "tlf-arrivals"

def fetch_and_send_tlf_arrivals():
    # See API Sample data print
    url = "https://api.tfl.gov.uk/Line/25/Arrivals"
    response = requests.get(url)
    data = response.json()

    # Get 10 lines from all lines for buses only
    url = "https://api.tfl.gov.uk/Line/Mode/bus"
    lines = requests.get(url).json()
    line_ids = [line["id"] for line in lines]
    all_data = []
    for line_id in line_ids:
        url = f"https://api.tfl.gov.uk/Line/{line_id}/Arrivals"

        try:
            response = requests.get(url)
            data = response.json()
            all_data.extend(data)
        except:
            continue

    # Keep only dicts
    clean_data = [d for d in all_data if isinstance(d, dict)]
    df_arrivals = pd.DataFrame(clean_data)

    # Unnest timing column 
    timing_df = pd.json_normalize(df_arrivals['timing'])
    # rename columns to avoid confusion 
    timing_df = timing_df.add_prefix('timing_')

    #concat timing df with main df and drop previous timing column
    timing_df = timing_df[['timing_source', 'timing_insert', 'timing_read', 'timing_sent', 'timing_received']].copy()
    df_arrivals.drop(columns=['timing'], inplace=True)
    line_data = pd.concat([df_arrivals, timing_df], axis=1)


    columns = ['id', 'operationType', 'vehicleId', 'naptanId', 'stationName',
        'lineId', 'lineName', 'platformName', 'direction', 'bearing', 'tripId',
        'baseVersion', 'destinationNaptanId', 'destinationName', 'timestamp',
        'timeToStation', 'currentLocation', 'towards', 'expectedArrival',
        'timeToLive', 'modeName', 'timing_source', 'timing_insert',
        'timing_read', 'timing_sent', 'timing_received'] 
    line_data = line_data[columns]
  

    #|---Send to redpanda---|
    for _, row in line_data.iterrows():
        trip = trip_from_row(row)
        producer.send(topic_name, value=trip)
    producer.flush()

def consume_to_parquet_task(**context):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[server],
        value_deserializer=trip_deserializer,
        group_id="airflow-parquet-exporter", # This ID stores your progress
        auto_offset_reset='earliest',
        enable_auto_commit=True, # Tells Redpanda "I've read this"
        consumer_timeout_ms=5000 # CRITICAL: This stops the task when the stream is empty
    )

    rows = []
    # This loop runs until there is no more data left in Redpanda
    for message in consumer:
        rows.append(dataclasses.asdict(message.value))

    if rows:
        df = pd.DataFrame(rows)
        # Use Airflow's logical date to name the file uniquely
        execution_date = context['logical_date'].strftime('%Y%m%d_%H%M')
        filename = f"/path/to/data/arrivals_{execution_date}.parquet"
        df.to_parquet(filename, engine='pyarrow')
        print(f"Captured {len(rows)} events into {filename}")
    else:
        print("No new data since last run.")
