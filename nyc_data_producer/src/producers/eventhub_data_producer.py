import os
import asyncio
import json
import datetime
import random

from src.data_generators.azure_data_generator import AzureDataGenerator
from src.connectors.azure_blob_connector import AzureBlobConnector
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

download_prefix = "nyc_data"
container = "vdcadena"
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", None)
eventhub_connection_string = os.getenv("EVENTHUB_CONNECTION_STRING", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="Endpoint=sb://bd520w2022.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=VybVBOYURTOx/euPd/e8mWkkxA4BgvFgcvGrovrKwV8=";')
eventhub_topic = os.getenv("EVENTHUB_TOPIC", "vdcadena")
batch_size = 20

async def run():
    data_connector = AzureBlobConnector(connection_str=connection_string, container=container)
    azure_data_generator = AzureDataGenerator(download_prefix ,data_connector)
    data_iterator = azure_data_generator.get_random_data_iterator()
    producer = EventHubProducerClient.from_connection_string(conn_str=eventhub_connection_string, eventhub_name=eventhub_topic)
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()
        # Add events to the batch.
        for i, data in enumerate(data_iterator, start=1):
            add_additional_keys(data)
            string_data = json.dumps(data)
            event_data_batch.add(EventData(body=string_data))
            if i % batch_size == 0:
                print(f"Sending data batch {i} to event hub")
                # Send the batch of events to the event hub.
                await producer.send_batch(event_data_batch) 
                event_data_batch = await producer.create_batch()        
                print(f"Sent data batch {i}")
                await asyncio.sleep(1.5)

def add_additional_keys(data: dict):
    current_time = datetime.datetime.utcnow()
    data["simulated_pickup_timestamp"] = str(current_time.strftime("%Y-%m-%d %H:%M:%S"))
    dropoff_time = current_time + datetime.timedelta(minutes=random.randint(15, 60))
    data["simulated_dropoff_datetime"] = str(dropoff_time.strftime("%Y-%m-%d %H:%M:%S"))

def start_data_production():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())