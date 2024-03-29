# Connect using the connection string
# Install the package azure-eventhub

# Importing libraries
import asyncio
import csv

import pandas as pd
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient


# Event Hub connection String
EVENT_HUB_CONNECTION_STR = "Endpoint=sb://stock-market-analytics.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=2DAC20eCHGHTVVW0LqkwF17QJFMc4UFRx+AEhOhdzYs="
# Event Hub name
EVENT_HUB_NAME = "stock-stream"


async def send():
    # Create a producer client to send messages to the event hub.
    producer = EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_CONNECTION_STR,eventhub_name=EVENT_HUB_NAME)
    async with producer:
        with open('indexProcessed.csv','r') as file:
            reader = csv.reader(file)
            for row in reader:
                event_data = ','.join(row)
                event_data_batch = await producer.create_batch()
                event_data_batch.add(EventData(event_data))
                await producer.send_batch(event_data_batch)

        print("All data has been streamed")

asyncio.run(send())
