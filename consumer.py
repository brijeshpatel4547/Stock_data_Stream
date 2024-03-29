import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import (BlobCheckpointStore)

# Storage account connection String for checkpointing
BLOB_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=streamcheckpoint;AccountKey=uDK4qIhjtpXWZblRFdY+eNnBdRSjgN9D0GDsUN+oI2UuSI2MAW8n6FHM5WKIypIXEXmAiYpik9t6+AStvCX7KQ==;EndpointSuffix=core.windows.net"
BLOB_CONTAINER_NAME = "stream-checkpoint"

# Event Hub connection String
EVENT_HUB_CONNECTION_STR = "Endpoint=sb://stock-market-analytics.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=2DAC20eCHGHTVVW0LqkwF17QJFMc4UFRx+AEhOhdzYs="
# Event Hub name
EVENT_HUB_NAME = "stock-stream"


async def on_event(partition_context, event):
    # Print event data
    print('Received event: "{}" from the partition with ID: {}'.format(
        event.body_as_str(encoding="UTF-8"), partition_context.partition_id))
    # Update the checkpoint so the program doesn't read events that is already read when run it next time
    await partition_context.update_checkpoint(event)


async def main():
    # Create Azure Blob Check Point
    checkpoint_store = BlobCheckpointStore.from_connection_string(BLOB_STORAGE_CONNECTION_STRING, BLOB_CONTAINER_NAME)
    # Create consumer
    consumer = EventHubConsumerClient.from_connection_string(
        EVENT_HUB_CONNECTION_STR, consumer_group="$Default",
        eventhub_name=EVENT_HUB_NAME, checkpoint_store=checkpoint_store,
    )
    async with consumer:
        # call receive method to read the event from beginnig of partition
        await consumer.receive(on_event=on_event,
                               starting_position="-1")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    # run the main method
    loop.run_until_complete(main())
