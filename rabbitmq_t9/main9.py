import json
import asyncio

from rabbitmq_t9 import RabbitmqConnectionTask9
from publisher_t9 import RabbitmqPublisherTask9

async def main9():
    rabbitmq = RabbitmqConnectionTask9("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    await rabbitmq.channel.declare_queue(name="file_queue", durable=True)
    publisher = RabbitmqPublisherTask9(rabbitmq.exchange)

    await publisher.publish(
        {
            "event": "file.uploaded", 
            "file_id":100
        }, 
        routing_key="file.uploaded"
        )
    
    await publisher.publish(
        {
            "event": "file.deleted", 
            "file_id":200
        }, 
        routing_key="file.deleted"
        )

    await publisher.publish(
        {
            "event": "file.processed", 
            "file_id":300
        }, 
        routing_key="file.processed"
        )

    await asyncio.sleep(5)

    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main9())