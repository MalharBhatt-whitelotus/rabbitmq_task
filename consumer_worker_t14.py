import asyncio
import aio_pika

from consumer_t14 import consume
from rabbitmq_t14 import RabbitmqConnectionTask14

async def main():

    rabbitmq = RabbitmqConnectionTask14("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    await rabbitmq.channel.set_qos(prefetch_count=5)

    await consume(rabbitmq.competitor_consumer_queue, "Consumer-A")
    await consume(rabbitmq.competitor_consumer_queue, "Consumer-B")
    await consume(rabbitmq.competitor_consumer_queue, "Consumer-C")

    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
