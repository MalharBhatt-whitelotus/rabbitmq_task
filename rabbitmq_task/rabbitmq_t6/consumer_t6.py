import json
import asyncio

from rabbitmq_t6 import RabbitmqConnectionTask6

async def consume(message):
    async with message.process():
        message_body = json.loads(message.body.decode())
        if message_body["file_id"] == 6:
            raise Exception("Something went 6....")
        print("Message received.")
        print(message_body)

async def main():

    rabbitmq = RabbitmqConnectionTask6("amqp://guest:guest@localhost:5672")

    await rabbitmq.connect()

    queue = await rabbitmq.channel.declare_queue(name="test_queue6", durable=True)

    await queue.consume(consume)

    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())